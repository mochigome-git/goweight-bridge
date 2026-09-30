package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochigome-git/goweight-bridge/internal/config"
	"go.uber.org/zap"
)

// Pool wraps a pgx connection pool with the same reconnect / health-check
// resilience pattern used by gokafka-raw's DBManager: explicit TLS config,
// a larger connection pool, a periodic auto-reconnect ping loop, and a
// graceful shutdown sequence.
type Pool struct {
	pool         *pgxpool.Pool
	logger       *zap.Logger
	shutdownChan chan struct{}
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

func New(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Pool, error) {
	// cfg.SupabaseDBPassword may be stored pre-URL-encoded (e.g. in ECS, to
	// dodge issues with raw "@" or other reserved characters elsewhere).
	// url.UserPassword() percent-encodes whatever it's given when the URL is
	// serialized, so feeding it an already-encoded password double-encodes it
	// and breaks auth. Decode first so UserPassword sees the real password.
	rawPassword, err := url.QueryUnescape(cfg.SupabaseDBPassword)
	if err != nil {
		logger.Warn("SupabaseDBPassword did not decode as URL-encoded, using as-is", zap.Error(err))
		rawPassword = cfg.SupabaseDBPassword
	}

	u := &url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(cfg.SupabaseDBUser, rawPassword),
		Host:   fmt.Sprintf("%s:%s", cfg.SupabaseDBHost, cfg.SupabaseDBPort),
		Path:   "/" + cfg.SupabaseDBName,
	}

	poolCfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}

	// Disable prepared statements — same reasoning as gokafka-raw: avoids
	// "prepared statement already exists" against Supabase's transaction-mode pooler.
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	// Explicit TLS config, matching gokafka-raw's cfg.CreatePostgresTLSConfig().
	// This project's config package doesn't have that helper yet — if you add
	// one, swap this line for cfg.CreatePostgresTLSConfig() to share the logic.
	// Equivalent to the old sslmode=require query param: encrypted, no cert verification.
	poolCfg.ConnConfig.TLSConfig = &tls.Config{
		ServerName:         cfg.SupabaseDBHost,
		InsecureSkipVerify: true,
	}

	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute
	poolCfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	logger.Info("Database connected")

	p := &Pool{
		pool:         pool,
		logger:       logger,
		shutdownChan: make(chan struct{}),
	}
	p.startAutoReconnect(ctx)

	return p, nil
}

// PoolHandle exposes the underlying pgx pool for call sites that need direct access.
// pgxpool.Pool is already safe for concurrent use on its own, and this field is
// never reassigned after construction, so no locking is needed here.
func (p *Pool) PoolHandle() *pgxpool.Pool {
	return p.pool
}

// startAutoReconnect periodically pings the DB and logs failures — the same
// pattern gokafka-raw's DBManager uses. It surfaces auth or connectivity
// problems within 30s of them starting, instead of only finding out when the
// next real query happens to fail.
func (p *Pool) startAutoReconnect(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-p.shutdownChan:
				p.logger.Info("Auto-reconnect stopped: shutdown signal received")
				return
			case <-ticker.C:
				if err := p.pool.Ping(ctx); err != nil {
					p.logger.Error("DB ping failed", zap.Error(err))
				} else {
					p.logger.Debug("DB ping successful")
				}
			}
		}
	}()
}

// IsShuttingDown reports whether Close has been called.
func (p *Pool) IsShuttingDown() bool {
	select {
	case <-p.shutdownChan:
		return true
	default:
		return false
	}
}

// Close gracefully stops the auto-reconnect loop and closes the pool.
// Kept as "Close" (not "Shutdown") so existing call sites — e.g.
// `defer database.Close()` in main.go — don't need to change.
func (p *Pool) Close() {
	p.shutdownOnce.Do(func() {
		p.logger.Info("Initiating DB pool graceful shutdown...")

		close(p.shutdownChan)
		p.wg.Wait()

		if p.pool != nil {
			p.pool.Close()
			p.logger.Info("Database connection pool closed")
		}

		p.logger.Info("DB pool shutdown completed")
	})
}

// ---------------------------------------------------------------------------
// Everything below is unchanged business logic, kept as-is from the original
// db.go. Methods still receive *Pool and use p.pool directly, so nothing here
// needed to change for the refactor above.
// ---------------------------------------------------------------------------

// DeviceStatus is the payload from MQTT status topic.
type DeviceStatus struct {
	DeviceID       string
	TenantID       string
	Online         bool
	Mode           string // "idle" | "run" | ""
	CurrentModelID *string
}

// UpsertDevice inserts or updates a device row in app.weight_devices.
func (p *Pool) UpsertDevice(ctx context.Context, d DeviceStatus) error {
	sql := `
		insert into app.weight_devices
			(device_id, tenant_id, online, mode, current_model_id, last_seen_at, first_seen_at)
		values
			($1, $2, $3, $4, $5, now(), now())
		on conflict (device_id) do update set
			tenant_id        = excluded.tenant_id,
			online           = excluded.online,
			mode             = excluded.mode,
			current_model_id = excluded.current_model_id,
			last_seen_at     = now()
	`
	_, err := p.pool.Exec(ctx, sql,
		d.DeviceID,
		d.TenantID,
		d.Online,
		nullableString(d.Mode),
		d.CurrentModelID,
	)
	if err != nil {
		p.logger.Error("UpsertDevice failed",
			zap.String("device_id", d.DeviceID),
			zap.Error(err),
		)
		return err
	}
	p.logger.Info("Device upserted",
		zap.String("device_id", d.DeviceID),
		zap.Bool("online", d.Online),
	)
	return nil
}

// WeightModel is a single model record from app.weight_models.
type WeightModel struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	LowerLimit float64 `json:"lower_limit"`
	Standard   float64 `json:"standard"`
	UpperLimit float64 `json:"upper_limit"`
	Unit       string  `json:"unit"`
}

// FetchModelsForTenant returns all active (non-deleted) models for a tenant.
func (p *Pool) FetchModelsForTenant(ctx context.Context, tenantID string) ([]WeightModel, error) {
	sql := `
		select id, name, lower_limit, standard, upper_limit, unit
		from app.weight_models
		where tenant_id = $1
		  and deleted_at is null
		order by name asc
	`
	rows, err := p.pool.Query(ctx, sql, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fetch models: %w", err)
	}
	defer rows.Close()

	models, err := pgx.CollectRows(rows, pgx.RowToStructByName[WeightModel])
	if err != nil {
		return nil, fmt.Errorf("collect models: %w", err)
	}
	return models, nil
}

// FetchOnlineDevicesForTenant returns device_ids of all online devices for a tenant.
func (p *Pool) FetchOnlineDevicesForTenant(ctx context.Context, tenantID string) ([]string, error) {
	sql := `
		select device_id
		from app.weight_devices
		where tenant_id = $1
		  and online = true
	`
	rows, err := p.pool.Query(ctx, sql, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fetch online devices: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// FetchAllTenantIDs returns distinct tenant_ids that have at least one device.
func (p *Pool) FetchAllTenantIDs(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`select distinct tenant_id::text from app.weight_devices`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// FetchTenantCode returns the tenant_code for a given tenant UUID.
func (p *Pool) FetchTenantCode(ctx context.Context, tenantID string) (string, error) {
	var code string
	err := p.pool.QueryRow(ctx,
		`select tenant_code from public.tenants where id = $1`,
		tenantID,
	).Scan(&code)
	if err != nil {
		return "", fmt.Errorf("fetch tenant_code: %w", err)
	}
	return code, nil
}

// FetchTenantIDByCode returns the tenant UUID for a given tenant_code.
func (p *Pool) FetchTenantIDByCode(ctx context.Context, tenantCode string) (string, error) {
	var id string
	err := p.pool.QueryRow(ctx,
		`select id::text from public.tenants where tenant_code = $1`,
		tenantCode,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("fetch tenant_id by code: %w", err)
	}
	return id, nil
}

// UpsertModelFromDevice inserts or updates a weight model reported by a device.
func (p *Pool) UpsertModelFromDevice(ctx context.Context, tenantID, id, name string, lower, standard, upper float64, unit string) error {
	_, err := p.pool.Exec(ctx, `
        insert into app.weight_models
            (id, tenant_id, name, lower_limit, standard, upper_limit, unit)
        values ($1, $2, $3, $4, $5, $6, $7)
        on conflict (id) do update set
            name        = excluded.name,
            lower_limit = excluded.lower_limit,
            standard    = excluded.standard,
            upper_limit = excluded.upper_limit,
            unit        = excluded.unit,
            updated_at  = now()
    `, id, tenantID, name, lower, standard, upper, unit)
	if err != nil {
		p.logger.Error("UpsertModelFromDevice failed", zap.Error(err))
		return err
	}
	p.logger.Info("Model upserted from device",
		zap.String("id", id),
		zap.String("name", name),
	)
	return nil
}

func (p *Pool) DeleteModel(ctx context.Context, tenantID, id string) error {
	_, err := p.pool.Exec(ctx, `
        update app.weight_models
        set deleted_at = now()
        where id = $1 and tenant_id = $2
    `, id, tenantID)
	if err != nil {
		p.logger.Error("DeleteModel failed", zap.Error(err))
		return err
	}
	return nil
}

// InsertLot creates a new lot for the machine. production.lot generates the
// lot itself (defaults/trigger), exactly like the gopub-edge -> gokafka-raw path.
// Returns the inserted row as JSON so we don't hard-code production.lot's columns.
func (p *Pool) InsertLot(ctx context.Context, machineID string) ([]byte, error) {
	var row []byte
	err := p.pool.QueryRow(ctx, `
		insert into production.lot as l (machine_id)
		values ($1)
		returning to_jsonb(l)`,
		machineID,
	).Scan(&row)
	if err != nil {
		p.logger.Error("InsertLot failed", zap.String("machine_id", machineID), zap.Error(err))
		return nil, err
	}
	p.logger.Info("Lot inserted", zap.String("machine_id", machineID), zap.ByteString("lot", row))
	return row, nil
}
