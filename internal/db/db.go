package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochigome-git/goweight-bridge/internal/config"
	"go.uber.org/zap"
)

type Pool struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func New(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Pool, error) {
	databaseURL := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s?sslmode=require&default_query_exec_mode=simple_protocol",
		cfg.SupabaseDBUser,
		cfg.SupabaseDBPassword,
		cfg.SupabaseDBHost,
		cfg.SupabaseDBPort,
		cfg.SupabaseDBName,
	)

	poolCfg, err := pgxpool.ParseConfig(databaseURL) // renamed to poolCfg
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}

	poolCfg.MaxConns = 5
	poolCfg.MinConns = 1
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	logger.Info("Database connected")
	return &Pool{pool: pool, logger: logger}, nil
}

func (p *Pool) Close() {
	p.pool.Close()
}

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

// Replace InsertModel with this:
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
