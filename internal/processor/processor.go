package processor

import (
	"context"
	"encoding/json"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochigome-git/goweight-bridge/internal/db"
	"github.com/mochigome-git/goweight-bridge/internal/models"
	"go.uber.org/zap"
)

// Processor routes incoming MQTT messages to the correct handler.
// Inbound topic format: {tenant_code}/{device_id}/{type}
type Processor struct {
	db        *db.Pool
	publisher *models.Publisher
	logger    *zap.Logger
}

func New(database *db.Pool, publisher *models.Publisher, logger *zap.Logger) *Processor {
	return &Processor{
		db:        database,
		publisher: publisher,
		logger:    logger,
	}
}

type modelPushPayload struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	LowerLimit float64 `json:"lower_limit"`
	Standard   float64 `json:"standard"`
	UpperLimit float64 `json:"upper_limit"`
	Unit       string  `json:"unit"`
	Deleted    bool    `json:"deleted"`
}

func (p *Processor) handleModelPush(ctx context.Context, tenantCode string, payload []byte) {
	tenantID, err := p.db.FetchTenantIDByCode(ctx, tenantCode)
	if err != nil {
		p.logger.Error("Unknown tenant_code", zap.String("tenant_code", tenantCode), zap.Error(err))
		return
	}

	var m modelPushPayload
	if err := json.Unmarshal(payload, &m); err != nil {
		p.logger.Warn("Bad models/push payload", zap.Error(err))
		return
	}

	// Handle delete
	if m.Deleted {
		if m.ID == "" {
			p.logger.Warn("models/push delete missing id, skipping")
			return
		}
		if err := p.db.DeleteModel(ctx, tenantID, m.ID); err != nil {
			p.logger.Error("DeleteModel failed", zap.Error(err))
			return
		}
		p.logger.Info("Model deleted from device", zap.String("id", m.ID))
		return
	}

	if m.Name == "" {
		p.logger.Warn("models/push missing name, skipping")
		return
	}

	if err := p.db.UpsertModelFromDevice(ctx, tenantID, m.ID, m.Name,
		m.LowerLimit, m.Standard, m.UpperLimit, m.Unit); err != nil {
		p.logger.Error("UpsertModelFromDevice failed", zap.Error(err))
		return
	}
}

// HandleMessage is the Paho MessageHandler — called for every subscribed message.
func (p *Processor) HandleMessage(_ mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()
	parts := strings.SplitN(topic, "/", 3)
	ctx := context.Background()

	p.logger.Info("MQTT message received", // ← add this
		zap.String("topic", topic),
		zap.Int("payload_len", len(payload)),
	)

	if len(parts) == 3 && parts[1] == "models" && parts[2] == "push" {
		p.handleModelPush(ctx, parts[0], payload)
		return
	}

	if len(parts) < 3 {
		p.logger.Debug("Unexpected topic format, skipping", zap.String("topic", topic))
		return
	}

	tenantCode := parts[0]
	deviceID := parts[1]
	msgType := parts[2]

	switch msgType {
	case "status":
		p.handleStatus(ctx, tenantCode, deviceID, payload)
	case "model/ack":
		p.handleModelAck(ctx, tenantCode, deviceID, payload)
	default:
		p.logger.Debug("Ignoring unhandled topic type",
			zap.String("topic", topic),
			zap.String("type", msgType),
		)
	}
}

// -----------------------------------------------------------------------
// status payload from device (retained + LWT)
// {"online": true, "mode": "run", "model_id": "uuid-or-omitted"}
// LWT: {"online": false}
// -----------------------------------------------------------------------
type statusPayload struct {
	Online         bool    `json:"online"`
	Mode           string  `json:"mode"`
	CurrentModelID *string `json:"model_id,omitempty"`
}

func (p *Processor) handleStatus(ctx context.Context, tenantCode, deviceID string, payload []byte) {
	tenantID, err := p.db.FetchTenantIDByCode(ctx, tenantCode)
	if err != nil {
		p.logger.Error("Unknown tenant_code",
			zap.String("tenant_code", tenantCode),
			zap.Error(err),
		)
		return
	}

	var s statusPayload
	if err := json.Unmarshal(payload, &s); err != nil {
		p.logger.Warn("Bad status payload",
			zap.String("device_id", deviceID),
			zap.Error(err),
		)
		return
	}

	if err := p.db.UpsertDevice(ctx, db.DeviceStatus{
		DeviceID:       deviceID,
		TenantID:       tenantID,
		Online:         s.Online,
		Mode:           s.Mode,
		CurrentModelID: s.CurrentModelID,
	}); err != nil {
		p.logger.Error("UpsertDevice failed",
			zap.String("device_id", deviceID),
			zap.Error(err),
		)
	}
}

// -----------------------------------------------------------------------
// model/ack — device confirms it applied a model selection.
// {"model_id": "uuid"}
// -----------------------------------------------------------------------
type modelAckPayload struct {
	ModelID string `json:"model_id"`
}

func (p *Processor) handleModelAck(ctx context.Context, tenantCode, deviceID string, payload []byte) {
	tenantID, err := p.db.FetchTenantIDByCode(ctx, tenantCode)
	if err != nil {
		p.logger.Error("Unknown tenant_code",
			zap.String("tenant_code", tenantCode),
			zap.Error(err),
		)
		return
	}

	var ack modelAckPayload
	if err := json.Unmarshal(payload, &ack); err != nil {
		p.logger.Warn("Bad model/ack payload",
			zap.String("device_id", deviceID),
			zap.Error(err),
		)
		return
	}

	if err := p.db.UpsertDevice(ctx, db.DeviceStatus{
		DeviceID:       deviceID,
		TenantID:       tenantID,
		Online:         true,
		CurrentModelID: &ack.ModelID,
	}); err != nil {
		p.logger.Error("model/ack upsert failed",
			zap.String("device_id", deviceID),
			zap.Error(err),
		)
		return
	}

	p.logger.Info("Model ack received",
		zap.String("device_id", deviceID),
		zap.String("model_id", ack.ModelID),
	)
}
