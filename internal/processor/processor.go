package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

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
func (p *Processor) HandleMessage(client mqtt.Client, msg mqtt.Message) {
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
	case "lot/new":
		p.handleLotNew(ctx, client, tenantCode, deviceID, payload)
	case "lot/get":
		p.handleLotGet(ctx, client, tenantCode, deviceID, payload)
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
	MachineState   string  `json:"machine_state"`
	CurrentModelID *string `json:"current_model_id"`
	LegacyModelID  *string `json:"model_id"`
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

	mode := s.Mode
	switch s.MachineState {
	case "running":
		mode = "run"
	case "idle":
		mode = "idle"
	}
	modelID := s.CurrentModelID
	if modelID == nil {
		modelID = s.LegacyModelID
	}
	if modelID != nil && *modelID == "" {
		modelID = nil
	}

	if err := p.db.UpsertDevice(ctx, db.DeviceStatus{
		DeviceID:       deviceID,
		TenantID:       tenantID,
		Online:         s.Online,
		Mode:           mode,
		CurrentModelID: modelID,
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

// -----------------------------------------------------------------------
// lot/new - operator pressed NEW LOT on the HMI
// {"request_id":"uuid","machine_id":"uuid","device_id":"...",
//
//	"tenant_id":"uuid","requested_at":"..."}
//
// Action: INSERT INTO production.lot (machine_id)
// Reply (not retained): {tenant}/{device}/lot/ack
// -----------------------------------------------------------------------

type lotNewPayload struct {
	RequestID   string `json:"request_id"`
	MachineID   string `json:"machine_id"`
	DeviceID    string `json:"device_id"`
	TenantID    string `json:"tenant_id"`
	RequestedAt string `json:"requested_at"`
}

type lotAckPayload struct {
	RequestID string          `json:"request_id"`
	OK        bool            `json:"ok"`
	Duplicate bool            `json:"duplicate,omitempty"`
	Query     bool            `json:"query,omitempty"` // true = answer to lot/get (no lot created)
	Lot       json.RawMessage `json:"lot,omitempty"`   // inserted production.lot row
	Error     string          `json:"error,omitempty"`
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// QoS1 can redeliver the same request (reconnect, broker retry). production.lot
// has no request_id column, so dedupe here: a request_id seen in the last
// 10 minutes is acked again without inserting a second lot.
const lotDedupeTTL = 10 * time.Minute

type lotSeen struct {
	at  time.Time
	ack lotAckPayload
}

var (
	lotMu      sync.Mutex
	lotSeenMap = map[string]lotSeen{}
)

func lotLookup(reqID string) (lotAckPayload, bool) {
	lotMu.Lock()
	defer lotMu.Unlock()
	now := time.Now()
	for k, v := range lotSeenMap { // tiny map, prune inline
		if now.Sub(v.at) > lotDedupeTTL {
			delete(lotSeenMap, k)
		}
	}
	v, ok := lotSeenMap[reqID]
	return v.ack, ok
}

func lotRemember(reqID string, ack lotAckPayload) {
	lotMu.Lock()
	lotSeenMap[reqID] = lotSeen{at: time.Now(), ack: ack}
	lotMu.Unlock()
}

func (p *Processor) handleLotNew(ctx context.Context, client mqtt.Client,
	tenantCode, deviceID string, payload []byte) {

	var req lotNewPayload
	if err := json.Unmarshal(payload, &req); err != nil {
		p.logger.Warn("Bad lot/new payload", zap.String("device_id", deviceID), zap.Error(err))
		return // no request_id to ack
	}

	ack := lotAckPayload{RequestID: req.RequestID}
	defer func() { p.publishLotAck(client, tenantCode, deviceID, ack) }()

	switch {
	case !uuidRe.MatchString(req.RequestID):
		ack.Error = "invalid request_id"
	case !uuidRe.MatchString(req.MachineID):
		ack.Error = "invalid machine_id"
	case req.DeviceID != "" && req.DeviceID != deviceID:
		ack.Error = "device_id does not match topic"
	}
	if ack.Error != "" {
		p.logger.Warn("lot/new rejected", zap.String("device_id", deviceID), zap.String("reason", ack.Error))
		return
	}

	// Redelivery of a request we already inserted -> same answer, no new lot
	if prev, ok := lotLookup(req.RequestID); ok && prev.OK {
		ack = prev
		ack.Duplicate = true
		p.logger.Info("lot/new duplicate ignored",
			zap.String("device_id", deviceID), zap.String("request_id", req.RequestID))
		return
	}

	tenantID, err := p.db.FetchTenantIDByCode(ctx, tenantCode)
	if err != nil {
		ack.Error = "unknown tenant"
		p.logger.Error("Unknown tenant_code", zap.String("tenant_code", tenantCode), zap.Error(err))
		return
	}
	if req.TenantID != "" && req.TenantID != tenantID {
		ack.Error = "tenant_id does not match topic"
		p.logger.Warn("lot/new tenant mismatch",
			zap.String("topic_tenant", tenantID), zap.String("payload_tenant", req.TenantID))
		return
	}

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	row, err := p.db.InsertLot(dbCtx, req.MachineID)
	if err != nil {
		ack.Error = "db error"
		return // not remembered -> a retry from the HMI can succeed
	}

	ack.OK = true
	ack.Lot = row
	lotRemember(req.RequestID, ack)

	p.logger.Info("lot/new handled",
		zap.String("device_id", deviceID),
		zap.String("machine_id", req.MachineID),
		zap.String("request_id", req.RequestID))
}

// -----------------------------------------------------------------------
// lot/get - HMI asks for the machine's current lot (boot / reconnect)
// {"request_id":"uuid","machine_id":"uuid"}
//
// Reply on the same {tenant}/{device}/lot/ack topic with query=true.
// ok=true and no "lot" field means the machine has no active lot.
// -----------------------------------------------------------------------

type lotGetPayload struct {
	RequestID string `json:"request_id"`
	MachineID string `json:"machine_id"`
}

func (p *Processor) handleLotGet(ctx context.Context, client mqtt.Client,
	tenantCode, deviceID string, payload []byte) {

	var req lotGetPayload
	if err := json.Unmarshal(payload, &req); err != nil {
		p.logger.Warn("Bad lot/get payload", zap.String("device_id", deviceID), zap.Error(err))
		return
	}

	ack := lotAckPayload{RequestID: req.RequestID, Query: true}
	defer func() { p.publishLotAck(client, tenantCode, deviceID, ack) }()

	if !uuidRe.MatchString(req.MachineID) {
		ack.Error = "invalid machine_id"
		return
	}

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	row, err := p.db.GetActiveLot(dbCtx, req.MachineID)
	if err != nil {
		ack.Error = "db error"
		return
	}

	ack.OK = true
	ack.Lot = row // nil -> omitted -> "no active lot"
	p.logger.Info("lot/get handled",
		zap.String("device_id", deviceID),
		zap.String("machine_id", req.MachineID),
		zap.Bool("found", row != nil))
}

// Publishing from inside a Paho message handler must not block on the token
// (can deadlock with ordered delivery), so wait in a goroutine.
func (p *Processor) publishLotAck(client mqtt.Client, tenantCode, deviceID string, ack lotAckPayload) {
	if client == nil || ack.RequestID == "" {
		return
	}
	b, err := json.Marshal(ack)
	if err != nil {
		return
	}
	topic := fmt.Sprintf("%s/%s/lot/ack", tenantCode, deviceID)
	tok := client.Publish(topic, 1, false, b)
	go func() {
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			p.logger.Warn("lot/ack publish failed", zap.String("topic", topic), zap.Error(tok.Error()))
		}
	}()
}
