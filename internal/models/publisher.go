package models

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochigome-git/goweight-bridge/internal/db"
	"go.uber.org/zap"
)

// Publisher fetches models from DB and pushes them to devices via MQTT.
type Publisher struct {
	db         *db.Pool
	mqttClient mqtt.Client
	logger     *zap.Logger
}

func NewPublisher(database *db.Pool, mqttClient mqtt.Client, logger *zap.Logger) *Publisher {
	return &Publisher{
		db:         database,
		mqttClient: mqttClient,
		logger:     logger,
	}
}

type modelSyncPayload struct {
	Timestamp string           `json:"timestamp"`
	Models    []db.WeightModel `json:"models"`
}

// PushModelsToTenant fetches all models for tenantID and publishes one retained
// message to {tenant_code}/models/sync — all devices under the tenant receive it.
func (p *Publisher) PushModelsToTenant(ctx context.Context, tenantID string) error {
	models, err := p.db.FetchModelsForTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("fetch models: %w", err)
	}

	tenantCode, err := p.db.FetchTenantCode(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("fetch tenant_code: %w", err)
	}

	data, err := json.Marshal(modelSyncPayload{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Models:    models,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	topic := fmt.Sprintf("%s/models/sync", tenantCode)

	token := p.mqttClient.Publish(topic, 1, true, data) // QoS 1, retained
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("mqtt publish: %w", token.Error())
	}

	p.logger.Info("Models pushed to tenant",
		zap.String("topic", topic),
		zap.String("tenant_id", tenantID),
		zap.Int("model_count", len(models)),
	)
	return nil
}
