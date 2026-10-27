package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochigome-git/goweight-bridge/internal/config"
	"github.com/mochigome-git/goweight-bridge/internal/db"
	"github.com/mochigome-git/goweight-bridge/internal/models"
	mqttclient "github.com/mochigome-git/goweight-bridge/internal/mqtt"
	"github.com/mochigome-git/goweight-bridge/internal/processor"
	supabase "github.com/mochigome-git/goweight-bridge/pkg/supabase-realtime"
	"go.uber.org/zap"
)

func main() {
	// ------------------------------------------------------------------ logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// ------------------------------------------------------------------ config
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Config load failed", zap.Error(err))
	}

	// ------------------------------------------------------------------ DB
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	database, err := db.New(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("DB init failed", zap.Error(err))
	}
	defer database.Close()

	// ------------------------------------------------------------------ MQTT
	var proc *processor.Processor

	mqttClient, err := mqttclient.New(mqttclient.Options{
		Broker:   cfg.MQTTBroker,
		Port:     cfg.MQTTPort,
		ClientID: cfg.MQTTClientID,
		Username: cfg.MQTTUsername,
		Password: cfg.MQTTPassword,
		UseTLS:   cfg.MQTTUseTLS,
		CACert:   cfg.MQTTCACert,
	}, logger, func(c mqtt.Client) {
		// Subscribe only to inbound device topics: {tenant_code}/{device_id}/{type}
		topics := map[string]byte{
			"+/+/status":    1,
			"+/+/model/ack": 1,
			"+/+/lot/new":   1,
			"+/+/lot/get":   1,
			"+/models/push": 1,
		}
		token := c.SubscribeMultiple(topics, func(client mqtt.Client, msg mqtt.Message) {
			if proc != nil {
				proc.HandleMessage(client, msg)
			}
		})
		token.Wait()
		if token.Error() != nil {
			logger.Error("MQTT subscribe failed", zap.Error(token.Error()))
		} else {
			logger.Info("Subscribed to device topics",
				zap.Strings("topics", []string{"+/+/status", "+/+/model/ack", "+/+/lot/new", "+/+/lot/get", "+/models/push"}),
			)
		}
	})
	if err != nil {
		logger.Fatal("MQTT init failed", zap.Error(err))
	}
	defer mqttClient.Disconnect(250)

	// ------------------------------------------------------------------ Publisher + Processor
	publisher := models.NewPublisher(database, mqttClient, logger)
	proc = processor.New(database, publisher, logger)

	// ------------------------------------------------------------------ Supabase Realtime
	// Extract project ref from SUPABASE_URL: https://<ref>.supabase.co
	u := strings.TrimPrefix(cfg.SupabaseURL, "https://")
	projectRef := strings.Split(u, ".")[0]

	rtClient := supabase.CreateRealtimeClient(projectRef, cfg.SupabaseKey, logger)

	if err := rtClient.Connect(); err != nil {
		logger.Fatal("Supabase Realtime connect failed", zap.Error(err))
	}
	defer rtClient.Disconnect()

	// Listen for weight_models changes → push retained model list to tenant topic
	if err := rtClient.ListenToPostgresChanges(supabase.PostgresChangesOptions{
		Schema: cfg.SupabaseRealtimeSchema,
		Table:  cfg.SupabaseRealtimeTable,
	}, func(payload map[string]any) {
		tenantID, ok := extractTenantID(payload)
		if !ok {
			logger.Warn("weight_models change missing tenant_id", zap.Any("payload", payload))
			return
		}
		logger.Info("weight_models changed, pushing to tenant",
			zap.String("tenant_id", tenantID),
		)
		if err := publisher.PushModelsToTenant(ctx, tenantID); err != nil {
			logger.Error("PushModelsToTenant failed",
				zap.String("tenant_id", tenantID),
				zap.Error(err),
			)
		}
	}); err != nil {
		logger.Fatal("Supabase Realtime subscribe failed", zap.Error(err))
	}

	logger.Info("goweight-bridge running",
		zap.String("mqtt_broker", cfg.MQTTBroker),
	)

	// ------------------------------------------------------------------ shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")
}

// extractTenantID navigates the Supabase Realtime postgres_changes payload:
// payload -> payload -> data -> record -> tenant_id
func extractTenantID(payload map[string]any) (string, bool) {
	p, ok := payload["payload"].(map[string]any)
	if !ok {
		return "", false
	}
	data, ok := p["data"].(map[string]any)
	if !ok {
		return "", false
	}
	// INSERT/UPDATE → record; DELETE → old_record
	for _, key := range []string{"record", "old_record"} {
		if rec, ok := data[key].(map[string]any); ok {
			if tid, ok := rec["tenant_id"].(string); ok && tid != "" {
				return tid, true
			}
		}
	}
	return "", false
}

// keep unused import happy if needed
var _ = fmt.Sprintf
