package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	// Supabase DB (direct connection)
	SupabaseDBHost     string
	SupabaseDBName     string
	SupabaseDBPassword string
	SupabaseDBPort     string
	SupabaseDBUser     string

	// Supabase API
	SupabaseURL string
	SupabaseKey string // service role key

	// Supabase Realtime
	SupabaseRealtimeSchema string
	SupabaseRealtimeTable  string

	// MQTT
	MQTTBroker   string
	MQTTPort     string
	MQTTClientID string
	MQTTUsername string
	MQTTPassword string
	MQTTUseTLS   bool
	MQTTCACert   string

	// App
	TopicPrefix string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		SupabaseDBHost:     os.Getenv("SUPABASE_DB_HOST"),
		SupabaseDBName:     os.Getenv("SUPABASE_DB_NAME"),
		SupabaseDBPassword: os.Getenv("SUPABASE_DB_PASSWORD"),
		SupabaseDBPort:     getEnvOrDefault("SUPABASE_DB_PORT", "5432"),
		SupabaseDBUser:     os.Getenv("SUPABASE_DB_USER"),

		SupabaseURL: os.Getenv("SUPABASE_URL"),
		SupabaseKey: os.Getenv("SUPABASE_KEY"),

		SupabaseRealtimeSchema: getEnvOrDefault("SUPABASE_REALTIME_SCHEMA", "app"),
		SupabaseRealtimeTable:  getEnvOrDefault("SUPABASE_REALTIME_TABLE", "weight_models"),

		MQTTBroker:   getEnvOrDefault("MQTT_BROKER", "localhost"),
		MQTTPort:     getEnvOrDefault("MQTT_PORT", "1883"),
		MQTTClientID: getEnvOrDefault("MQTT_CLIENT_ID", "goweight-bridge"),
		MQTTUsername: os.Getenv("MQTT_USERNAME"),
		MQTTPassword: os.Getenv("MQTT_PASSWORD"),
		MQTTUseTLS:   os.Getenv("MQTT_USE_TLS") == "true",
		MQTTCACert:   os.Getenv("MQTT_CA_CERT"),

		TopicPrefix: getEnvOrDefault("TOPIC_PREFIX", "gim"),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	required := map[string]string{
		"SUPABASE_DB_HOST":     c.SupabaseDBHost,
		"SUPABASE_DB_NAME":     c.SupabaseDBName,
		"SUPABASE_DB_PASSWORD": c.SupabaseDBPassword,
		"SUPABASE_DB_USER":     c.SupabaseDBUser,
		"SUPABASE_URL":         c.SupabaseURL,
		"SUPABASE_KEY":         c.SupabaseKey,
	}
	for k, v := range required {
		if v == "" {
			return fmt.Errorf("missing required env var: %s", k)
		}
	}
	return nil
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
