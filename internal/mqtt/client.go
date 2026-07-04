package mqttclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type Options struct {
	Broker   string
	Port     string
	ClientID string
	Username string
	Password string
	UseTLS   bool
	CACert   string
}

func New(opts Options, logger *zap.Logger, onConnect func(mqtt.Client)) (mqtt.Client, error) {
	broker := fmt.Sprintf("tcp://%s:%s", opts.Broker, opts.Port)
	if opts.UseTLS {
		broker = fmt.Sprintf("ssl://%s:%s", opts.Broker, opts.Port)
	}

	mqttOpts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(opts.ClientID).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetCleanSession(true)

	if opts.Username != "" {
		mqttOpts.SetUsername(opts.Username)
		mqttOpts.SetPassword(opts.Password)
	}

	if opts.UseTLS {
		tlsCfg := &tls.Config{}
		if opts.CACert != "" {
			// Replace literal \n with real newlines in case env var uses \n
			pemData := strings.ReplaceAll(opts.CACert, `\n`, "\n")
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(pemData)) {
				return nil, fmt.Errorf("failed to parse CA cert")
			}
			tlsCfg.RootCAs = pool
		}
		mqttOpts.SetTLSConfig(tlsCfg)
	}

	mqttOpts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		logger.Warn("MQTT connection lost", zap.Error(err))
	})

	mqttOpts.SetReconnectingHandler(func(c mqtt.Client, opts *mqtt.ClientOptions) {
		logger.Info("MQTT reconnecting...")
	})

	mqttOpts.SetOnConnectHandler(func(c mqtt.Client) {
		logger.Info("MQTT connected", zap.String("broker", broker))
		if onConnect != nil {
			onConnect(c)
		}
	})

	client := mqtt.NewClient(mqttOpts)
	token := client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt connect: %w", err)
	}

	return client, nil
}
