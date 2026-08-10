package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

func runMQTT(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	lost := make(chan error, 1)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(model.NormalizeMQTTBrokerURL(cfg.URL))
	opts.SetClientID(mqttClientID("beacon-source", cfg.ID))
	opts.SetCleanSession(true)
	// The enclosing dialer source owns reconnect/backoff. Paho auto-reconnect
	// here would race a second client created by that loop after ConnectionLost
	// and retain duplicate subscriptions during a long outage.
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		if err == nil {
			err = errors.New("mqtt connection lost")
		}
		select {
		case lost <- err:
		default:
		}
	})

	client := mqtt.NewClient(opts)
	// Disconnect also aborts an in-progress Connect. Register cleanup before
	// waiting so cancellation cannot return from waitMQTTToken while Paho keeps
	// an unowned dial alive (or completes it after this source has stopped).
	defer client.Disconnect(250)
	if err := waitMQTTToken(ctx, client.Connect()); err != nil {
		return err
	}

	handler := func(_ mqtt.Client, m mqtt.Message) {
		if len(m.Payload()) > msg.MaxWireEnvelopeBytes {
			return
		}
		var e msg.Envelope
		if err := json.Unmarshal(m.Payload(), &e); err != nil {
			return
		}
		if err := msg.ValidateRemote(&e, len(m.Payload())); err != nil {
			return
		}
		publish(&e)
	}
	if err := waitMQTTToken(ctx, client.Subscribe(cfg.Topic, 1, handler)); err != nil {
		return err
	}
	connected()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-lost:
		return err
	}
}

func waitMQTTToken(ctx context.Context, token mqtt.Token) error {
	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mqttClientID(prefix, id string) string {
	return fmt.Sprintf("%s-%s", prefix, id)
}
