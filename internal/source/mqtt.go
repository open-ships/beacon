package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/mqtttransport"
	"github.com/open-ships/beacon/internal/msg"
)

var mqttHandshakeTimeout = 15 * time.Second

func runMQTT(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	lost := make(chan error, 1)
	opts := mqtt.NewClientOptions()
	closeTransport := mqtttransport.Configure(opts)
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
	var deliveryMu sync.Mutex
	accepting := true
	// Retire callbacks before returning to the source owner, which can then
	// delete its metrics safely. Paho's Disconnect has a bounded quiesce wait
	// and can return while a handler or CONNECT is still unwinding.
	defer func() {
		deliveryMu.Lock()
		accepting = false
		deliveryMu.Unlock()
		closeTransport()
		client.Disconnect(250)
	}()
	if err := waitMQTTToken(ctx, client.Connect()); err != nil {
		return err
	}

	handler := func(_ mqtt.Client, m mqtt.Message) {
		deliveryMu.Lock()
		defer deliveryMu.Unlock()
		if !accepting || ctx.Err() != nil {
			return
		}
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
	ctx, cancel := context.WithTimeout(ctx, mqttHandshakeTimeout)
	defer cancel()
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
