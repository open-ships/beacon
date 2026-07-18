package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

const (
	mqttTokenPoll      = 100 * time.Millisecond
	mqttPublishWait    = 2 * time.Second
	mqttDisconnectMsec = 250
)

// mqttSink publishes live connector entries to a broker topic. It is a
// Broadcaster: broker outages show as degraded and new entries are skipped
// until Paho's reconnect loop restores the session.
type mqttSink struct {
	id     string
	topic  string
	client mqtt.Client
	cancel context.CancelFunc

	mu      sync.Mutex
	state   string
	lastErr error
}

func newMQTTSink(ctx context.Context, cfg model.Sink, log *slog.Logger, _ *metrics.Set) (Runtime, error) {
	if log == nil {
		log = slog.Default()
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &mqttSink{id: cfg.ID, topic: cfg.Topic, cancel: cancel, state: "degraded"}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(model.NormalizeMQTTBrokerURL(cfg.URL))
	opts.SetClientID(mqttClientID("beacon-sink", cfg.ID))
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		s.setState("up", nil)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		if err == nil {
			err = errors.New("mqtt connection lost")
		}
		s.setState("degraded", err)
	})

	s.client = mqtt.NewClient(opts)
	go func() {
		if err := waitMQTTToken(runCtx, s.client.Connect()); err != nil && runCtx.Err() == nil {
			s.setState("degraded", err)
			log.Warn("mqtt sink connect failed; reconnecting", "sink", cfg.ID, "err", err)
		}
		<-runCtx.Done()
		if s.client.IsConnected() {
			s.client.Disconnect(mqttDisconnectMsec)
		}
	}()
	return s, nil
}

func (s *mqttSink) ID() string { return s.id }

func (s *mqttSink) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.lastErr
}

func (s *mqttSink) setState(state string, err error) {
	s.mu.Lock()
	s.state, s.lastErr = state, err
	s.mu.Unlock()
}

func (s *mqttSink) Broadcast(entries []queue.Entry) {
	if !s.client.IsConnected() {
		s.setState("degraded", errors.New("mqtt broker not connected"))
		return
	}
	for _, e := range entries {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			continue
		}
		token := s.client.Publish(s.topic, 0, false, doc)
		if !token.WaitTimeout(mqttPublishWait) {
			s.setState("degraded", errors.New("mqtt publish timed out"))
			continue
		}
		if err := token.Error(); err != nil {
			s.setState("degraded", err)
			continue
		}
		s.setState("up", nil)
	}
}

func (s *mqttSink) Stop() {
	s.cancel()
}

func waitMQTTToken(ctx context.Context, token mqtt.Token) error {
	for {
		if token.WaitTimeout(mqttTokenPoll) {
			return token.Error()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func mqttClientID(prefix, id string) string {
	return fmt.Sprintf("%s-%s", prefix, id)
}
