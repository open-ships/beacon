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
	wg      sync.WaitGroup
	stop    sync.Once
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
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := waitMQTTToken(runCtx, s.client.Connect()); err != nil && runCtx.Err() == nil {
			s.setState("degraded", err)
			log.Warn("mqtt sink connect failed; reconnecting", "sink", cfg.ID, "err", err)
		}
		<-runCtx.Done()
		// Disconnect even when the initial connection has not completed.
		// ConnectRetry is owned by Paho rather than runCtx; without an explicit
		// Disconnect an unreachable broker keeps its retry goroutine alive after
		// this sink has been deleted or replaced.
		s.client.Disconnect(mqttDisconnectMsec)
	}()
	return s, nil
}

func (s *mqttSink) ID() string                   { return s.id }
func (s *mqttSink) DeliveryClass() DeliveryClass { return DeliveryBestEffort }

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

func (s *mqttSink) Broadcast(entries []queue.Entry) BroadcastReport {
	report := BroadcastReport{Accepted: make([]bool, len(entries))}
	if !s.client.IsConnected() {
		err := errors.New("mqtt broker not connected")
		s.setState("degraded", err)
		report.Err = err
		return report
	}
	for i, e := range entries {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			report.Err = err
			continue
		}
		token := s.client.Publish(s.topic, 0, false, doc)
		if !token.WaitTimeout(mqttPublishWait) {
			err := errors.New("mqtt publish timed out")
			s.setState("degraded", err)
			report.Err = err
			continue
		}
		if err := token.Error(); err != nil {
			s.setState("degraded", err)
			report.Err = err
			continue
		}
		s.setState("up", nil)
		report.Accepted[i] = true
	}
	return report
}

func (s *mqttSink) Stop() {
	s.stop.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
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
