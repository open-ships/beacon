package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/mqtttransport"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/retry"
)

const (
	mqttDisconnectMsec      = 250
	mqttPublishWriteTimeout = 5 * time.Second
	mqttStableConnection    = 30 * time.Second
)

var (
	mqttReconnectMin      = 2 * time.Second
	mqttReconnectMax      = time.Minute
	mqttPublishAckTimeout = 30 * time.Second
)

// mqttSink publishes connector entries to a broker topic with MQTT QoS 1.
// Push returns successfully only after the broker acknowledges the PUBLISH;
// subscriber receipt is outside this delivery boundary. An acknowledgement
// lost after broker acceptance is retried by the connector, so delivery is
// intentionally at least once and consumers must tolerate duplicates.
type mqttSink struct {
	id     string
	topic  string
	broker string
	cancel context.CancelFunc
	// One broker-confirmed publish may be ambiguous at a time even when many
	// Connector routes share this sink. This bounds token/Envelope retention
	// during a stalled acknowledgement and makes outage recovery gradual.
	publishSlot chan struct{}

	mu             sync.Mutex
	state          string
	lastErr        error
	active         *mqttClientGeneration
	nextGeneration uint64
	wg             sync.WaitGroup
	stop           sync.Once
}

// mqttClientGeneration owns exactly one Paho client and one broker
// connection. Paho publish tokens do not provide Beacon's confirmation
// boundary when its automatic reconnect resumes an in-flight QoS 1 packet, and
// Paho explicitly discourages manually reconnecting a used Client. Beacon
// therefore discards the whole generation after any connection or publish
// failure. Closing done invalidates every waiter before another generation can
// become active.
type mqttClientGeneration struct {
	id             uint64
	client         mqtt.Client
	done           chan struct{}
	closeTransport func()

	mu      sync.Mutex
	err     error
	invalid sync.Once
}

func (g *mqttClientGeneration) invalidate(err error) {
	if err == nil {
		err = errors.New("mqtt client generation invalidated")
	}
	g.invalid.Do(func() {
		g.mu.Lock()
		g.err = err
		g.mu.Unlock()
		close(g.done)
	})
}

func (g *mqttClientGeneration) invalidated() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *mqttClientGeneration) error() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	return errors.New("mqtt client generation is no longer active")
}

func newMQTTSink(ctx context.Context, cfg model.Sink, log *slog.Logger, _ *metrics.Set) (Runtime, error) {
	if log == nil {
		log = slog.Default()
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &mqttSink{
		id: cfg.ID, topic: cfg.Topic, broker: model.NormalizeMQTTBrokerURL(cfg.URL),
		cancel: cancel, state: "degraded",
		publishSlot: make(chan struct{}, 1),
	}
	s.wg.Add(1)
	go s.connectLoop(runCtx, log)
	return s, nil
}

func (s *mqttSink) newGeneration() *mqttClientGeneration {
	s.mu.Lock()
	s.nextGeneration++
	generationID := s.nextGeneration
	s.mu.Unlock()

	g := &mqttClientGeneration{id: generationID, done: make(chan struct{})}
	opts := mqtt.NewClientOptions()
	g.closeTransport = mqtttransport.Configure(opts)
	opts.AddBroker(s.broker)
	opts.SetClientID(mqttClientID("beacon-sink", s.id))
	opts.SetCleanSession(true)
	// Paho's automatic reconnect deliberately completes the original publish
	// token before a replayed QoS 1 packet receives PUBACK. Beacon cannot use
	// that weaker boundary: a connection loss must fail the in-flight Push so
	// the durable Connector queue remains pending and performs the retry. A
	// fresh Paho client is constructed for every explicit connection generation;
	// a used client is never reconnected.
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	// Publish is synchronous until Paho hands the packet to its outbound
	// worker. Bound that handoff so a connection-loss race cannot hold the
	// Connector goroutine for Paho's longer fallback timeout.
	opts.SetWriteTimeout(mqttPublishWriteTimeout)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		if err == nil {
			err = errors.New("mqtt connection lost")
		}
		s.invalidateGeneration(g, err)
	})
	g.client = mqtt.NewClient(opts)
	return g
}

func (s *mqttSink) connectLoop(ctx context.Context, log *slog.Logger) {
	defer s.wg.Done()
	backoff := retry.NewBackoff(mqttReconnectMin, mqttReconnectMax)
	failures := 0
	for {
		g := s.newGeneration()
		if err := waitMQTTToken(ctx, g.client.Connect()); err != nil {
			s.retireGeneration(g, err)
			if ctx.Err() != nil {
				return
			}
			s.setState("degraded", err)
			failures++
			delay := backoff.Next()
			if failures == 1 || failures%60 == 0 {
				log.Warn("mqtt sink connect failed; retrying", "sink", s.id, "err", err,
					"retry_in", delay, "consecutive_failures", failures)
			} else {
				log.Debug("mqtt sink still disconnected", "sink", s.id, "err", err,
					"retry_in", delay, "consecutive_failures", failures)
			}
			if !retry.Sleep(ctx, delay) {
				return
			}
			continue
		}
		if ctx.Err() != nil {
			s.retireGeneration(g, ctx.Err())
			return
		}
		if g.invalidated() || !g.client.IsConnectionOpen() {
			err := g.error()
			if !g.invalidated() {
				err = errors.New("mqtt connection closed during handshake")
			}
			s.retireGeneration(g, err)
			s.setState("degraded", err)
			failures++
			delay := backoff.Next()
			if failures == 1 || failures%60 == 0 {
				log.Warn("mqtt sink connection closed during setup; retrying", "sink", s.id,
					"generation", g.id, "err", err, "retry_in", delay,
					"consecutive_failures", failures)
			} else {
				log.Debug("mqtt sink connection still unstable", "sink", s.id,
					"generation", g.id, "err", err, "retry_in", delay,
					"consecutive_failures", failures)
			}
			if !retry.Sleep(ctx, delay) {
				return
			}
			continue
		}
		if !s.activateGeneration(g) {
			err := errors.New("mqtt connection invalidated before activation")
			s.retireGeneration(g, err)
			s.setState("degraded", err)
			if !retry.Sleep(ctx, backoff.Next()) {
				return
			}
			continue
		}
		connectedAt := time.Now()
		select {
		case <-ctx.Done():
			s.retireGeneration(g, ctx.Err())
			return
		case <-g.done:
			// The generation is no longer publishable. Discard its Paho client
			// rather than reconnecting it; the Connector retains and retries any
			// QoS 1 packet whose PUBACK was not observed.
			err := g.error()
			s.retireGeneration(g, err)
			// A broker that reaches CONNACK and immediately drops must not reset
			// outage history forever. Only sustained useful connectivity earns a
			// return to the minimum retry interval.
			if time.Since(connectedAt) >= mqttStableConnection {
				backoff.Reset()
				failures = 0
			}
			failures++
			delay := backoff.Next()
			if failures == 1 || failures%60 == 0 {
				log.Warn("mqtt sink connection lost; retrying", "sink", s.id,
					"generation", g.id, "err", err, "retry_in", delay,
					"consecutive_failures", failures)
			} else {
				log.Debug("mqtt sink connection still unstable", "sink", s.id,
					"generation", g.id, "err", err, "retry_in", delay,
					"consecutive_failures", failures)
			}
			if !retry.Sleep(ctx, delay) {
				return
			}
		}
	}
}

func (s *mqttSink) activateGeneration(g *mqttClientGeneration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.invalidated() || !g.client.IsConnectionOpen() || s.active != nil {
		return false
	}
	s.active = g
	s.state, s.lastErr = "up", nil
	return true
}

func (s *mqttSink) invalidateGeneration(g *mqttClientGeneration, err error) {
	if err == nil {
		err = errors.New("mqtt client generation invalidated")
	}
	s.mu.Lock()
	// Invalidation and removal from the active slot are one logical state
	// transition. A Push taking the slot after this unlock cannot acquire g;
	// one that acquired it earlier observes the already-closed done channel.
	g.invalidate(err)
	if s.active == g {
		s.active = nil
		s.state, s.lastErr = "degraded", err
	}
	s.mu.Unlock()
}

func (s *mqttSink) retireGeneration(g *mqttClientGeneration, err error) {
	s.invalidateGeneration(g, err)
	g.closeTransport()
	// This is the generation's only Disconnect call. It is never used again:
	// the next attempt constructs a fresh Paho client.
	g.client.Disconnect(mqttDisconnectMsec)
}

func (s *mqttSink) ID() string                   { return s.id }
func (s *mqttSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

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

func (s *mqttSink) activeGeneration() *mqttClientGeneration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *mqttSink) Push(ctx context.Context, env *msg.Envelope) error {
	select {
	case s.publishSlot <- struct{}{}:
		defer func() { <-s.publishSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	g := s.activeGeneration()
	if g == nil || g.invalidated() || !g.client.IsConnectionOpen() {
		err := errors.New("mqtt broker not connected")
		if g != nil {
			s.invalidateGeneration(g, err)
		} else {
			s.setState("degraded", err)
		}
		return err
	}
	doc, err := env.WireBytes()
	if err != nil {
		return err
	}
	// Recheck the atomically-swapped generation immediately before handing the
	// packet to Paho. If it was invalidated or replaced, this Push belongs to no
	// live broker connection and must remain pending in the Connector queue.
	if current := s.activeGeneration(); current != g || g.invalidated() || !g.client.IsConnectionOpen() {
		err := errors.New("mqtt connection generation changed before publish")
		s.invalidateGeneration(g, err)
		return err
	}
	// Wait for PUBACK within the attempt deadline. The generation's done
	// channel also terminates the wait if connection loss races registration.
	// Any unsuccessful wait retires the client before a retry, bounding Paho's
	// retained tokens and preventing an ambiguous packet from confirming one.
	token := g.client.Publish(s.topic, 1, false, doc)
	// Publish checks Paho's connection status internally, but connection-loss
	// cleanup and token registration are concurrent inside Paho. Accept an
	// already-completed token (including a real PUBACK), otherwise refuse to
	// await a token if the generation died or was swapped during registration.
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			s.invalidateGeneration(g, err)
			return err
		}
	default:
		if current := s.activeGeneration(); current != g || g.invalidated() || !g.client.IsConnectionOpen() {
			err := errors.New("mqtt connection generation changed during publish")
			s.invalidateGeneration(g, err)
			return err
		}
	}
	if err := waitMQTTPublish(ctx, g, token); err != nil {
		s.invalidateGeneration(g, err)
		return err
	}
	s.mu.Lock()
	if s.active == g {
		s.state, s.lastErr = "up", nil
	}
	s.mu.Unlock()
	return nil
}

func (s *mqttSink) Stop() {
	s.stop.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
}

func waitMQTTToken(ctx context.Context, token mqtt.Token) error {
	ctx, cancel := context.WithTimeout(ctx, mqtttransport.ConnectTimeout)
	defer cancel()
	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitMQTTPublish(ctx context.Context, generation *mqttClientGeneration, token mqtt.Token) error {
	// A responsive broker can still withhold PUBACK indefinitely. End the
	// attempt and retire its generation before retrying from durable storage.
	ctx, cancel := context.WithTimeout(ctx, mqttPublishAckTimeout)
	defer cancel()
	select {
	case <-token.Done():
		// With automatic reconnect disabled, nil is reachable only through the
		// QoS 1 PUBACK path. A generation loss completes outstanding Paho tokens
		// with an error; generation.done covers the narrow callback race as well.
		return token.Error()
	case <-generation.done:
		return generation.error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mqttClientID(prefix, id string) string {
	return fmt.Sprintf("%s-%s", prefix, id)
}
