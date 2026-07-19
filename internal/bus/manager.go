// Package bus owns one n2k.Client per physical CAN endpoint. NMEA 2000
// address claiming allows one bus participant per interface, so sources
// and sinks on the same interface share a client via refcounted handles.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	n2k "github.com/open-ships/n2k"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/stats"
)

// ErrNotEncodable marks an envelope that cannot be re-encoded onto a CAN
// bus — most commonly a PGN with no cataloged decoder (an UnknownPGN
// envelope, which beacon always produces since sources run with
// n2k.IncludeUnknown()). Callers (see sink.canSink.Push) treat this as
// skippable rather than a transient write failure worth retrying forever.
var ErrNotEncodable = errors.New("envelope cannot be encoded for CAN transmission")

const (
	// n2k subscriptions and writes are bounded in v0.3. Keep Beacon's budgets
	// explicit: a larger receive window absorbs short CAN bursts while the
	// manager converts messages, and the write queue retains n2k's conservative
	// default backpressure bound. extraOpts are appended after these defaults so
	// tests and specialized deployments can override either value.
	clientReceiveBuffer = 256
	clientWriteQueue    = 64
)

type Endpoint struct {
	Kind   string // "socketcan" | "usbcan" | "tcp" (NMEA-2000 gateway)
	Name   string // interface name, serial port path, or gateway host:port
	Format string // tcp only: model.StreamFormatYDRaw / model.StreamFormatActisense
}

// EndpointStatus is a point-in-time health snapshot for one shared n2k
// client. Queue and subscriber fields come directly from n2k.Client.Status;
// they make bounded-runtime pressure visible without performing bus I/O.
type EndpointStatus struct {
	Endpoint           string `json:"endpoint"`
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	State              string `json:"state"`
	Err                string `json:"err,omitempty"`
	Address            uint8  `json:"address"`
	AddressClaimed     bool   `json:"address_claimed"`
	Closed             bool   `json:"closed"`
	WriteQueueDepth    int    `json:"write_queue_depth"`
	WriteQueueCapacity int    `json:"write_queue_capacity"`
	ReceiveSubscribers int    `json:"receive_subscribers"`
}

// StreamFormat maps beacon's config stream-format string to n2k's enum,
// shared by the tcp endpoint here and the read-only tcp/udp sources in
// internal/source.
func StreamFormat(f string) (n2k.StreamFormat, error) {
	switch f {
	case model.StreamFormatYDRaw:
		return n2k.FormatYDRaw, nil
	case model.StreamFormatActisense:
		return n2k.FormatActisense, nil
	default:
		return 0, fmt.Errorf("unknown stream format %q", f)
	}
}

func (ep Endpoint) options() ([]n2k.Option, error) {
	switch ep.Kind {
	case "socketcan":
		return []n2k.Option{n2k.CAN(ep.Name)}, nil
	case "usbcan":
		return []n2k.Option{n2k.USB(ep.Name)}, nil
	case "tcp":
		f, err := StreamFormat(ep.Format)
		if err != nil {
			return nil, fmt.Errorf("tcp endpoint %q: %w", ep.Name, err)
		}
		// WithReconnect keeps the gateway transport alive across drops
		// inside one client, so a brief WiFi blip doesn't force a full
		// client teardown + re-claim; only a dead client (Receive ending)
		// falls back to the manager's own reconnect loop.
		return []n2k.Option{n2k.TCP(ep.Name, f), n2k.WithReconnect(n2k.ReconnectPolicy{})}, nil
	default:
		return nil, fmt.Errorf("unknown CAN endpoint kind %q", ep.Kind)
	}
}

func (ep Endpoint) preflight() error {
	if ep.Kind != "usbcan" {
		return nil
	}
	info, err := os.Stat(ep.Name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("usbcan port %q does not exist", ep.Name)
		}
		return fmt.Errorf("usbcan port %q is not accessible: %w", ep.Name, err)
	}
	if info.IsDir() {
		return fmt.Errorf("usbcan port %q is a directory", ep.Name)
	}
	return nil
}

type Manager struct {
	log       *slog.Logger
	met       *metrics.Set
	extraOpts []n2k.Option
	customBus n2k.Bus
	st        *stats.Registry

	mu      sync.Mutex
	clients map[Endpoint]*busClient
}

func (m *Manager) SetStatsRegistry(st *stats.Registry) { m.st = st }

func NewManager(log *slog.Logger, met *metrics.Set, extraOpts ...n2k.Option) *Manager {
	return &Manager{log: log, met: met, extraOpts: extraOpts, clients: map[Endpoint]*busClient{}}
}

// NewManagerWithBus builds a manager over a caller-supplied n2k Bus. It is
// useful for deterministic simulations and embedded transports; unlike a
// physical endpoint, n2k v0.3.0 does not permit WithBus to be combined with
// CAN/USB/TCP source options.
func NewManagerWithBus(log *slog.Logger, met *metrics.Set, customBus n2k.Bus, extraOpts ...n2k.Option) *Manager {
	return &Manager{
		log: log, met: met, extraOpts: extraOpts, customBus: customBus,
		clients: map[Endpoint]*busClient{},
	}
}

func (m *Manager) clientCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients)
}

// Acquire returns a refcounted handle on the endpoint's shared client,
// starting it if this is the first reference.
//
// ctx bounds only the acquisition itself; it does NOT bound the client's
// lifetime. The shared client runs on its own background context because it
// is shared between acquirers with independent lifecycles — it lives until
// the last Handle is Released.
func (m *Manager) Acquire(ctx context.Context, ep Endpoint) (*Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	bc, ok := m.clients[ep]
	if !ok {
		opts, err := ep.options()
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		bc = &busClient{
			mgr: m, ep: ep, opts: opts, cancel: cancel,
			subs:      map[int64]busSubscriber{},
			supported: map[uint64]SupportedPGNs{},
			requested: map[uint64]time.Time{},
			state:     "degraded",
		}
		m.clients[ep] = bc
		bc.wg.Add(1)
		go bc.run(runCtx)
	}
	bc.refs++
	return &Handle{bc: bc}, nil
}

type busClient struct {
	mgr    *Manager
	ep     Endpoint
	opts   []n2k.Option
	cancel context.CancelFunc
	wg     sync.WaitGroup // tracks the run goroutine; waited on by the last Release

	// refs is guarded by mgr.mu (not this mu): Acquire and Release both
	// mutate it while holding the manager lock, which also serializes it
	// with the clients-map bookkeeping.
	refs int

	mu      sync.Mutex
	client  *n2k.Client
	subs    map[int64]busSubscriber
	nextSub int64
	state   string
	lastErr error

	supported map[uint64]SupportedPGNs
	requested map[uint64]time.Time
	requestWG sync.WaitGroup
}

type busSubscriber struct {
	label string
	ch    chan *msg.Envelope
}

// run maintains the client: (re)connect, pump Receive into subscribers,
// back off on failure, until cancelled by the last Release.
func (bc *busClient) run(ctx context.Context) {
	defer bc.wg.Done()
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		if err := bc.ep.preflight(); err != nil {
			bc.setState("error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
			continue
		}
		var opts []n2k.Option
		if bc.mgr.customBus != nil {
			opts = append(opts, n2k.WithBus(bc.mgr.customBus))
		} else {
			opts = append(opts, bc.opts...)
		}
		opts = append(opts,
			n2k.IncludeUnknown(),
			n2k.WithLogger(bc.mgr.log),
			n2k.WithReceiveBuffer(clientReceiveBuffer),
			n2k.WithWriteQueue(clientWriteQueue),
		)
		opts = append(opts, bc.mgr.extraOpts...)
		client, err := bc.newClient(ctx, opts...)
		if err != nil {
			bc.setState("error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
			continue
		}
		bc.mu.Lock()
		bc.client = client
		bc.mu.Unlock()
		bc.setState("up", nil)
		backoff = 250 * time.Millisecond

		// n2k's real socketcan/usbcan Bus.Run implementations ignore ctx
		// (they only unblock when the underlying socket/port is closed), so
		// cancelling ctx would not by itself end the Receive loop on real
		// hardware. Force-close the client on cancellation; client.Close is
		// idempotent, so racing the loop-exit Close below is harmless.
		iterDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = client.Close()
			case <-iterDone:
			}
		}()

		var receiveErr error
		for m, err := range client.Receive() {
			if err != nil {
				receiveErr = err
				bc.mgr.log.Debug("n2k receive error", "endpoint", bc.ep.Name, "err", err)
				break
			}
			e, err := msg.FromPGN(m)
			if err != nil {
				bc.mgr.log.Debug("envelope conversion error", "err", err)
				continue
			}
			e.Ingress = bc.ep.Kind + ":" + bc.ep.Name
			e.OriginIngress = e.Ingress
			if device, ok := client.DeviceAt(e.Source); ok {
				name := device.RawName
				e.DeviceName = &name
				e.DeviceNameHex = fmt.Sprintf("%016X", name)
				if list, ok := m.(*pgn.ParameterGroupNumberListTransmitAndReceive); ok {
					bc.recordSupportedPGNs(name, list)
				}
			}
			bc.maybeRequestPGNList(ctx, client, e.Source)
			bc.broadcast(ctx, e)
		}
		if receiveErr == nil {
			receiveErr = client.Err()
		}
		bc.requestWG.Wait()
		// Receive ended: client dead or ctx cancelled.
		_ = client.Close()
		close(iterDone) // retire this iteration's close watchdog
		bc.mu.Lock()
		bc.client = nil
		bc.mu.Unlock()
		if ctx.Err() == nil {
			err := errors.New("receive loop ended; reconnecting")
			if receiveErr != nil {
				err = fmt.Errorf("receive loop ended; reconnecting: %w", receiveErr)
			}
			bc.setState("degraded", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
	}
}

func (bc *busClient) newClient(ctx context.Context, opts ...n2k.Option) (client *n2k.Client, err error) {
	defer func() {
		if r := recover(); r != nil {
			if client != nil {
				_ = client.Close()
			}
			client = nil
			err = fmt.Errorf("bus %s:%s: n2k client startup panic: %v", bc.ep.Kind, bc.ep.Name, r)
		}
	}()
	return n2k.NewClient(ctx, opts...)
}

func (bc *busClient) broadcast(ctx context.Context, e *msg.Envelope) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	var dropped int64
	droppedBySource := map[string]int64{}
	for _, sub := range bc.subs {
		select {
		case sub.ch <- e:
		default: // slow subscriber: drop rather than block the bus
			dropped++
			if sub.label != "" {
				droppedBySource[sub.label]++
			}
		}
	}
	if dropped > 0 {
		bc.mgr.met.SourceDrops(ctx, "bus:"+bc.ep.Kind+":"+bc.ep.Name, dropped)
	}
	for source, count := range droppedBySource {
		bc.mgr.met.SourceDrops(ctx, source, count)
		bc.mgr.st.RecordSourceDrops(source, count)
	}
}

func (bc *busClient) setState(state string, err error) {
	bc.mu.Lock()
	bc.state, bc.lastErr = state, err
	bc.mu.Unlock()
	var v int64
	switch state {
	case "up":
		v = 2
	case "degraded":
		v = 1
	}
	bc.mgr.met.SetComponentState("bus", bc.ep.Kind+":"+bc.ep.Name, v)
}

// DeviceInfo is one device observed on a CAN endpoint's bus. n2k v0.3.0's
// client tracks every claimed NAME automatically (from address-claim traffic),
// so this data comes for free once a bus client is running.
type DeviceInfo struct {
	Endpoint                 string    `json:"endpoint"`
	Address                  uint8     `json:"address"`
	Name                     uint64    `json:"name"`
	NameHex                  string    `json:"name_hex"`
	LastSeen                 time.Time `json:"last_seen"`
	IdentityNumber           uint32    `json:"identity_number"`
	ManufacturerCode         uint16    `json:"manufacturer_code"`
	Manufacturer             string    `json:"manufacturer,omitempty"`
	DeviceInstance           uint8     `json:"device_instance"`
	SystemInstance           uint8     `json:"system_instance"`
	DeviceClass              uint8     `json:"device_class"`
	DeviceClassName          string    `json:"device_class_name,omitempty"`
	DeviceFunction           uint8     `json:"device_function"`
	DeviceFunctionName       string    `json:"device_function_name,omitempty"`
	IndustryGroup            uint8     `json:"industry_group"`
	IndustryGroupName        string    `json:"industry_group_name,omitempty"`
	ArbitraryAddressCapable  bool      `json:"arbitrary_address_capable"`
	N2KVersion               uint64    `json:"n2k_version,omitempty"`
	ProductCode              uint64    `json:"product_code,omitempty"`
	Model                    string    `json:"model,omitempty"`
	SoftwareVersion          string    `json:"software_version,omitempty"`
	ModelVersion             string    `json:"model_version,omitempty"`
	Serial                   string    `json:"serial,omitempty"`
	CertificationLevel       uint64    `json:"certification_level,omitempty"`
	LoadEquivalency          uint64    `json:"load_equivalency,omitempty"`
	InstallationDescription1 string    `json:"installation_description_1,omitempty"`
	InstallationDescription2 string    `json:"installation_description_2,omitempty"`
	ManufacturerInformation  string    `json:"manufacturer_information,omitempty"`
	TransmitPGNs             []uint32  `json:"transmit_pgns,omitempty"`
	ReceivePGNs              []uint32  `json:"receive_pgns,omitempty"`
}

type SupportedPGNs struct {
	Transmit []uint32
	Receive  []uint32
}

func uint64Value(v *uint64) uint64 {
	if v == nil {
		return 0
	}
	return *v
}

func (bc *busClient) recordSupportedPGNs(name uint64, resp *pgn.ParameterGroupNumberListTransmitAndReceive) {
	seen := make(map[uint32]struct{}, len(resp.Repeating1))
	values := make([]uint32, 0, len(resp.Repeating1))
	for _, item := range resp.Repeating1 {
		if item.Pgn == nil {
			continue
		}
		if *item.Pgn > 0x3FFFF {
			continue
		}
		value := uint32(*item.Pgn)
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	bc.mu.Lock()
	lists := bc.supported[name]
	switch uint64Value(resp.FunctionCode) {
	case uint64(pgn.TransmitPGNList):
		lists.Transmit = values
	case uint64(pgn.ReceivePGNList):
		lists.Receive = values
	default:
		bc.mu.Unlock()
		return
	}
	bc.supported[name] = lists
	bc.mu.Unlock()
}

func (bc *busClient) maybeRequestPGNList(ctx context.Context, client *n2k.Client, source uint8) {
	device, ok := client.DeviceAt(source)
	if !ok {
		return
	}
	bc.mu.Lock()
	if time.Since(bc.requested[device.RawName]) < time.Minute {
		bc.mu.Unlock()
		return
	}
	bc.requested[device.RawName] = time.Now()
	bc.requestWG.Add(1)
	bc.mu.Unlock()

	go func(name uint64, target uint8) {
		defer bc.requestWG.Done()
		resp, err := n2k.Request[*pgn.ParameterGroupNumberListTransmitAndReceive](ctx, client, target)
		if err != nil {
			return
		}
		// Request returns the first matching reply. Devices commonly answer
		// PGN 126464 with both transmit and receive lists; the normal Receive
		// loop records every reply, while this call also records the first so
		// neither response ordering nor scheduler timing can hide it.
		bc.recordSupportedPGNs(name, resp)
	}(device.RawName, source)
}

// Devices returns every device currently known across all running bus
// endpoints, newest activity first. Safe to call concurrently with Acquire/
// Release; it snapshots the client set under mgr.mu, then reads each client's
// registry without holding the manager lock.
func (m *Manager) Devices() []DeviceInfo {
	m.mu.Lock()
	clients := make([]*busClient, 0, len(m.clients))
	for _, bc := range m.clients {
		clients = append(clients, bc)
	}
	m.mu.Unlock()

	var out []DeviceInfo
	for _, bc := range clients {
		bc.mu.Lock()
		client := bc.client
		ep := bc.ep
		bc.mu.Unlock()
		if client == nil {
			continue
		}
		for _, d := range client.Devices() {
			name := d.Name
			di := DeviceInfo{
				Endpoint:                ep.Kind + ":" + ep.Name,
				Address:                 d.Address,
				Name:                    d.RawName,
				NameHex:                 fmt.Sprintf("%016X", d.RawName),
				LastSeen:                d.LastSeen,
				IdentityNumber:          name.IdentityNumber,
				ManufacturerCode:        name.ManufacturerCode,
				Manufacturer:            pgn.ManufacturerCodeConst(name.ManufacturerCode).String(),
				DeviceInstance:          name.DeviceInstance,
				SystemInstance:          name.SystemInstance,
				DeviceClass:             name.DeviceClass,
				DeviceClassName:         pgn.DeviceClassConst(name.DeviceClass).String(),
				DeviceFunction:          name.DeviceFunction,
				IndustryGroup:           name.IndustryGroup,
				IndustryGroupName:       pgn.IndustryCodeConst(name.IndustryGroup).String(),
				ArbitraryAddressCapable: d.RawName&(uint64(1)<<63) != 0,
			}
			if functions := pgn.DeviceFunctionConstMap[int(name.DeviceClass)]; functions != nil {
				di.DeviceFunctionName = functions[int(name.DeviceFunction)]
			}
			if d.ProductInfo != nil {
				di.Model = strings.TrimSpace(d.ProductInfo.ModelId)
				di.Serial = strings.TrimSpace(d.ProductInfo.ModelSerialCode)
				di.N2KVersion = uint64Value(d.ProductInfo.Nmea2000Version)
				di.ProductCode = uint64Value(d.ProductInfo.ProductCode)
				di.SoftwareVersion = strings.TrimSpace(d.ProductInfo.SoftwareVersionCode)
				di.ModelVersion = strings.TrimSpace(d.ProductInfo.ModelVersion)
				di.CertificationLevel = uint64Value(d.ProductInfo.CertificationLevel)
				di.LoadEquivalency = uint64Value(d.ProductInfo.LoadEquivalency)
			}
			if d.ConfigInfo != nil {
				di.InstallationDescription1 = strings.TrimSpace(d.ConfigInfo.InstallationDescription1)
				di.InstallationDescription2 = strings.TrimSpace(d.ConfigInfo.InstallationDescription2)
				di.ManufacturerInformation = strings.TrimSpace(d.ConfigInfo.ManufacturerInformation)
			}
			bc.mu.Lock()
			lists := bc.supported[d.RawName]
			bc.mu.Unlock()
			di.TransmitPGNs = append([]uint32(nil), lists.Transmit...)
			di.ReceivePGNs = append([]uint32(nil), lists.Receive...)
			out = append(out, di)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.After(out[j].LastSeen) })
	return out
}

// Statuses returns a stable, sorted snapshot of every shared n2k client.
// It exposes the bounded runtime state added in n2k v0.3.0 and is safe to
// call concurrently with endpoint acquisition, reconnect, and release.
func (m *Manager) Statuses() []EndpointStatus {
	m.mu.Lock()
	clients := make([]*busClient, 0, len(m.clients))
	for _, bc := range m.clients {
		clients = append(clients, bc)
	}
	m.mu.Unlock()

	out := make([]EndpointStatus, 0, len(clients))
	for _, bc := range clients {
		status, _ := bc.snapshotStatus()
		out = append(out, status)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Endpoint < out[j].Endpoint })
	return out
}

func (bc *busClient) snapshotStatus() (EndpointStatus, error) {
	bc.mu.Lock()
	client := bc.client
	state := bc.state
	lastErr := bc.lastErr
	ep := bc.ep
	bc.mu.Unlock()

	out := EndpointStatus{
		Endpoint: ep.Kind + ":" + ep.Name,
		Kind:     ep.Kind,
		Name:     ep.Name,
		State:    state,
	}
	if client != nil {
		status := client.Status()
		out.Address = status.Address
		out.AddressClaimed = status.AddressClaimed
		out.Closed = status.Closed
		out.WriteQueueDepth = status.WriteQueueDepth
		out.WriteQueueCapacity = status.WriteQueueCapacity
		out.ReceiveSubscribers = status.ReceiveSubscribers

		switch {
		case status.TerminalError != nil:
			out.State = "error"
			lastErr = status.TerminalError
		case status.Closed && out.State == "up":
			out.State = "degraded"
			lastErr = n2k.ErrClientClosed
		case !status.AddressClaimed && out.State == "up":
			out.State = "degraded"
			lastErr = errors.New("n2k address claim incomplete")
		case status.WriteQueueCapacity > 0 && status.WriteQueueDepth >= status.WriteQueueCapacity && out.State == "up":
			out.State = "degraded"
			lastErr = n2k.ErrWriteQueueFull
		}
	}
	if lastErr != nil {
		out.Err = lastErr.Error()
	}
	return out, lastErr
}

type Handle struct {
	bc       *busClient
	released sync.Once
}

// Subscribe returns a channel of decoded envelopes received on the shared
// client, and an unsubscribe function that removes it. buf sets the
// channel's buffer size; a full subscriber has envelopes dropped rather than
// blocking the shared receive loop.
//
// unsub does NOT close the channel (envelopes may be in flight from the
// broadcast loop when it runs). Consumers must select on their own
// context/done signal alongside the channel — never bare-range over it.
func (h *Handle) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	return h.SubscribeNamed("", buf)
}

func (h *Handle) SubscribeNamed(label string, buf int) (<-chan *msg.Envelope, func()) {
	bc := h.bc
	bc.mu.Lock()
	id := bc.nextSub
	bc.nextSub++
	ch := make(chan *msg.Envelope, buf)
	bc.subs[id] = busSubscriber{label: label, ch: ch}
	bc.mu.Unlock()
	return ch, func() {
		bc.mu.Lock()
		delete(bc.subs, id)
		bc.mu.Unlock()
	}
}

// Write re-encodes the envelope onto the bus. Requires Raw bytes. It blocks
// until the write completes or ctx is done; cancellation abandons the wait
// but does not retract a write already handed to the client.
func (h *Handle) Write(ctx context.Context, e *msg.Envelope) error {
	if len(e.Raw) == 0 {
		return fmt.Errorf("pgn %d: envelope has no raw bytes; cannot encode to CAN", e.PGN)
	}
	h.bc.mu.Lock()
	client := h.bc.client
	h.bc.mu.Unlock()
	if client == nil {
		return fmt.Errorf("bus %s not connected", h.bc.ep.Name)
	}
	m, err := pgn.DecodeMessage(e.Info(), e.Raw)
	if err != nil {
		return fmt.Errorf("decode envelope for CAN write: pgn %d: %w: %w", e.PGN, ErrNotEncodable, err)
	}

	// client may be concurrently Closed by run()'s reconnect path after we
	// captured it above. As of n2k v0.3.0 that is safe: Client.Write registers
	// as an in-flight sender under the same lock Close takes, and Close drains
	// those senders before closing its write channel, so the old "send on
	// closed channel" panic can no longer occur (a write racing Close simply
	// returns an "n2k: client closed" error). We therefore no longer need the
	// recover-wrapped goroutine that used to guard it. Selecting on the write's
	// Done channel still lets ctx cancellation abandon the wait; a write already
	// handed to the client is not retracted.
	wr := client.Write(m)
	select {
	case <-wr.Done():
		return wr.Wait() // Done is closed, so Wait returns the result without blocking
	case <-ctx.Done():
		return ctx.Err()
	}
}

// State reports the shared client's current connection state ("up",
// "degraded", or "error") and the last error observed, if any.
func (h *Handle) State() (state string, lastErr error) {
	status, err := h.bc.snapshotStatus()
	return status.State, err
}

// Release decrements the handle's reference on the shared client. The last
// Release for an endpoint cancels the receive loop, closes the client, and
// blocks until the run goroutine has fully exited.
func (h *Handle) Release() {
	h.released.Do(func() {
		bc := h.bc
		mgr := bc.mgr
		mgr.mu.Lock()
		bc.refs-- // guarded by mgr.mu, matching Acquire
		last := bc.refs == 0
		if last {
			bc.cancel()
			delete(mgr.clients, bc.ep)
		}
		mgr.mu.Unlock()
		if last {
			// Wait outside mgr.mu so a slow client teardown doesn't block
			// Acquires of other endpoints. run never takes mgr.mu, so this
			// cannot deadlock.
			bc.wg.Wait()
		}
	})
}
