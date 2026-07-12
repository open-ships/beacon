# Beacon Phase 1 — Core Connector Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace beacon's fixed pipeline with a source/sink/connector runtime: registries of sources and sinks, per-connector CEL filtering, durable per-connector SQLite queues with replay, and a reconciling supervisor — seeded from a JSON config file (API and UI come in Phases 2–3).

**Architecture:** One `n2k.Client` per physical CAN endpoint (shared read/write via a refcounted bus manager). Each source broadcasts envelopes to subscribing connectors; each connector filters, appends to its durable queue, and delivers to its sink (push-confirmed for CAN, broadcast+client-replay for SSE/WS, live-tail for TCP). A supervisor reconciles desired state (SQLite) against running pipelines. Spec: `docs/superpowers/specs/2026-07-12-beacon-gateway-design.md`.

**Tech Stack:** Go 1.25, `github.com/open-ships/n2k` (latest main), `github.com/google/cel-go`, `modernc.org/sqlite` (CGO-free), `github.com/coder/websocket`, `github.com/spf13/cobra`, OTel + Prometheus exporter.

## Global Constraints

- Module: `github.com/open-ships/beacon`; Go `1.25.2`; `CGO_ENABLED=0` must always build.
- n2k pinned to latest `main` (commit `b067b1a` or newer) — uses `pgn.PGN`/`pgn.Message` interfaces, `pgn.DecodeMessage`, `pgn.EncodeMessage`, `n2k.Bus`, `n2k.WithBus`, `n2k.WithClaimTimeout`.
- The message envelope JSON field names are fixed API surface: `id`, `connector`, `pgn`, `source`, `dest`, `priority`, `timestamp`, `payload`, `raw`.
- CEL variables are fixed API surface: `msg.pgn`, `msg.source`, `msg.dest`, `msg.priority` (all CEL `int`), `msg.timestamp` (string, RFC3339Nano), `msg.payload` (map). Plain integer literals must work: `msg.pgn == 127250`.
- Reserved HTTP path prefixes (rejected for sink paths): `/api`, `/ui`, `/docs`, `/metrics`, `/health`.
- Sink-path validation and entity ID slugs: `^[a-z0-9][a-z0-9_-]*$` for IDs.
- Buffer limits: at least one of `max_messages`, `max_age`, `max_bytes` must be set; default when none given is `max_messages=100000`.
- Every component failure degrades state; nothing crashes the process.
- Tests: `go test ./...` green after every task; race detector (`go test -race ./...`) green at task 12.
- All new n2k clients in tests use `n2k.WithBus(fake)` + `n2k.WithClaimTimeout(50*time.Millisecond)` so address claiming doesn't stall tests.

## File Structure (Phase 1 target)

```
cmd/beacon/main.go            wiring: store, managers, supervisor, /metrics+/health, --seed
internal/model/model.go       Source/Sink/Connector/Config structs, Duration, JSON
internal/model/validate.go    structural validation (IDs, types, paths, limits, refs)
internal/msg/envelope.go      Envelope, FromPGN, Info(), PayloadMap(), SizeBytes()
internal/store/store.go       SQLite open, migrations, config CRUD, LoadConfig/ReplaceConfig
internal/queue/queue.go       Queue interface, Entry, Limits, Stats
internal/queue/sqlite.go      SQLite queue implementation (append/read/ack/prune/stats)
internal/filter/filter.go     CEL Chain: Compile, Match
internal/metrics/metrics.go   OTel instrument set (nil-safe)
internal/bus/manager.go       refcounted n2k.Client per CAN endpoint; Handle{Subscribe,Write}
internal/source/source.go     Runtime interface + hub + CAN source
internal/source/http.go       SSE dialer + WS dialer sources
internal/sink/sink.go         Runtime/Pusher/Broadcaster interfaces + CAN sink
internal/sink/dataserver.go   dynamic-route HTTP server for serve-mode sinks
internal/sink/serve.go        SSE + WS served sinks (replay + live)
internal/sink/tcp.go          TCP NDJSON live-tail sink
internal/connector/connector.go  intake + delivery + prune pipeline
internal/supervisor/supervisor.go reconciler (desired SQLite state ⇄ running components)
```

Old code (`internal/{admin,buffer,can,config,filter,n2k,sink}`, current `cmd/beacon/main.go` body, `config*.toml`, `examples/*.toml`) is deleted in Task 1. Git history preserves it.

---

### Task 1: Teardown + model package

**Files:**
- Delete: `internal/admin/`, `internal/buffer/`, `internal/can/`, `internal/config/`, `internal/filter/`, `internal/n2k/`, `internal/sink/`, `config.toml`, `config.example.toml`, `examples/` (all TOML), tracked build artifacts `beacon`, `beacon.db` if tracked
- Modify: `cmd/beacon/main.go` (stub), `go.mod` (bump n2k, drop viper), `.gitignore`
- Create: `internal/model/model.go`, `internal/model/validate.go`
- Test: `internal/model/model_test.go`

**Interfaces:**
- Produces: `model.Source{ID,Name,Type,Enabled,Interface,Port,URL,Headers}`, `model.Sink{ID,Name,Type,Enabled,Interface,Port,Path,Address}`, `model.Connector{ID,Name,SourceID,SinkID,Filters,Buffer,Enabled}`, `model.BufferLimits{MaxMessages int64, MaxAge model.Duration, MaxBytes int64}`, `model.Config{Sources,Sinks,Connectors}`, `model.Duration` (JSON `"30s"` strings), `func (c *Config) Validate() error`, `func (l BufferLimits) ApplyDefaults() BufferLimits`, type constants `model.SourceSocketCAN/SourceUSBCAN/SourceHTTPSSE/SourceHTTPWS`, `model.SinkSocketCAN/SinkUSBCAN/SinkHTTPSSE/SinkHTTPWS/SinkTCP`, `model.ReservedPathPrefixes = []string{"/api","/ui","/docs","/metrics","/health"}`

- [ ] **Step 1: Delete old code, stub main, update deps**

```bash
cd /Users/jacobthomas/code/openships/beacon
git rm -r internal/admin internal/buffer internal/can internal/config internal/filter internal/n2k internal/sink
git rm config.toml config.example.toml examples/engine-room.toml examples/high-volume.toml examples/vcan-dev.toml examples/navigation.toml examples/minimal.toml examples/source-allowlist.toml
git rm --cached beacon beacon.db 2>/dev/null || true
printf 'beacon\nbeacon.db\n*.db-wal\n*.db-shm\n' >> .gitignore
```

Replace `cmd/beacon/main.go` entirely with:

```go
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	root := &cobra.Command{
		Use:     "beacon",
		Short:   "NMEA 2000 gateway: sources, sinks, connectors",
		Version: version,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return fmt.Errorf("core runtime not wired yet (Phase 1 in progress)")
		},
	}
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
```

```bash
go get github.com/open-ships/n2k@main github.com/coder/websocket@latest
go mod tidy   # viper drops out once no code imports it
go build ./... && go test ./...
```

Expected: build OK, no tests yet besides none.

- [ ] **Step 2: Write failing model tests**

`internal/model/model_test.go`:

```go
package model

import (
	"encoding/json"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		Sources: []Source{
			{ID: "can0", Name: "Main bus", Type: SourceSocketCAN, Enabled: true, Interface: "can0"},
			{ID: "remote", Name: "Remote beacon", Type: SourceHTTPSSE, Enabled: true, URL: "http://10.0.0.2:8080/events"},
		},
		Sinks: []Sink{
			{ID: "nav-sse", Name: "Nav stream", Type: SinkHTTPSSE, Enabled: true, Path: "/nav"},
			{ID: "can2", Name: "Second bus", Type: SinkSocketCAN, Enabled: true, Interface: "can2"},
		},
		Connectors: []Connector{
			{ID: "nav", Name: "Nav to SSE", SourceID: "can0", SinkID: "nav-sse", Enabled: true,
				Filters: []string{"msg.pgn == 127250"},
				Buffer:  BufferLimits{MaxMessages: 1000}},
		},
	}
}

func TestValidateOK(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"duplicate id", func(c *Config) { c.Sources = append(c.Sources, c.Sources[0]) }},
		{"bad slug", func(c *Config) { c.Sources[0].ID = "Can 0!" }},
		{"socketcan without interface", func(c *Config) { c.Sources[0].Interface = "" }},
		{"http source without url", func(c *Config) { c.Sources[1].URL = "" }},
		{"sse sink without path", func(c *Config) { c.Sinks[0].Path = "" }},
		{"sink path not absolute", func(c *Config) { c.Sinks[0].Path = "nav" }},
		{"sink path reserved", func(c *Config) { c.Sinks[0].Path = "/api/v1/x" }},
		{"sink path duplicate", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "dup", Name: "d", Type: SinkHTTPWS, Enabled: true, Path: "/nav"})
		}},
		{"connector unknown source", func(c *Config) { c.Connectors[0].SourceID = "nope" }},
		{"connector unknown sink", func(c *Config) { c.Connectors[0].SinkID = "nope" }},
		{"unknown source type", func(c *Config) { c.Sources[0].Type = "carrier-pigeon" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestBufferDefaults(t *testing.T) {
	l := BufferLimits{}.ApplyDefaults()
	if l.MaxMessages != 100000 {
		t.Fatalf("default MaxMessages = %d, want 100000", l.MaxMessages)
	}
	// explicit limit is preserved, no default injected
	l = BufferLimits{MaxAge: Duration(time.Hour)}.ApplyDefaults()
	if l.MaxMessages != 0 || time.Duration(l.MaxAge) != time.Hour {
		t.Fatalf("explicit limits mangled: %+v", l)
	}
}

func TestDurationJSON(t *testing.T) {
	var l BufferLimits
	if err := json.Unmarshal([]byte(`{"max_age":"90s"}`), &l); err != nil {
		t.Fatal(err)
	}
	if time.Duration(l.MaxAge) != 90*time.Second {
		t.Fatalf("MaxAge = %v", l.MaxAge)
	}
	out, _ := json.Marshal(l)
	if string(out) != `{"max_age":"1m30s"}` {
		t.Fatalf("marshal = %s", out)
	}
}
```

- [ ] **Step 3: Run tests, verify failure**

Run: `go test ./internal/model/ -v`
Expected: FAIL (package does not exist / undefined types).

- [ ] **Step 4: Implement model.go**

`internal/model/model.go`:

```go
// Package model defines beacon's configuration entities: sources, sinks,
// and connectors. Validation here is structural; CEL filter compilation is
// checked by the filter package at apply time.
package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type SourceType string

const (
	SourceSocketCAN SourceType = "socketcan"
	SourceUSBCAN    SourceType = "usbcan"
	SourceHTTPSSE   SourceType = "http_sse"
	SourceHTTPWS    SourceType = "http_ws"
)

type SinkType string

const (
	SinkSocketCAN SinkType = "socketcan"
	SinkUSBCAN    SinkType = "usbcan"
	SinkHTTPSSE   SinkType = "http_sse"
	SinkHTTPWS    SinkType = "http_ws"
	SinkTCP       SinkType = "tcp"
)

// ReservedPathPrefixes cannot be used by HTTP sink paths.
var ReservedPathPrefixes = []string{"/api", "/ui", "/docs", "/metrics", "/health"}

// Duration marshals as a Go duration string ("90s", "24h").
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

type Source struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      SourceType        `json:"type"`
	Enabled   bool              `json:"enabled"`
	Interface string            `json:"interface,omitempty"` // socketcan
	Port      string            `json:"port,omitempty"`      // usbcan (serial device path)
	URL       string            `json:"url,omitempty"`       // http_sse / http_ws
	Headers   map[string]string `json:"headers,omitempty"`   // http_sse / http_ws
}

type Sink struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Type      SinkType `json:"type"`
	Enabled   bool     `json:"enabled"`
	Interface string   `json:"interface,omitempty"` // socketcan
	Port      string   `json:"port,omitempty"`      // usbcan
	Path      string   `json:"path,omitempty"`      // http_sse / http_ws (served on data server)
	Address   string   `json:"address,omitempty"`   // tcp listen address
}

type BufferLimits struct {
	MaxMessages int64    `json:"max_messages,omitempty"`
	MaxAge      Duration `json:"max_age,omitempty"`
	MaxBytes    int64    `json:"max_bytes,omitempty"`
}

// ApplyDefaults returns l with the spec default (max_messages=100000)
// applied when no limit at all is set.
func (l BufferLimits) ApplyDefaults() BufferLimits {
	if l.MaxMessages == 0 && l.MaxAge == 0 && l.MaxBytes == 0 {
		l.MaxMessages = 100000
	}
	return l
}

type Connector struct {
	ID       string       `json:"id"`
	Name     string       `json:"name"`
	SourceID string       `json:"source_id"`
	SinkID   string       `json:"sink_id"`
	Filters  []string     `json:"filters,omitempty"`
	Buffer   BufferLimits `json:"buffer"`
	Enabled  bool         `json:"enabled"`
}

type Config struct {
	Sources    []Source    `json:"sources"`
	Sinks      []Sink      `json:"sinks"`
	Connectors []Connector `json:"connectors"`
}
```

`internal/model/validate.go`:

```go
package model

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func validSlug(id string) error {
	if !slugRE.MatchString(id) {
		return fmt.Errorf("id %q: must match %s", id, slugRE)
	}
	return nil
}

func (s Source) Validate() error {
	if err := validSlug(s.ID); err != nil {
		return fmt.Errorf("source %w", err)
	}
	switch s.Type {
	case SourceSocketCAN:
		if s.Interface == "" {
			return fmt.Errorf("source %q: socketcan requires interface", s.ID)
		}
	case SourceUSBCAN:
		if s.Port == "" {
			return fmt.Errorf("source %q: usbcan requires port", s.ID)
		}
	case SourceHTTPSSE, SourceHTTPWS:
		u, err := url.Parse(s.URL)
		if err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ws" && u.Scheme != "wss") {
			return fmt.Errorf("source %q: invalid url %q", s.ID, s.URL)
		}
	default:
		return fmt.Errorf("source %q: unknown type %q", s.ID, s.Type)
	}
	return nil
}

func (s Sink) Validate() error {
	if err := validSlug(s.ID); err != nil {
		return fmt.Errorf("sink %w", err)
	}
	switch s.Type {
	case SinkSocketCAN:
		if s.Interface == "" {
			return fmt.Errorf("sink %q: socketcan requires interface", s.ID)
		}
	case SinkUSBCAN:
		if s.Port == "" {
			return fmt.Errorf("sink %q: usbcan requires port", s.ID)
		}
	case SinkHTTPSSE, SinkHTTPWS:
		if !strings.HasPrefix(s.Path, "/") {
			return fmt.Errorf("sink %q: path must start with /", s.ID)
		}
		for _, p := range ReservedPathPrefixes {
			if s.Path == p || strings.HasPrefix(s.Path, p+"/") {
				return fmt.Errorf("sink %q: path %q is reserved", s.ID, s.Path)
			}
		}
	case SinkTCP:
		if _, _, err := net.SplitHostPort(s.Address); err != nil {
			return fmt.Errorf("sink %q: invalid tcp address %q: %v", s.ID, s.Address, err)
		}
	default:
		return fmt.Errorf("sink %q: unknown type %q", s.ID, s.Type)
	}
	return nil
}

func (c Connector) Validate() error {
	if err := validSlug(c.ID); err != nil {
		return fmt.Errorf("connector %w", err)
	}
	if c.SourceID == "" || c.SinkID == "" {
		return fmt.Errorf("connector %q: source_id and sink_id are required", c.ID)
	}
	return nil
}

// Validate checks structural rules across the whole config: per-entity
// rules, ID uniqueness, reference integrity, and sink path collisions.
func (c *Config) Validate() error {
	ids := map[string]bool{}
	srcIDs := map[string]bool{}
	for _, s := range c.Sources {
		if err := s.Validate(); err != nil {
			return err
		}
		if ids["src:"+s.ID] {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		ids["src:"+s.ID] = true
		srcIDs[s.ID] = true
	}
	sinkIDs := map[string]bool{}
	paths := map[string]string{}
	for _, s := range c.Sinks {
		if err := s.Validate(); err != nil {
			return err
		}
		if ids["snk:"+s.ID] {
			return fmt.Errorf("duplicate sink id %q", s.ID)
		}
		ids["snk:"+s.ID] = true
		sinkIDs[s.ID] = true
		if s.Path != "" {
			if other, dup := paths[s.Path]; dup {
				return fmt.Errorf("sinks %q and %q share path %q", other, s.ID, s.Path)
			}
			paths[s.Path] = s.ID
		}
	}
	for _, cn := range c.Connectors {
		if err := cn.Validate(); err != nil {
			return err
		}
		if ids["con:"+cn.ID] {
			return fmt.Errorf("duplicate connector id %q", cn.ID)
		}
		ids["con:"+cn.ID] = true
		if !srcIDs[cn.SourceID] {
			return fmt.Errorf("connector %q: unknown source %q", cn.ID, cn.SourceID)
		}
		if !sinkIDs[cn.SinkID] {
			return fmt.Errorf("connector %q: unknown sink %q", cn.ID, cn.SinkID)
		}
	}
	return nil
}
```

- [ ] **Step 5: Run tests, verify pass**

Run: `go test ./internal/model/ -v`
Expected: PASS (all cases).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: teardown old pipeline, add model package for sources/sinks/connectors"
```

---

### Task 2: Envelope package

**Files:**
- Create: `internal/msg/envelope.go`
- Test: `internal/msg/envelope_test.go`

**Interfaces:**
- Consumes: `github.com/open-ships/n2k/pgn` — `pgn.Message`, `pgn.PGN`, `pgn.UnknownPGN`, `pgn.MessageInfo{Timestamp, Priority *uint8, PGN, SourceId, TargetId *uint8}`, `pgn.EncodeMessage(msg) ([]byte, error)`, `pgn.DecodeMessage(info, payload) (pgn.PGN, error)`
- Produces: `msg.Envelope{Seq int64, ConnectorID string, PGN uint32, Source uint8, Dest uint8, Priority uint8, Timestamp time.Time, Payload json.RawMessage, Raw []byte}` with JSON tags `id,connector,pgn,source,dest,priority,timestamp,payload,raw`; `msg.FromPGN(m pgn.Message) (*Envelope, error)`; `(*Envelope).Info() pgn.MessageInfo`; `(*Envelope).PayloadMap() map[string]any` (cached, `nil` payload → empty map); `(*Envelope).SizeBytes() int`

- [ ] **Step 1: Write failing tests**

`internal/msg/envelope_test.go`:

```go
package msg

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/open-ships/n2k/pgn"
)

func heading(v uint64) *pgn.VesselHeading {
	h := v
	return &pgn.VesselHeading{
		Info: pgn.MessageInfo{
			Timestamp: time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC),
			Priority:  pgn.Priority(2),
			PGN:       127250,
			SourceId:  12,
		},
		Heading: &h,
	}
}

func TestFromPGNKnown(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	if e.PGN != 127250 || e.Source != 12 || e.Priority != 2 || e.Dest != 255 {
		t.Fatalf("header mismatch: %+v", e)
	}
	if len(e.Raw) == 0 {
		t.Fatal("Raw must be populated for known PGNs (canonical re-encode)")
	}
	if e.PayloadMap()["heading"] == nil {
		t.Fatalf("payload heading missing: %s", e.Payload)
	}
	// Raw must round-trip through the codec back to the same PGN
	back, err := pgn.DecodeMessage(e.Info(), e.Raw)
	if err != nil {
		t.Fatal(err)
	}
	if back.PGNNumber() != 127250 {
		t.Fatalf("round-trip PGN = %d", back.PGNNumber())
	}
}

func TestFromPGNUnknown(t *testing.T) {
	u := &pgn.UnknownPGN{
		Info: pgn.MessageInfo{PGN: 130999, SourceId: 9, Timestamp: time.Now()},
		Data: []byte{1, 2, 3, 4},
	}
	e, err := FromPGN(u)
	if err != nil {
		t.Fatal(err)
	}
	if string(e.Payload) != "null" {
		t.Fatalf("unknown payload = %s, want null", e.Payload)
	}
	if len(e.Raw) != 4 {
		t.Fatalf("raw = %v", e.Raw)
	}
	if e.Dest != 255 || e.Priority != 6 {
		t.Fatalf("defaults not applied: %+v", e)
	}
}

func TestEnvelopeJSONShape(t *testing.T) {
	e, _ := FromPGN(heading(15708))
	e.Seq = 42
	e.ConnectorID = "nav"
	b, _ := json.Marshal(e)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"id", "connector", "pgn", "source", "dest", "priority", "timestamp", "payload", "raw"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("marshaled envelope missing %q: %s", k, b)
		}
	}
}

func TestSizeBytes(t *testing.T) {
	e, _ := FromPGN(heading(15708))
	if e.SizeBytes() < len(e.Payload)+len(e.Raw) {
		t.Fatalf("SizeBytes too small: %d", e.SizeBytes())
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/msg/ -v`
Expected: FAIL (undefined: FromPGN, Envelope).

- [ ] **Step 3: Implement envelope.go**

```go
// Package msg defines the canonical message envelope that flows through
// queues, HTTP wire formats, and CEL filters.
package msg

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/open-ships/n2k/pgn"
)

type Envelope struct {
	Seq         int64           `json:"id,omitempty"`
	ConnectorID string          `json:"connector,omitempty"`
	PGN         uint32          `json:"pgn"`
	Source      uint8           `json:"source"`
	Dest        uint8           `json:"dest"`
	Priority    uint8           `json:"priority"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
	Raw         []byte          `json:"raw,omitempty"`

	payloadMap map[string]any // lazy cache for CEL
}

const (
	defaultPriority = 6
	broadcastDest   = 255
)

// FromPGN converts a decoded n2k message into an Envelope. Known PGNs get
// their payload marshaled to JSON and Raw set to the canonical re-encoding;
// UnknownPGN keeps its original bytes and a null payload.
func FromPGN(m pgn.Message) (*Envelope, error) {
	p, ok := m.(pgn.PGN)
	if !ok {
		return nil, fmt.Errorf("%T does not implement pgn.PGN", m)
	}
	info := p.MessageInfo()
	e := &Envelope{
		PGN:       info.PGN,
		Source:    info.SourceId,
		Dest:      broadcastDest,
		Priority:  defaultPriority,
		Timestamp: info.Timestamp,
	}
	if e.PGN == 0 {
		e.PGN = m.PGNNumber()
	}
	if info.TargetId != nil {
		e.Dest = *info.TargetId
	}
	if info.Priority != nil {
		e.Priority = *info.Priority
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}

	if u, isUnknown := m.(*pgn.UnknownPGN); isUnknown {
		e.Payload = json.RawMessage("null")
		e.Raw = append([]byte(nil), u.Data...)
		return e, nil
	}

	payload, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal PGN %d payload: %w", e.PGN, err)
	}
	e.Payload = payload
	raw, err := pgn.EncodeMessage(m)
	if err != nil {
		// Still deliverable to HTTP sinks; CAN sinks will skip it.
		e.Raw = nil
	} else {
		e.Raw = raw
	}
	return e, nil
}

// Info rebuilds the MessageInfo for encoding this envelope back onto a CAN bus.
func (e *Envelope) Info() pgn.MessageInfo {
	return pgn.MessageInfo{
		Timestamp: e.Timestamp,
		Priority:  pgn.Priority(e.Priority),
		PGN:       e.PGN,
		SourceId:  e.Source,
		TargetId:  pgn.Target(e.Dest),
	}
}

// PayloadMap returns the payload as a map for CEL evaluation, cached after
// the first call. A null/empty payload yields an empty map.
func (e *Envelope) PayloadMap() map[string]any {
	if e.payloadMap != nil {
		return e.payloadMap
	}
	m := map[string]any{}
	if len(e.Payload) > 0 && string(e.Payload) != "null" {
		_ = json.Unmarshal(e.Payload, &m)
	}
	e.payloadMap = m
	return m
}

// SizeBytes approximates the stored size for buffer byte-limit accounting.
func (e *Envelope) SizeBytes() int {
	return len(e.Payload) + len(e.Raw) + 64
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/msg/ -v`
Expected: PASS. Note: if `TestFromPGNKnown` fails because `VesselHeading` marshals `Info` into the payload JSON, that is acceptable Phase 1 behavior (payload contains an `info` key alongside the fields) — do NOT strip it with custom marshaling; just assert on `heading` presence as written.

- [ ] **Step 5: Commit**

```bash
git add internal/msg
git commit -m "feat: canonical message envelope with n2k round-trip support"
```

---

### Task 3: Store (SQLite, migrations, config CRUD)

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `model.Config`, `model.Source`, `model.Sink`, `model.Connector`
- Produces: `store.Open(path string) (*store.Store, error)` (applies migrations, WAL); `(*Store).DB() *sql.DB`; `(*Store).Close() error`; `(*Store).LoadConfig(ctx) (model.Config, error)`; `(*Store).ReplaceConfig(ctx, cfg model.Config) error` (transactional full replace); `(*Store).PutSource/PutSink/PutConnector(ctx, v) error` (upsert); `(*Store).DeleteSource/DeleteSink/DeleteConnector(ctx, id string) error`; `(*Store).IsEmpty(ctx) (bool, error)` (no config rows at all)

- [ ] **Step 1: Write failing tests**

`internal/store/store_test.go`:

```go
package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/model"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestReplaceAndLoad(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	empty, err := s.IsEmpty(ctx)
	if err != nil || !empty {
		t.Fatalf("fresh store IsEmpty = %v, %v", empty, err)
	}

	cfg := model.Config{
		Sources:    []model.Source{{ID: "can0", Name: "Bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}},
		Sinks:      []model.Sink{{ID: "sse", Name: "SSE", Type: model.SinkHTTPSSE, Enabled: true, Path: "/events"}},
		Connectors: []model.Connector{{ID: "all", Name: "All", SourceID: "can0", SinkID: "sse", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}}},
	}
	if err := s.ReplaceConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 || got.Sources[0].Interface != "can0" ||
		len(got.Sinks) != 1 || got.Sinks[0].Path != "/events" ||
		len(got.Connectors) != 1 || got.Connectors[0].Buffer.MaxMessages != 10 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestUpsertAndDelete(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	src := model.Source{ID: "u1", Name: "USB", Type: model.SourceUSBCAN, Enabled: true, Port: "/dev/ttyUSB0"}
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	src.Name = "USB adapter"
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.LoadConfig(ctx)
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "USB adapter" {
		t.Fatalf("upsert failed: %+v", cfg.Sources)
	}
	if err := s.DeleteSource(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	cfg, _ = s.LoadConfig(ctx)
	if len(cfg.Sources) != 0 {
		t.Fatal("delete failed")
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = s.PutSink(ctx, model.Sink{ID: "t", Name: "T", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:9090"})
	_ = s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cfg, _ := s2.LoadConfig(ctx)
	if len(cfg.Sinks) != 1 {
		t.Fatal("data lost across reopen")
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/store/ -v`
Expected: FAIL (undefined: Open).

- [ ] **Step 3: Implement store.go**

Entities are stored as JSON blobs keyed by id — the schema stays stable as model fields evolve. The queue tables are created here too (used by Task 4).

```go
// Package store owns the SQLite database: schema migrations, config
// entity CRUD, and the connection shared with the queue implementation.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/open-ships/beacon/internal/model"
)

type Store struct{ db *sql.DB }

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS sinks (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS connectors (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS queue (
	   id INTEGER PRIMARY KEY AUTOINCREMENT,
	   connector_id TEXT NOT NULL,
	   ts INTEGER NOT NULL,
	   envelope TEXT NOT NULL,
	   bytes INTEGER NOT NULL
	 );
	 CREATE INDEX IF NOT EXISTS queue_connector ON queue(connector_id, id);
	 CREATE INDEX IF NOT EXISTS queue_connector_ts ON queue(connector_id, ts);
	 CREATE TABLE IF NOT EXISTS checkpoints (
	   connector_id TEXT PRIMARY KEY,
	   last_seq INTEGER NOT NULL DEFAULT 0
	 );`,
}

func Open(path string) (*Store, error) {
	// _pragma handling is modernc.org/sqlite-specific.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// modernc.org/sqlite serializes writes; a single conn avoids SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return err
	}
	var current int
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current)
	for i := current; i < len(migrations); i++ {
		if _, err := s.db.Exec(migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := s.db.Exec(`INSERT INTO schema_migrations(version) VALUES (?)`, i+1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DB() *sql.DB  { return s.db }
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM sources) + (SELECT COUNT(*) FROM sinks) + (SELECT COUNT(*) FROM connectors)`).Scan(&n)
	return n == 0, err
}

func put[T any](ctx context.Context, db *sql.DB, table, id string, v T) error {
	doc, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+table+` (id, doc) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET doc = excluded.doc`, id, string(doc))
	return err
}

func list[T any](ctx context.Context, db *sql.DB, table string) ([]T, error) {
	rows, err := db.QueryContext(ctx, `SELECT doc FROM `+table+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		var v T
		if err := json.Unmarshal([]byte(doc), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) PutSource(ctx context.Context, v model.Source) error {
	return put(ctx, s.db, "sources", v.ID, v)
}
func (s *Store) PutSink(ctx context.Context, v model.Sink) error {
	return put(ctx, s.db, "sinks", v.ID, v)
}
func (s *Store) PutConnector(ctx context.Context, v model.Connector) error {
	return put(ctx, s.db, "connectors", v.ID, v)
}

func (s *Store) DeleteSource(ctx context.Context, id string) error    { return s.del(ctx, "sources", id) }
func (s *Store) DeleteSink(ctx context.Context, id string) error      { return s.del(ctx, "sinks", id) }
func (s *Store) DeleteConnector(ctx context.Context, id string) error { return s.del(ctx, "connectors", id) }

func (s *Store) del(ctx context.Context, table, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE id = ?`, id)
	return err
}

func (s *Store) LoadConfig(ctx context.Context) (model.Config, error) {
	var cfg model.Config
	var err error
	if cfg.Sources, err = list[model.Source](ctx, s.db, "sources"); err != nil {
		return cfg, err
	}
	if cfg.Sinks, err = list[model.Sink](ctx, s.db, "sinks"); err != nil {
		return cfg, err
	}
	if cfg.Connectors, err = list[model.Connector](ctx, s.db, "connectors"); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// ReplaceConfig transactionally replaces the whole configuration.
func (s *Store) ReplaceConfig(ctx context.Context, cfg model.Config) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"sources", "sinks", "connectors"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return err
		}
	}
	ins := func(table, id string, v any) error {
		doc, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (id, doc) VALUES (?, ?)`, id, string(doc))
		return err
	}
	for _, v := range cfg.Sources {
		if err := ins("sources", v.ID, v); err != nil {
			return err
		}
	}
	for _, v := range cfg.Sinks {
		if err := ins("sinks", v.ID, v); err != nil {
			return err
		}
	}
	for _, v := range cfg.Connectors {
		if err := ins("connectors", v.ID, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/store/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat: SQLite store with migrations and config CRUD"
```

---

### Task 4: Queue (durable per-connector buffer)

**Files:**
- Create: `internal/queue/queue.go`, `internal/queue/sqlite.go`
- Test: `internal/queue/sqlite_test.go`

**Interfaces:**
- Consumes: `store.Store` (shared `*sql.DB`), `msg.Envelope`, `model.BufferLimits`
- Produces:

```go
type Entry struct { Seq int64; Env *msg.Envelope }
type Stats struct { Depth int64; Bytes int64; Oldest time.Time }
type Queue interface {
    Append(ctx context.Context, envs []*msg.Envelope) error
    Read(ctx context.Context, after int64, limit int) ([]Entry, error)
    Cursor(ctx context.Context) (int64, error)
    Ack(ctx context.Context, upTo int64) error
    Prune(ctx context.Context) (int64, error)
    Stats(ctx context.Context) (Stats, error)
}
func NewSQLite(st *store.Store, connectorID string, limits model.BufferLimits) Queue
```

Read returns entries with `Env.Seq` and `Env.ConnectorID` populated. `NewSQLite` applies `limits.ApplyDefaults()`.

- [ ] **Step 1: Write failing tests**

`internal/queue/sqlite_test.go`:

```go
package queue

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

func testQueue(t *testing.T, limits model.BufferLimits) Queue {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return NewSQLite(st, "test-conn", limits)
}

func env(pgn uint32, ts time.Time) *msg.Envelope {
	return &msg.Envelope{PGN: pgn, Source: 1, Dest: 255, Priority: 3,
		Timestamp: ts, Payload: json.RawMessage(`{"x":1}`)}
}

func appendN(t *testing.T, q Queue, n int, start time.Time) {
	t.Helper()
	var batch []*msg.Envelope
	for i := 0; i < n; i++ {
		batch = append(batch, env(uint32(127000+i), start.Add(time.Duration(i)*time.Second)))
	}
	if err := q.Append(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
}

func TestAppendReadAck(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())

	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("read %d entries, want 5", len(entries))
	}
	if entries[0].Env.Seq != entries[0].Seq || entries[0].Env.ConnectorID != "test-conn" {
		t.Fatalf("entry env not annotated: %+v", entries[0].Env)
	}
	if entries[0].Seq >= entries[4].Seq {
		t.Fatal("entries not in ascending seq order")
	}

	// resume after partial read
	mid := entries[2].Seq
	rest, _ := q.Read(ctx, mid, 10)
	if len(rest) != 2 {
		t.Fatalf("read after mid = %d entries, want 2", len(rest))
	}

	if err := q.Ack(ctx, entries[4].Seq); err != nil {
		t.Fatal(err)
	}
	cur, err := q.Cursor(ctx)
	if err != nil || cur != entries[4].Seq {
		t.Fatalf("cursor = %d, %v; want %d", cur, err, entries[4].Seq)
	}
}

func TestPruneByCount(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 3})
	ctx := context.Background()
	appendN(t, q, 10, time.Now())
	pruned, err := q.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 7 {
		t.Fatalf("pruned %d, want 7", pruned)
	}
	st, _ := q.Stats(ctx)
	if st.Depth != 3 {
		t.Fatalf("depth = %d, want 3", st.Depth)
	}
}

func TestPruneByAge(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxAge: model.Duration(time.Hour)})
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour)
	appendN(t, q, 4, old)               // all stale
	appendN(t, q, 3, time.Now())        // fresh
	pruned, err := q.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 4 {
		t.Fatalf("pruned %d, want 4", pruned)
	}
}

func TestPruneByBytes(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxBytes: 1}) // absurdly small: keep nothing but newest
	ctx := context.Background()
	appendN(t, q, 5, time.Now())
	if _, err := q.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(ctx)
	if st.Depth > 1 {
		t.Fatalf("depth = %d after byte prune, want <= 1", st.Depth)
	}
}

func TestQueuesAreIsolated(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	qa := NewSQLite(st, "a", model.BufferLimits{MaxMessages: 100})
	qb := NewSQLite(st, "b", model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	_ = qa.Append(ctx, []*msg.Envelope{env(1, time.Now())})
	entries, _ := qb.Read(ctx, 0, 10)
	if len(entries) != 0 {
		t.Fatal("queue b sees queue a's entries")
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/queue/ -v`
Expected: FAIL (undefined: NewSQLite).

- [ ] **Step 3: Implement queue.go + sqlite.go**

`internal/queue/queue.go`:

```go
// Package queue provides the durable per-connector buffer. The interface is
// deliberately broker-shaped so an embedded broker (e.g. NATS JetStream)
// can replace the SQLite implementation without touching connector logic.
package queue

import (
	"context"
	"time"

	"github.com/open-ships/beacon/internal/msg"
)

type Entry struct {
	Seq int64
	Env *msg.Envelope
}

type Stats struct {
	Depth  int64
	Bytes  int64
	Oldest time.Time
}

type Queue interface {
	// Append persists envelopes in order. Seq is assigned by the queue.
	Append(ctx context.Context, envs []*msg.Envelope) error
	// Read returns up to limit entries with Seq > after, ascending.
	Read(ctx context.Context, after int64, limit int) ([]Entry, error)
	// Cursor returns the delivery checkpoint (0 if none).
	Cursor(ctx context.Context) (int64, error)
	// Ack advances the delivery checkpoint.
	Ack(ctx context.Context, upTo int64) error
	// Prune enforces the configured limits; returns rows removed.
	Prune(ctx context.Context) (int64, error)
	Stats(ctx context.Context) (Stats, error)
}
```

`internal/queue/sqlite.go`:

```go
package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

type sqliteQueue struct {
	db          *sql.DB
	connectorID string
	limits      model.BufferLimits
}

func NewSQLite(st *store.Store, connectorID string, limits model.BufferLimits) Queue {
	return &sqliteQueue{db: st.DB(), connectorID: connectorID, limits: limits.ApplyDefaults()}
}

func (q *sqliteQueue) Append(ctx context.Context, envs []*msg.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO queue (connector_id, ts, envelope, bytes) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range envs {
		doc, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, q.connectorID, e.Timestamp.UnixNano(), string(doc), e.SizeBytes()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (q *sqliteQueue) Read(ctx context.Context, after int64, limit int) ([]Entry, error) {
	rows, err := q.db.QueryContext(ctx,
		`SELECT id, envelope FROM queue WHERE connector_id = ? AND id > ? ORDER BY id LIMIT ?`,
		q.connectorID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var seq int64
		var doc string
		if err := rows.Scan(&seq, &doc); err != nil {
			return nil, err
		}
		var e msg.Envelope
		if err := json.Unmarshal([]byte(doc), &e); err != nil {
			return nil, err
		}
		e.Seq = seq
		e.ConnectorID = q.connectorID
		out = append(out, Entry{Seq: seq, Env: &e})
	}
	return out, rows.Err()
}

func (q *sqliteQueue) Cursor(ctx context.Context) (int64, error) {
	var cur int64
	err := q.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?), 0)`,
		q.connectorID).Scan(&cur)
	return cur, err
}

func (q *sqliteQueue) Ack(ctx context.Context, upTo int64) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO checkpoints (connector_id, last_seq) VALUES (?, ?)
		 ON CONFLICT(connector_id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)`,
		q.connectorID, upTo)
	return err
}

func (q *sqliteQueue) Prune(ctx context.Context) (int64, error) {
	var total int64
	if n := q.limits.MaxMessages; n > 0 {
		res, err := q.db.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ?1 AND id <= COALESCE(
			   (SELECT id FROM queue WHERE connector_id = ?1 ORDER BY id DESC LIMIT 1 OFFSET ?2), 0)`,
			q.connectorID, n)
		if err != nil {
			return total, err
		}
		c, _ := res.RowsAffected()
		total += c
	}
	if d := time.Duration(q.limits.MaxAge); d > 0 {
		res, err := q.db.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ? AND ts < ?`,
			q.connectorID, time.Now().Add(-d).UnixNano())
		if err != nil {
			return total, err
		}
		c, _ := res.RowsAffected()
		total += c
	}
	if b := q.limits.MaxBytes; b > 0 {
		res, err := q.db.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ?1 AND id <= COALESCE(
			   (SELECT id FROM (
			      SELECT id, SUM(bytes) OVER (ORDER BY id DESC) AS running
			      FROM queue WHERE connector_id = ?1
			    ) WHERE running > ?2 ORDER BY id DESC LIMIT 1), 0)`,
			q.connectorID, b)
		if err != nil {
			return total, err
		}
		c, _ := res.RowsAffected()
		total += c
	}
	return total, nil
}

func (q *sqliteQueue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var oldest sql.NullInt64
	err := q.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(bytes), 0), MIN(ts) FROM queue WHERE connector_id = ?`,
		q.connectorID).Scan(&s.Depth, &s.Bytes, &oldest)
	if oldest.Valid {
		s.Oldest = time.Unix(0, oldest.Int64)
	}
	return s, err
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/queue/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/queue
git commit -m "feat: durable per-connector SQLite queue with count/age/byte pruning"
```

---

### Task 5: CEL filter chain

**Files:**
- Create: `internal/filter/filter.go`
- Test: `internal/filter/filter_test.go`

**Interfaces:**
- Consumes: `msg.Envelope` (fields + `PayloadMap()`)
- Produces: `filter.Compile(exprs []string) (*filter.Chain, error)` (nil-safe: empty exprs → chain that always matches); `(*Chain).Match(e *msg.Envelope) (bool, error)` — AND semantics, eval error returns `(false, err)`

- [ ] **Step 1: Write failing tests**

`internal/filter/filter_test.go`:

```go
package filter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
)

func env(pgnNum uint32, source uint8, payload string) *msg.Envelope {
	return &msg.Envelope{PGN: pgnNum, Source: source, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(payload)}
}

func TestPlainIntLiterals(t *testing.T) {
	c, err := Compile([]string{"msg.pgn == 127250"})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := c.Match(env(127250, 1, `{}`))
	if err != nil || !ok {
		t.Fatalf("match = %v, %v; want true", ok, err)
	}
	ok, _ = c.Match(env(127251, 1, `{}`))
	if ok {
		t.Fatal("wrong PGN matched")
	}
}

func TestAndSemanticsAcrossExprs(t *testing.T) {
	c, err := Compile([]string{"msg.pgn in [127250, 128259]", "msg.source != 42"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(127250, 42, `{}`)); ok {
		t.Fatal("second expr should have rejected")
	}
	if ok, _ := c.Match(env(128259, 7, `{}`)); !ok {
		t.Fatal("both exprs should pass")
	}
}

func TestPayloadField(t *testing.T) {
	c, err := Compile([]string{"double(msg.payload.speed) > 2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(128259, 1, `{"speed": 3.5}`)); !ok {
		t.Fatal("payload threshold should pass")
	}
	if ok, _ := c.Match(env(128259, 1, `{"speed": 1.0}`)); ok {
		t.Fatal("payload threshold should reject")
	}
}

func TestEvalErrorReturnsError(t *testing.T) {
	c, err := Compile([]string{"double(msg.payload.missing) > 1.0"})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := c.Match(env(128259, 1, `{}`))
	if ok || err == nil {
		t.Fatalf("missing field: match=%v err=%v; want false + error", ok, err)
	}
}

func TestCompileErrorRejected(t *testing.T) {
	if _, err := Compile([]string{"msg.pgn =="}); err == nil {
		t.Fatal("invalid CEL accepted")
	}
}

func TestEmptyChainMatchesAll(t *testing.T) {
	c, err := Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(1, 1, `{}`)); !ok {
		t.Fatal("empty chain must pass everything")
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/filter/ -v`
Expected: FAIL (undefined: Compile).

- [ ] **Step 3: Implement filter.go**

```go
// Package filter compiles and evaluates CEL expressions against message
// envelopes. Numeric header fields are exposed as CEL ints so plain
// integer literals work (msg.pgn == 127250).
package filter

import (
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"

	"github.com/open-ships/beacon/internal/msg"
)

type Chain struct {
	exprs []string
	progs []cel.Program
}

func Compile(exprs []string) (*Chain, error) {
	env, err := cel.NewEnv(cel.Variable("msg", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		return nil, err
	}
	c := &Chain{exprs: exprs}
	for _, expr := range exprs {
		ast, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("filter %q: %w", expr, issues.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("filter %q: %w", expr, err)
		}
		c.progs = append(c.progs, prg)
	}
	return c, nil
}

// Match evaluates all expressions (AND). Returns an error if any
// expression errors at eval time; callers drop the message and count it.
func (c *Chain) Match(e *msg.Envelope) (bool, error) {
	if len(c.progs) == 0 {
		return true, nil
	}
	in := map[string]any{"msg": map[string]any{
		"pgn":       int64(e.PGN),
		"source":    int64(e.Source),
		"dest":      int64(e.Dest),
		"priority":  int64(e.Priority),
		"timestamp": e.Timestamp.Format(time.RFC3339Nano),
		"payload":   e.PayloadMap(),
	}}
	for i, prg := range c.progs {
		out, _, err := prg.Eval(in)
		if err != nil {
			return false, fmt.Errorf("filter %q: %w", c.exprs[i], err)
		}
		if out != types.True {
			return false, nil
		}
	}
	return true, nil
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/filter/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/filter
git commit -m "feat: CEL filter chain over message envelopes"
```

---

### Task 6: Metrics set

**Files:**
- Create: `internal/metrics/metrics.go`
- Test: `internal/metrics/metrics_test.go`

**Interfaces:**
- Produces: `metrics.Set` with nil-safe methods (a nil `*Set` no-ops so tests can pass nil): `New() (*Set, http.Handler, error)` (OTel meter + Prometheus exporter, handler serves `/metrics`); methods `ConnectorMessages(ctx, connector, stage string, n int64)` (stage: `received|matched|delivered|filter_error|pruned`), `ConnectorBytes(ctx, connector string, n int64)`, `SetQueueDepth(connector string, depth, bytes int64)`, `SetComponentState(kind, id string, state int64)` (0=error,1=degraded,2=up), `SourceMessages(ctx, source string, n int64)`, `SinkClients(sink string, delta int64)`
- Metric names: `beacon_connector_messages_total`, `beacon_connector_bytes_total`, `beacon_connector_queue_depth`, `beacon_connector_queue_bytes`, `beacon_component_state`, `beacon_source_messages_total`, `beacon_sink_clients`

- [ ] **Step 1: Write failing test**

`internal/metrics/metrics_test.go`:

```go
package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNilSetIsSafe(t *testing.T) {
	var s *Set
	s.ConnectorMessages(context.Background(), "c", "received", 1)
	s.ConnectorBytes(context.Background(), "c", 10)
	s.SetQueueDepth("c", 5, 100)
	s.SetComponentState("source", "can0", 2)
	s.SourceMessages(context.Background(), "can0", 1)
	s.SinkClients("sse", 1)
}

func TestPrometheusExposition(t *testing.T) {
	s, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s.ConnectorMessages(ctx, "nav", "delivered", 3)
	s.SourceMessages(ctx, "can0", 7)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{"beacon_connector_messages_total", "beacon_source_messages_total"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("exposition missing %s:\n%s", want, body)
		}
	}
}
```

- [ ] **Step 2: Run test, verify failure**

Run: `go test ./internal/metrics/ -v`
Expected: FAIL (undefined: Set, New).

- [ ] **Step 3: Implement metrics.go**

```go
// Package metrics owns the OTel instrument set. A nil *Set no-ops every
// method so components never need nil checks around instrumentation.
package metrics

import (
	"context"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type gaugeKey struct{ kind, id string }

type Set struct {
	connectorMessages api.Int64Counter
	connectorBytes    api.Int64Counter
	sourceMessages    api.Int64Counter
	queueDepth        api.Int64ObservableGauge
	queueBytes        api.Int64ObservableGauge
	componentState    api.Int64ObservableGauge
	sinkClients       api.Int64UpDownCounter

	mu     sync.Mutex
	depths map[string][2]int64 // connector -> {depth, bytes}
	states map[gaugeKey]int64
}

func New() (*Set, http.Handler, error) {
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("beacon")

	s := &Set{depths: map[string][2]int64{}, states: map[gaugeKey]int64{}}
	s.connectorMessages, _ = meter.Int64Counter("beacon.connector.messages")
	s.connectorBytes, _ = meter.Int64Counter("beacon.connector.bytes")
	s.sourceMessages, _ = meter.Int64Counter("beacon.source.messages")
	s.sinkClients, _ = meter.Int64UpDownCounter("beacon.sink.clients")
	s.queueDepth, _ = meter.Int64ObservableGauge("beacon.connector.queue.depth")
	s.queueBytes, _ = meter.Int64ObservableGauge("beacon.connector.queue.bytes")
	s.componentState, _ = meter.Int64ObservableGauge("beacon.component.state")
	_, err = meter.RegisterCallback(func(_ context.Context, o api.Observer) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for c, v := range s.depths {
			o.ObserveInt64(s.queueDepth, v[0], api.WithAttributes(attribute.String("connector", c)))
			o.ObserveInt64(s.queueBytes, v[1], api.WithAttributes(attribute.String("connector", c)))
		}
		for k, v := range s.states {
			o.ObserveInt64(s.componentState, v,
				api.WithAttributes(attribute.String("kind", k.kind), attribute.String("id", k.id)))
		}
		return nil
	}, s.queueDepth, s.queueBytes, s.componentState)
	if err != nil {
		return nil, nil, err
	}
	return s, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

func (s *Set) ConnectorMessages(ctx context.Context, connector, stage string, n int64) {
	if s == nil {
		return
	}
	s.connectorMessages.Add(ctx, n, api.WithAttributes(
		attribute.String("connector", connector), attribute.String("stage", stage)))
}

func (s *Set) ConnectorBytes(ctx context.Context, connector string, n int64) {
	if s == nil {
		return
	}
	s.connectorBytes.Add(ctx, n, api.WithAttributes(attribute.String("connector", connector)))
}

func (s *Set) SourceMessages(ctx context.Context, source string, n int64) {
	if s == nil {
		return
	}
	s.sourceMessages.Add(ctx, n, api.WithAttributes(attribute.String("source", source)))
}

func (s *Set) SinkClients(sink string, delta int64) {
	if s == nil {
		return
	}
	s.sinkClients.Add(context.Background(), delta, api.WithAttributes(attribute.String("sink", sink)))
}

func (s *Set) SetQueueDepth(connector string, depth, bytes int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.depths[connector] = [2]int64{depth, bytes}
	s.mu.Unlock()
}

func (s *Set) SetComponentState(kind, id string, state int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.states[gaugeKey{kind, id}] = state
	s.mu.Unlock()
}
```

- [ ] **Step 4: Run test, verify pass**

Run: `go test ./internal/metrics/ -v`
Expected: PASS. If the OTel API differs on observable-gauge registration in the pinned version, adapt the callback registration only — keep the public `Set` methods exactly as specified.

- [ ] **Step 5: Commit**

```bash
git add internal/metrics
git commit -m "feat: nil-safe OTel metric set with Prometheus exposition"
```

---

### Task 7: Bus manager

**Files:**
- Create: `internal/bus/manager.go`
- Test: `internal/bus/manager_test.go`

**Interfaces:**
- Consumes: `n2k.NewClient`, `n2k.CAN(iface)`, `n2k.USB(port)`, `n2k.IncludeUnknown()`, `n2k.WithLogger`, `n2k.WithBus`, `n2k.WithClaimTimeout`, `client.Receive() iter.Seq2[pgn.Message, error]`, `client.Write(pgn.Message) *WriteResult` (+ `.Wait() error`), `client.Close()`, `msg.FromPGN`, `pgn.DecodeMessage`
- Produces:

```go
type Endpoint struct { Kind string; Name string } // Kind: "socketcan"|"usbcan"; Name: iface or port
type Manager struct{ ... }
func NewManager(log *slog.Logger, met *metrics.Set, extraOpts ...n2k.Option) *Manager
func (m *Manager) Acquire(ctx context.Context, ep Endpoint) (*Handle, error)
type Handle struct{ ... }
func (h *Handle) Subscribe(buf int) (<-chan *msg.Envelope, func())  // func() unsubscribes
func (h *Handle) Write(ctx context.Context, e *msg.Envelope) error  // decode raw → client.Write → Wait
func (h *Handle) Release()                                          // refcount--; last release closes client
func (h *Handle) State() (state string, lastErr error)              // "up" | "degraded" | "error"
```

`extraOpts` are appended to every `NewClient` call — tests inject `n2k.WithBus(fake)` + `n2k.WithClaimTimeout(50ms)` through them. Two `Acquire`s of the same Endpoint share one client. `Write` returns an error for envelopes with empty `Raw`. The receive loop restarts the client with backoff (250ms doubling to 5s) until all handles are released.

- [ ] **Step 1: Write the fake bus and failing tests**

`internal/bus/manager_test.go`:

```go
package bus

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/brutella/can"
	n2k "github.com/open-ships/n2k"
)

// fakeBus implements n2k.Bus. Frames written by the client are recorded;
// test-injected frames are delivered to the client's handler.
type fakeBus struct {
	mu      sync.Mutex
	handler func(can.Frame)
	written []can.Frame
	closed  chan struct{}
}

func newFakeBus() *fakeBus { return &fakeBus{closed: make(chan struct{})} }

func (f *fakeBus) Run(ctx context.Context, handler func(can.Frame)) error {
	f.mu.Lock()
	f.handler = handler
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.closed:
		return nil
	}
}

func (f *fakeBus) WriteFrame(frame can.Frame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, frame)
	return nil
}

func (f *fakeBus) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

func (f *fakeBus) inject(frame can.Frame) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h != nil {
		h(frame)
	}
}

func (f *fakeBus) writtenFrames() []can.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]can.Frame(nil), f.written...)
}

// vesselHeadingFrame is PGN 127250 (single-frame): priority 2, source 12,
// heading raw 15708 (0x3D5C), deviation/variation null, reference 0.
func vesselHeadingFrame() can.Frame {
	return can.Frame{
		ID:     0x89F1120C, // (2<<26)|(127250<<8)|12 with EFF flag semantics per brutella/can
		Length: 8,
		Data:   [8]uint8{0xFF, 0x5C, 0x3D, 0xFF, 0x7F, 0xFF, 0x7F, 0xFC},
	}
}

func testManager(t *testing.T, fake *fakeBus) *Manager {
	t.Helper()
	return NewManager(slog.Default(), nil,
		n2k.WithBus(fake), n2k.WithClaimTimeout(50*time.Millisecond))
}

func TestSubscribeReceivesDecodedEnvelope(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	ch, unsub := h.Subscribe(16)
	defer unsub()

	// give the client's read loop a moment, then inject
	time.Sleep(200 * time.Millisecond)
	fake.inject(vesselHeadingFrame())

	select {
	case e := <-ch:
		if e.PGN != 127250 || e.Source != 12 {
			t.Fatalf("envelope = %+v", e)
		}
		if e.PayloadMap()["heading"] == nil {
			t.Fatalf("payload missing heading: %s", e.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no envelope received")
	}
}

func TestSharedClientRefcount(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h1, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	if m.clientCount() != 1 {
		t.Fatalf("clientCount = %d, want 1 (shared)", m.clientCount())
	}
	h1.Release()
	if m.clientCount() != 1 {
		t.Fatal("client closed while still referenced")
	}
	h2.Release()
	if m.clientCount() != 0 {
		t.Fatal("client not closed after last release")
	}
}

func TestWriteEncodesToBus(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	// Build an envelope by decoding a known frame's payload
	src, _ := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	defer src.Release()
	ch, unsub := src.Subscribe(1)
	defer unsub()
	time.Sleep(200 * time.Millisecond)
	fake.inject(vesselHeadingFrame())
	e := <-ch

	before := len(fake.writtenFrames())
	if err := h.Write(ctx, e); err != nil {
		t.Fatal(err)
	}
	if len(fake.writtenFrames()) <= before {
		t.Fatal("no frame written to bus")
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/bus/ -v`
Expected: FAIL (undefined: NewManager, Endpoint, Manager).

Note: if `vesselHeadingFrame`'s ID constant doesn't decode (brutella/can EFF flag handling differs), check how n2k's own tests construct frames (`~/code/openships/n2k/receive_test.go` and `internal/framer`) and copy that exact construction. The frame must decode to PGN 127250 from source 12 — fix the test constant, not the manager.

- [ ] **Step 3: Implement manager.go**

```go
// Package bus owns one n2k.Client per physical CAN endpoint. NMEA 2000
// address claiming allows one bus participant per interface, so sources
// and sinks on the same interface share a client via refcounted handles.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	n2k "github.com/open-ships/n2k"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/msg"
)

type Endpoint struct {
	Kind string // "socketcan" | "usbcan"
	Name string // interface name or serial port path
}

func (ep Endpoint) option() (n2k.Option, error) {
	switch ep.Kind {
	case "socketcan":
		return n2k.CAN(ep.Name), nil
	case "usbcan":
		return n2k.USB(ep.Name), nil
	default:
		return nil, fmt.Errorf("unknown CAN endpoint kind %q", ep.Kind)
	}
}

type Manager struct {
	log       *slog.Logger
	met       *metrics.Set
	extraOpts []n2k.Option

	mu      sync.Mutex
	clients map[Endpoint]*busClient
}

func NewManager(log *slog.Logger, met *metrics.Set, extraOpts ...n2k.Option) *Manager {
	return &Manager{log: log, met: met, extraOpts: extraOpts, clients: map[Endpoint]*busClient{}}
}

func (m *Manager) clientCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients)
}

// Acquire returns a refcounted handle on the endpoint's shared client,
// starting it if this is the first reference.
func (m *Manager) Acquire(ctx context.Context, ep Endpoint) (*Handle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bc, ok := m.clients[ep]
	if !ok {
		opt, err := ep.option()
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		bc = &busClient{
			mgr: m, ep: ep, opt: opt, cancel: cancel,
			subs:  map[int64]chan *msg.Envelope{},
			state: "degraded",
		}
		m.clients[ep] = bc
		go bc.run(runCtx)
	}
	bc.refs++
	return &Handle{bc: bc}, nil
}

type busClient struct {
	mgr    *Manager
	ep     Endpoint
	opt    n2k.Option
	cancel context.CancelFunc

	mu      sync.Mutex
	refs    int
	client  *n2k.Client
	subs    map[int64]chan *msg.Envelope
	nextSub int64
	state   string
	lastErr error
}

// run maintains the client: (re)connect, pump Receive into subscribers,
// back off on failure, until cancelled by the last Release.
func (bc *busClient) run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		opts := append([]n2k.Option{bc.opt, n2k.IncludeUnknown(), n2k.WithLogger(bc.mgr.log)},
			bc.mgr.extraOpts...)
		client, err := n2k.NewClient(ctx, opts...)
		if err != nil {
			bc.setState("error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		bc.mu.Lock()
		bc.client = client
		bc.mu.Unlock()
		bc.setState("up", nil)
		backoff = 250 * time.Millisecond

		for m, err := range client.Receive() {
			if err != nil {
				bc.mgr.log.Debug("n2k receive error", "endpoint", bc.ep.Name, "err", err)
				continue
			}
			e, err := msg.FromPGN(m)
			if err != nil {
				bc.mgr.log.Debug("envelope conversion error", "err", err)
				continue
			}
			bc.broadcast(e)
		}
		// Receive ended: client dead or ctx cancelled.
		_ = client.Close()
		bc.mu.Lock()
		bc.client = nil
		bc.mu.Unlock()
		if ctx.Err() == nil {
			bc.setState("degraded", errors.New("receive loop ended; reconnecting"))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}
}

func (bc *busClient) broadcast(e *msg.Envelope) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for _, ch := range bc.subs {
		select {
		case ch <- e:
		default: // slow subscriber: drop rather than block the bus
		}
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

type Handle struct {
	bc       *busClient
	released sync.Once
}

func (h *Handle) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	bc := h.bc
	bc.mu.Lock()
	id := bc.nextSub
	bc.nextSub++
	ch := make(chan *msg.Envelope, buf)
	bc.subs[id] = ch
	bc.mu.Unlock()
	return ch, func() {
		bc.mu.Lock()
		delete(bc.subs, id)
		bc.mu.Unlock()
	}
}

// Write re-encodes the envelope onto the bus. Requires Raw bytes.
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
		return fmt.Errorf("decode envelope for CAN write: %w", err)
	}
	return client.Write(m).Wait()
}

func (h *Handle) State() (string, error) {
	h.bc.mu.Lock()
	defer h.bc.mu.Unlock()
	return h.bc.state, h.bc.lastErr
}

func (h *Handle) Release() {
	h.released.Do(func() {
		bc := h.bc
		mgr := bc.mgr
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		bc.mu.Lock()
		bc.refs--
		last := bc.refs == 0
		bc.mu.Unlock()
		if last {
			bc.cancel()
			delete(mgr.clients, bc.ep)
		}
	})
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/bus/ -v -timeout 60s`
Expected: PASS. If `Handle.Write` deadlocks because the fake bus's `Run` blocks writes, check n2k's writeLoop expectations — `WriteFrame` on the fake returns immediately, so `Wait()` should resolve.

- [ ] **Step 5: Commit**

```bash
git add internal/bus
git commit -m "feat: refcounted bus manager sharing one n2k client per CAN endpoint"
```

---

### Task 8: Source runtimes (hub + CAN + SSE/WS dialers)

**Files:**
- Create: `internal/source/source.go` (Runtime interface, hub, CAN source), `internal/source/http.go` (SSE + WS dialers)
- Test: `internal/source/source_test.go`, `internal/source/http_test.go`

**Interfaces:**
- Consumes: `bus.Manager`/`bus.Handle`, `model.Source`, `msg.Envelope`, `metrics.Set`, `github.com/coder/websocket`
- Produces:

```go
// Runtime is a running source: it broadcasts envelopes to subscribers.
type Runtime interface {
    ID() string
    Subscribe(buf int) (<-chan *msg.Envelope, func())
    Stop()
    State() (string, error) // "up" | "degraded" | "error"
}
// New starts the source runtime for cfg. CAN types acquire from mgr;
// HTTP types dial cfg.URL with reconnect backoff (250ms→5s).
func New(ctx context.Context, cfg model.Source, mgr *bus.Manager, log *slog.Logger, met *metrics.Set) (Runtime, error)
```

- HTTP SSE dialer: GET `cfg.URL` with `Accept: text/event-stream` + `cfg.Headers`; parses `data:` lines as envelope JSON (ignores `id:`/`event:`/comments); reconnects on EOF/error.
- HTTP WS dialer: `websocket.Dial(ctx, cfg.URL, ...)` with headers; each text message is one envelope JSON; reconnects on error.
- Every received envelope increments `met.SourceMessages` and is re-broadcast through an internal hub (same non-blocking drop-on-full semantics as the bus manager's broadcast).
- Incoming envelopes from HTTP get `Seq`/`ConnectorID` cleared (they are re-assigned by each connector's queue).

- [ ] **Step 1: Write failing tests**

`internal/source/http_test.go` (the CAN source is a thin wrapper over Task 7's tested Handle.Subscribe — the interesting behavior is the dialers):

```go
package source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

func envelopeJSON(pgn uint32) string {
	e := msg.Envelope{Seq: 99, ConnectorID: "upstream", PGN: pgn, Source: 7, Dest: 255,
		Priority: 2, Timestamp: time.Now(), Payload: json.RawMessage(`{"heading":15708}`)}
	b, _ := json.Marshal(&e)
	return string(b)
}

func TestSSEDialerReceivesAndReconnects(t *testing.T) {
	conns := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conns <- struct{}{}
		if got := r.Header.Get("Accept"); !strings.Contains(got, "text/event-stream") {
			t.Errorf("Accept = %q", got)
		}
		if r.Header.Get("X-Token") != "secret" {
			t.Errorf("custom header missing")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprintf(w, "id: upstream:99\ndata: %s\n\n", envelopeJSON(127250))
		fl.Flush()
		// close connection to force a reconnect
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "up", Name: "Upstream", Type: model.SourceHTTPSSE, Enabled: true,
		URL: srv.URL, Headers: map[string]string{"X-Token": "secret"},
	}, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	ch, unsub := rt.Subscribe(16)
	defer unsub()

	e := <-ch
	if e.PGN != 127250 {
		t.Fatalf("pgn = %d", e.PGN)
	}
	if e.Seq != 0 || e.ConnectorID != "" {
		t.Fatalf("upstream seq/connector must be cleared: %+v", e)
	}

	// server closed the stream; the dialer must reconnect
	select {
	case <-conns:
	case <-time.After(time.Second):
		t.Fatal("no first connection?")
	}
	select {
	case <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("dialer did not reconnect after stream end")
	}
}

func TestWSDialerReceives(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_ = c.Write(r.Context(), websocket.MessageText, []byte(envelopeJSON(128259)))
		time.Sleep(time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "upws", Name: "Upstream WS", Type: model.SourceHTTPWS, Enabled: true,
		URL: "ws" + strings.TrimPrefix(srv.URL, "http"),
	}, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	ch, unsub := rt.Subscribe(16)
	defer unsub()
	select {
	case e := <-ch:
		if e.PGN != 128259 {
			t.Fatalf("pgn = %d", e.PGN)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no envelope from WS dialer")
	}
}
```

`internal/source/source_test.go` — hub semantics:

```go
package source

import (
	"testing"

	"github.com/open-ships/beacon/internal/msg"
)

func TestHubFanOutAndUnsubscribe(t *testing.T) {
	h := newHub()
	a, unsubA := h.subscribe(4)
	b, _ := h.subscribe(4)

	h.publish(&msg.Envelope{PGN: 1})
	if e := <-a; e.PGN != 1 {
		t.Fatal("a missed message")
	}
	if e := <-b; e.PGN != 1 {
		t.Fatal("b missed message")
	}

	unsubA()
	h.publish(&msg.Envelope{PGN: 2})
	if e := <-b; e.PGN != 2 {
		t.Fatal("b missed second message")
	}
	select {
	case _, ok := <-a:
		if ok {
			t.Fatal("a received after unsubscribe")
		}
	default:
	}
}

func TestHubDropsWhenSubscriberFull(t *testing.T) {
	h := newHub()
	ch, _ := h.subscribe(1)
	h.publish(&msg.Envelope{PGN: 1})
	h.publish(&msg.Envelope{PGN: 2}) // must not block
	if e := <-ch; e.PGN != 1 {
		t.Fatal("first message lost")
	}
	select {
	case e := <-ch:
		t.Fatalf("unexpected second message %d (buffer 1 should have dropped it)", e.PGN)
	default:
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/source/ -v`
Expected: FAIL (undefined: New, newHub).

- [ ] **Step 3: Implement source.go**

```go
// Package source runs configured sources and fans their envelopes out to
// subscribing connectors.
package source

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

type Runtime interface {
	ID() string
	Subscribe(buf int) (<-chan *msg.Envelope, func())
	Stop()
	State() (string, error)
}

func New(ctx context.Context, cfg model.Source, mgr *bus.Manager, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	switch cfg.Type {
	case model.SourceSocketCAN, model.SourceUSBCAN:
		return newCANSource(ctx, cfg, mgr, log, met)
	case model.SourceHTTPSSE:
		return newDialerSource(ctx, cfg, log, met, runSSE)
	case model.SourceHTTPWS:
		return newDialerSource(ctx, cfg, log, met, runWS)
	default:
		return nil, fmt.Errorf("source %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

// hub is a non-blocking broadcast: slow subscribers drop, never block.
type hub struct {
	mu   sync.Mutex
	subs map[int64]chan *msg.Envelope
	next int64
}

func newHub() *hub { return &hub{subs: map[int64]chan *msg.Envelope{}} }

func (h *hub) subscribe(buf int) (<-chan *msg.Envelope, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.next
	h.next++
	ch := make(chan *msg.Envelope, buf)
	h.subs[id] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if c, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(c)
		}
	}
}

func (h *hub) publish(e *msg.Envelope) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (h *hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}

// canSource adapts a bus.Handle subscription into a source Runtime.
type canSource struct {
	id     string
	handle *bus.Handle
	hub    *hub
	cancel context.CancelFunc
}

func newCANSource(ctx context.Context, cfg model.Source, mgr *bus.Manager, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	ep := bus.Endpoint{Kind: string(cfg.Type), Name: cfg.Interface}
	if cfg.Type == model.SourceUSBCAN {
		ep.Name = cfg.Port
	}
	handle, err := mgr.Acquire(ctx, ep)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &canSource{id: cfg.ID, handle: handle, hub: newHub(), cancel: cancel}
	ch, unsub := handle.Subscribe(256)
	go func() {
		defer unsub()
		for {
			select {
			case <-runCtx.Done():
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				met.SourceMessages(runCtx, cfg.ID, 1)
				s.hub.publish(e)
			}
		}
	}()
	return s, nil
}

func (s *canSource) ID() string { return s.id }
func (s *canSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	return s.hub.subscribe(buf)
}
func (s *canSource) State() (string, error) { return s.handle.State() }
func (s *canSource) Stop() {
	s.cancel()
	s.hub.closeAll()
	s.handle.Release()
}
```

- [ ] **Step 4: Implement http.go**

```go
package source

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// runFunc runs one connection attempt, publishing envelopes until error.
type runFunc func(ctx context.Context, cfg model.Source, publish func(*msg.Envelope)) error

// dialerSource maintains a dial-reconnect loop around a runFunc.
type dialerSource struct {
	id     string
	hub    *hub
	cancel context.CancelFunc

	mu      sync.Mutex
	state   string
	lastErr error
}

func newDialerSource(ctx context.Context, cfg model.Source, log *slog.Logger, met *metrics.Set, run runFunc) (Runtime, error) {
	runCtx, cancel := context.WithCancel(ctx)
	s := &dialerSource{id: cfg.ID, hub: newHub(), cancel: cancel, state: "degraded"}
	publish := func(e *msg.Envelope) {
		e.Seq, e.ConnectorID = 0, "" // upstream identifiers do not survive re-ingest
		met.SourceMessages(runCtx, cfg.ID, 1)
		s.hub.publish(e)
	}
	go func() {
		backoff := 250 * time.Millisecond
		for runCtx.Err() == nil {
			s.setState("up", nil) // optimistic; run returns on failure
			err := run(runCtx, cfg, publish)
			if runCtx.Err() != nil {
				return
			}
			s.setState("degraded", err)
			log.Warn("source disconnected; reconnecting", "source", cfg.ID, "err", err)
			select {
			case <-runCtx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	}()
	_ = met
	return s, nil
}

func (s *dialerSource) setState(state string, err error) {
	s.mu.Lock()
	s.state, s.lastErr = state, err
	s.mu.Unlock()
}

func (s *dialerSource) ID() string { return s.id }
func (s *dialerSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	return s.hub.subscribe(buf)
}
func (s *dialerSource) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.lastErr
}
func (s *dialerSource) Stop() {
	s.cancel()
	s.hub.closeAll()
}

// runSSE consumes a Server-Sent Events stream of envelope JSON.
func runSSE(ctx context.Context, cfg model.Source, publish func(*msg.Envelope)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sse endpoint returned %s", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // id:, event:, retry:, comments, blank separators
		}
		var e msg.Envelope
		if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &e); err != nil {
			continue // tolerate junk lines
		}
		publish(&e)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("sse stream ended")
}

// runWS consumes NDJSON text messages from a WebSocket.
func runWS(ctx context.Context, cfg model.Source, publish func(*msg.Envelope)) error {
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	for k, v := range cfg.Headers {
		opts.HTTPHeader.Set(k, v)
	}
	c, _, err := websocket.Dial(ctx, cfg.URL, opts)
	if err != nil {
		return err
	}
	defer c.CloseNow()
	c.SetReadLimit(1024 * 1024)
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		var e msg.Envelope
		if err := json.Unmarshal(data, &e); err != nil {
			continue
		}
		publish(&e)
	}
}
```

- [ ] **Step 5: Run tests, verify pass**

Run: `go test ./internal/source/ -v -timeout 60s`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/source
git commit -m "feat: source runtimes - CAN via bus manager, SSE/WS dialers with reconnect"
```

---

### Task 9: Sink runtimes (CAN + data server + SSE/WS/TCP)

**Files:**
- Create: `internal/sink/sink.go` (interfaces + CAN sink), `internal/sink/dataserver.go`, `internal/sink/serve.go` (SSE + WS), `internal/sink/tcp.go`
- Test: `internal/sink/serve_test.go`, `internal/sink/tcp_test.go`

**Interfaces:**
- Consumes: `bus.Manager`/`bus.Handle`, `model.Sink`, `queue.Entry`, `msg.Envelope`, `metrics.Set`, `github.com/coder/websocket`
- Produces:

```go
type Runtime interface {
    ID() string
    Stop()
    State() (string, error)
}
// Pusher sinks confirm each delivery (CAN). ErrSkip means "cannot carry
// this message, count it and move on" (e.g. envelope without raw bytes).
var ErrSkip = errors.New("sink skipped message")
type Pusher interface{ Push(ctx context.Context, e *msg.Envelope) error }
// Broadcaster sinks fan out to connected clients without confirmation.
type Broadcaster interface{ Broadcast(entries []queue.Entry) }
// ReplayReader is what serve-mode sinks use to replay history for a client.
type ReplayReader interface {
    Read(ctx context.Context, after int64, limit int) ([]queue.Entry, error)
}
// ConnectorRegistrar lets connectors attach their queue for client replay.
type ConnectorRegistrar interface {
    RegisterConnector(id string, r ReplayReader)
    UnregisterConnector(id string)
}
func New(ctx context.Context, cfg model.Sink, mgr *bus.Manager, ds *DataServer, log *slog.Logger, met *metrics.Set) (Runtime, error)

type DataServer struct{ ... }
func NewDataServer(addr string, log *slog.Logger) *DataServer
func (d *DataServer) Start() error        // binds; serves in background
func (d *DataServer) Addr() string        // bound address (for tests with :0)
func (d *DataServer) SetRoute(path string, h http.Handler)
func (d *DataServer) RemoveRoute(path string)
func (d *DataServer) Stop(ctx context.Context) error
```

- Event id wire format everywhere: `<connectorID>:<seq>` (SSE `id:` line; envelope JSON already carries `connector` + `id` fields).
- Replay request format: SSE `Last-Event-ID` header or `?after=` query (both accept comma-separated `connector:seq` pairs); WS `?after=` only. Replay pulls from each named connector's `ReplayReader` in batches of 256 until caught up, then the client joins the live broadcast. TCP is live-only.
- Slow client policy: per-client buffered channel (cap 256); on overflow the client is disconnected (SSE/WS) or has its connection closed (TCP). `met.SinkClients(id, ±1)` on connect/disconnect.

- [ ] **Step 1: Write failing tests**

`internal/sink/serve_test.go`:

```go
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

// memReplay is an in-memory ReplayReader.
type memReplay struct{ entries []queue.Entry }

func (m *memReplay) Read(_ context.Context, after int64, limit int) ([]queue.Entry, error) {
	var out []queue.Entry
	for _, e := range m.entries {
		if e.Seq > after && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func entry(connector string, seq int64, pgn uint32) queue.Entry {
	return queue.Entry{Seq: seq, Env: &msg.Envelope{
		Seq: seq, ConnectorID: connector, PGN: pgn, Source: 1, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}}
}

func startSSE(t *testing.T) (*DataServer, Runtime) {
	t.Helper()
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	rt, err := New(context.Background(), model.Sink{
		ID: "sse", Name: "SSE", Type: model.SinkHTTPSSE, Enabled: true, Path: "/events",
	}, nil, ds, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Stop)
	return ds, rt
}

func readSSEEvents(t *testing.T, resp *http.Response, n int, timeout time.Duration) []map[string]any {
	t.Helper()
	var out []map[string]any
	deadline := time.After(timeout)
	lines := make(chan string, 64)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	for len(out) < n {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed after %d events, want %d", len(out), n)
			}
			if strings.HasPrefix(line, "data:") {
				var m map[string]any
				if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &m); err != nil {
					t.Fatalf("bad data line %q: %v", line, err)
				}
				out = append(out, m)
			}
		case <-deadline:
			t.Fatalf("timeout after %d events, want %d", len(out), n)
		}
	}
	return out
}

func TestSSELiveBroadcast(t *testing.T) {
	ds, rt := startSSE(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/events", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	time.Sleep(100 * time.Millisecond) // let the client register
	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 1, 127250)})
	events := readSSEEvents(t, resp, 1, 3*time.Second)
	if events[0]["pgn"].(float64) != 127250 {
		t.Fatalf("event = %v", events[0])
	}
}

func TestSSEReplayWithLastEventID(t *testing.T) {
	ds, rt := startSSE(t)
	replay := &memReplay{entries: []queue.Entry{
		entry("nav", 1, 127250), entry("nav", 2, 128259), entry("nav", 3, 129026)}}
	rt.(ConnectorRegistrar).RegisterConnector("nav", replay)

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/events", ds.Addr()), nil)
	req.Header.Set("Last-Event-ID", "nav:1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readSSEEvents(t, resp, 2, 3*time.Second)
	if events[0]["pgn"].(float64) != 128259 || events[1]["pgn"].(float64) != 129026 {
		t.Fatalf("replay wrong: %v", events)
	}
}

func TestDataServerRouteLifecycle(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	defer ds.Stop(context.Background())

	resp, _ := http.Get(fmt.Sprintf("http://%s/nope", ds.Addr()))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unrouted path = %d, want 404", resp.StatusCode)
	}
	ds.SetRoute("/x", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	resp, _ = http.Get(fmt.Sprintf("http://%s/x", ds.Addr()))
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("routed path = %d, want 418", resp.StatusCode)
	}
	ds.RemoveRoute("/x")
	resp, _ = http.Get(fmt.Sprintf("http://%s/x", ds.Addr()))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("removed path = %d, want 404", resp.StatusCode)
	}
}
```

`internal/sink/tcp_test.go`:

```go
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

func TestTCPLiveTail(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp", Name: "TCP", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	addr := rt.(interface{ Addr() string }).Addr()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(100 * time.Millisecond)

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 7, 127250)})

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatal(err)
	}
	if m["pgn"].(float64) != 127250 || m["connector"].(string) != "nav" {
		t.Fatalf("line = %s", line)
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/sink/ -v`
Expected: FAIL (undefined: NewDataServer, New).

- [ ] **Step 3: Implement sink.go (interfaces + CAN sink)**

```go
// Package sink runs configured sinks. CAN sinks push-confirm each message
// onto the bus; HTTP/TCP sinks broadcast to connected clients, with
// replay served straight from connector queues (SSE/WS only).
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

var ErrSkip = errors.New("sink skipped message")

type Runtime interface {
	ID() string
	Stop()
	State() (string, error)
}

type Pusher interface {
	Push(ctx context.Context, e *msg.Envelope) error
}

type Broadcaster interface {
	Broadcast(entries []queue.Entry)
}

type ReplayReader interface {
	Read(ctx context.Context, after int64, limit int) ([]queue.Entry, error)
}

type ConnectorRegistrar interface {
	RegisterConnector(id string, r ReplayReader)
	UnregisterConnector(id string)
}

func New(ctx context.Context, cfg model.Sink, mgr *bus.Manager, ds *DataServer, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	switch cfg.Type {
	case model.SinkSocketCAN, model.SinkUSBCAN:
		return newCANSink(ctx, cfg, mgr)
	case model.SinkHTTPSSE:
		return newServeSink(cfg, ds, log, met, serveSSE)
	case model.SinkHTTPWS:
		return newServeSink(cfg, ds, log, met, serveWS)
	case model.SinkTCP:
		return newTCPSink(cfg, log, met)
	default:
		return nil, fmt.Errorf("sink %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

var _ http.Handler = http.NotFoundHandler() // keep net/http import honest

type canSink struct {
	id     string
	handle *bus.Handle
}

func newCANSink(ctx context.Context, cfg model.Sink, mgr *bus.Manager) (Runtime, error) {
	ep := bus.Endpoint{Kind: string(cfg.Type), Name: cfg.Interface}
	if cfg.Type == model.SinkUSBCAN {
		ep.Name = cfg.Port
	}
	handle, err := mgr.Acquire(ctx, ep)
	if err != nil {
		return nil, err
	}
	return &canSink{id: cfg.ID, handle: handle}, nil
}

func (s *canSink) ID() string { return s.id }

// Push writes one envelope onto the bus; envelopes without raw bytes are
// skipped (ErrSkip) since they cannot be encoded.
func (s *canSink) Push(ctx context.Context, e *msg.Envelope) error {
	if len(e.Raw) == 0 {
		return ErrSkip
	}
	return s.handle.Write(ctx, e)
}

func (s *canSink) State() (string, error) { return s.handle.State() }
func (s *canSink) Stop()                  { s.handle.Release() }
```

- [ ] **Step 4: Implement dataserver.go**

```go
package sink

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
)

// DataServer hosts serve-mode sink endpoints with dynamically managed routes.
type DataServer struct {
	log *slog.Logger
	srv *http.Server
	ln  net.Listener

	mu     sync.RWMutex
	routes map[string]http.Handler
}

func NewDataServer(addr string, log *slog.Logger) *DataServer {
	d := &DataServer{log: log, routes: map[string]http.Handler{}}
	d.srv = &http.Server{Addr: addr, Handler: http.HandlerFunc(d.route)}
	return d
}

func (d *DataServer) route(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	h, ok := d.routes[r.URL.Path]
	d.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

func (d *DataServer) Start() error {
	ln, err := net.Listen("tcp", d.srv.Addr)
	if err != nil {
		return err
	}
	d.ln = ln
	go func() {
		if err := d.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			d.log.Error("data server error", "err", err)
		}
	}()
	return nil
}

func (d *DataServer) Addr() string {
	if d.ln == nil {
		return d.srv.Addr
	}
	return d.ln.Addr().String()
}

func (d *DataServer) SetRoute(path string, h http.Handler) {
	d.mu.Lock()
	d.routes[path] = h
	d.mu.Unlock()
}

func (d *DataServer) RemoveRoute(path string) {
	d.mu.Lock()
	delete(d.routes, path)
	d.mu.Unlock()
}

func (d *DataServer) Stop(ctx context.Context) error { return d.srv.Shutdown(ctx) }
```

- [ ] **Step 5: Implement serve.go (shared serve-sink core + SSE + WS)**

```go
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

const clientBuf = 256

type client struct {
	ch   chan queue.Entry
	drop func()
}

// serveHandler streams entries to one connected client; implementations
// differ only in wire format (SSE frames vs WS text messages).
type serveHandler func(s *serveSink, w http.ResponseWriter, r *http.Request)

type serveSink struct {
	id   string
	path string
	ds   *DataServer
	log  *slog.Logger
	met  *metrics.Set

	mu      sync.Mutex
	clients map[*client]bool
	replays map[string]ReplayReader
	stopped bool
}

func newServeSink(cfg model.Sink, ds *DataServer, log *slog.Logger, met *metrics.Set, h serveHandler) (Runtime, error) {
	s := &serveSink{id: cfg.ID, path: cfg.Path, ds: ds, log: log, met: met,
		clients: map[*client]bool{}, replays: map[string]ReplayReader{}}
	ds.SetRoute(cfg.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h(s, w, r) }))
	return s, nil
}

func (s *serveSink) ID() string { return s.id }
func (s *serveSink) State() (string, error) {
	return "up", nil // route is registered; per-client failures are per-client
}

func (s *serveSink) Stop() {
	s.ds.RemoveRoute(s.path)
	s.mu.Lock()
	s.stopped = true
	for c := range s.clients {
		close(c.ch)
		delete(s.clients, c)
	}
	s.mu.Unlock()
}

func (s *serveSink) RegisterConnector(id string, r ReplayReader) {
	s.mu.Lock()
	s.replays[id] = r
	s.mu.Unlock()
}

func (s *serveSink) UnregisterConnector(id string) {
	s.mu.Lock()
	delete(s.replays, id)
	s.mu.Unlock()
}

// Broadcast delivers entries to every connected client; clients that
// cannot keep up are dropped (they can replay on reconnect).
func (s *serveSink) Broadcast(entries []queue.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		for c := range s.clients {
			select {
			case c.ch <- e:
			default:
				delete(s.clients, c)
				close(c.ch)
				go c.drop()
			}
		}
	}
}

func (s *serveSink) addClient(drop func()) (*client, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, false
	}
	c := &client{ch: make(chan queue.Entry, clientBuf), drop: drop}
	s.clients[c] = true
	s.met.SinkClients(s.id, 1)
	return c, true
}

func (s *serveSink) removeClient(c *client) {
	s.mu.Lock()
	if s.clients[c] {
		delete(s.clients, c)
		close(c.ch)
	}
	s.mu.Unlock()
	s.met.SinkClients(s.id, -1)
}

// parseAfter parses "nav:12,engine:9" into cursors.
func parseAfter(vals ...string) map[string]int64 {
	out := map[string]int64{}
	for _, v := range vals {
		for _, pair := range strings.Split(v, ",") {
			pair = strings.TrimSpace(pair)
			i := strings.LastIndex(pair, ":")
			if i <= 0 {
				continue
			}
			seq, err := strconv.ParseInt(pair[i+1:], 10, 64)
			if err != nil {
				continue
			}
			out[pair[:i]] = seq
		}
	}
	return out
}

// replayEntries streams history for each requested connector cursor.
func (s *serveSink) replayEntries(ctx context.Context, cursors map[string]int64, emit func(queue.Entry) error) error {
	for connectorID, after := range cursors {
		s.mu.Lock()
		reader, ok := s.replays[connectorID]
		s.mu.Unlock()
		if !ok {
			continue
		}
		cur := after
		for {
			entries, err := reader.Read(ctx, cur, 256)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				break
			}
			for _, e := range entries {
				if err := emit(e); err != nil {
					return err
				}
				cur = e.Seq
			}
		}
	}
	return nil
}

func eventID(e queue.Entry) string {
	return fmt.Sprintf("%s:%d", e.Env.ConnectorID, e.Seq)
}

func serveSSE(s *serveSink, w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	fl.Flush()

	writeEvent := func(e queue.Entry) error {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", eventID(e), doc); err != nil {
			return err
		}
		fl.Flush()
		return nil
	}

	// Subscribe live first, then replay: duplicates over gaps (at-least-once).
	ctx := r.Context()
	c, ok := s.addClient(func() {})
	if !ok {
		return
	}
	defer s.removeClient(c)

	cursors := parseAfter(r.Header.Get("Last-Event-ID"), r.URL.Query().Get("after"))
	if err := s.replayEntries(ctx, cursors, writeEvent); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-c.ch:
			if !open {
				return
			}
			if err := writeEvent(e); err != nil {
				return
			}
		}
	}
}

func serveWS(s *serveSink, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := r.Context()

	writeEvent := func(e queue.Entry) error {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			return err
		}
		return conn.Write(ctx, websocket.MessageText, doc)
	}

	c, ok := s.addClient(func() { conn.Close(websocket.StatusPolicyViolation, "client too slow") })
	if !ok {
		return
	}
	defer s.removeClient(c)

	if err := s.replayEntries(ctx, parseAfter(r.URL.Query().Get("after")), writeEvent); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-c.ch:
			if !open {
				return
			}
			if err := writeEvent(e); err != nil {
				return
			}
		}
	}
}
```

- [ ] **Step 6: Implement tcp.go**

```go
package sink

import (
	"encoding/json"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

// tcpSink serves a live NDJSON tail; no replay (nc-grade simplicity).
type tcpSink struct {
	id  string
	log *slog.Logger
	met *metrics.Set
	ln  net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]bool
	stopped bool
}

func newTCPSink(cfg model.Sink, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}
	s := &tcpSink{id: cfg.ID, log: log, met: met, ln: ln, conns: map[net.Conn]bool{}}
	go s.acceptLoop()
	return s, nil
}

func (s *tcpSink) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = true
		s.mu.Unlock()
		s.met.SinkClients(s.id, 1)
	}
}

func (s *tcpSink) ID() string { return s.id }
func (s *tcpSink) Addr() string { return s.ln.Addr().String() }
func (s *tcpSink) State() (string, error) { return "up", nil }

func (s *tcpSink) Broadcast(entries []queue.Entry) {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, e := range entries {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			continue
		}
		doc = append(doc, '\n')
		for _, c := range conns {
			_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Write(doc); err != nil {
				s.dropConn(c)
			}
		}
	}
}

func (s *tcpSink) dropConn(c net.Conn) {
	s.mu.Lock()
	known := s.conns[c]
	delete(s.conns, c)
	s.mu.Unlock()
	if known {
		_ = c.Close()
		s.met.SinkClients(s.id, -1)
	}
}

func (s *tcpSink) Stop() {
	s.mu.Lock()
	s.stopped = true
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = map[net.Conn]bool{}
	s.mu.Unlock()
	_ = s.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}
```

- [ ] **Step 7: Run tests, verify pass**

Run: `go test ./internal/sink/ -v -timeout 60s`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/sink
git commit -m "feat: sink runtimes - CAN push, served SSE/WS with replay, TCP live tail"
```

---

### Task 10: Connector pipeline

**Files:**
- Create: `internal/connector/connector.go`
- Test: `internal/connector/connector_test.go`

**Interfaces:**
- Consumes: `source.Runtime`, `sink.Runtime` (+ `sink.Pusher`/`sink.Broadcaster`/`sink.ConnectorRegistrar`/`sink.ErrSkip`), `queue.Queue`, `filter.Chain`, `model.Connector`, `metrics.Set`
- Produces: `connector.New(cfg model.Connector, src source.Runtime, snk sink.Runtime, q queue.Queue, chain *filter.Chain, log *slog.Logger, met *metrics.Set) *Connector`; `(*Connector).Start(ctx context.Context)`; `(*Connector).Stop()` (graceful: final Ack flushed); `(*Connector).ID() string`

Behavior contract:
- Intake: subscribe (buf 256) → `Match` → batch-append (flush at 64 msgs or 50ms) → notify delivery. Metrics: `received` per message, `matched` per pass, `filter_error` per eval error (message dropped).
- Delivery: resume from `q.Cursor()`. Pusher sinks: per-entry `Push`; `ErrSkip` counts stage `delivered` (with the skip logged at debug); other errors retry the same entry with backoff 250ms→5s until success or Stop. Broadcaster sinks: `Broadcast(batch)` then advance. Ack at most every 500ms plus once on Stop. Metrics: `delivered` + `ConnectorBytes` per delivered entry.
- Registration: if the sink implements `ConnectorRegistrar`, `RegisterConnector(cfg.ID, q)` on Start (queue.Queue satisfies ReplayReader's Read signature) and `UnregisterConnector` on Stop.
- Prune loop every 5s: `q.Prune()` → stage `pruned` count; `q.Stats()` → `SetQueueDepth`.

- [ ] **Step 1: Write failing tests**

`internal/connector/connector_test.go`:

```go
package connector

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/store"
)

// fakeSource implements source.Runtime.
type fakeSource struct {
	mu   sync.Mutex
	subs []chan *msg.Envelope
}

func (f *fakeSource) ID() string { return "fake-src" }
func (f *fakeSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *msg.Envelope, buf)
	f.subs = append(f.subs, ch)
	return ch, func() {}
}
func (f *fakeSource) Stop()                  {}
func (f *fakeSource) State() (string, error) { return "up", nil }
func (f *fakeSource) emit(e *msg.Envelope) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		ch <- e
	}
}

// pushSink records pushes; can fail N times first.
type pushSink struct {
	mu       sync.Mutex
	failures int
	got      []*msg.Envelope
}

func (p *pushSink) ID() string             { return "fake-push" }
func (p *pushSink) Stop()                  {}
func (p *pushSink) State() (string, error) { return "up", nil }
func (p *pushSink) Push(_ context.Context, e *msg.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("bus down")
	}
	p.got = append(p.got, e)
	return nil
}
func (p *pushSink) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.got)
}

// bcastSink records broadcasts and registrations.
type bcastSink struct {
	mu         sync.Mutex
	got        []queue.Entry
	registered map[string]sink.ReplayReader
}

func (b *bcastSink) ID() string             { return "fake-bcast" }
func (b *bcastSink) Stop()                  {}
func (b *bcastSink) State() (string, error) { return "up", nil }
func (b *bcastSink) Broadcast(entries []queue.Entry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got = append(b.got, entries...)
}
func (b *bcastSink) RegisterConnector(id string, r sink.ReplayReader) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.registered == nil {
		b.registered = map[string]sink.ReplayReader{}
	}
	b.registered[id] = r
}
func (b *bcastSink) UnregisterConnector(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.registered, id)
}
func (b *bcastSink) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

func testQueue(t *testing.T) queue.Queue {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
}

func env(pgnNum uint32) *msg.Envelope {
	return &msg.Envelope{PGN: pgnNum, Source: 1, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(`{}`), Raw: []byte{1}}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestFilterThenBroadcast(t *testing.T) {
	src := &fakeSource{}
	snk := &bcastSink{}
	chain, _ := filter.Compile([]string{"msg.pgn == 127250"})
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	src.emit(env(127250))
	src.emit(env(999))    // filtered out
	src.emit(env(127250))

	waitFor(t, 3*time.Second, func() bool { return snk.count() == 2 }, "2 broadcasts")
	if snk.registered["conn1"] == nil {
		t.Fatal("connector did not register for replay")
	}
}

func TestPushRetriesUntilSinkRecovers(t *testing.T) {
	src := &fakeSource{}
	snk := &pushSink{failures: 3}
	chain, _ := filter.Compile(nil)
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	src.emit(env(127250))
	waitFor(t, 10*time.Second, func() bool { return snk.count() == 1 }, "delivery after retries")
}

func TestResumeFromCheckpointAfterRestart(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	chain, _ := filter.Compile(nil)

	// First run: deliver 2 messages.
	src1 := &fakeSource{}
	snk1 := &pushSink{}
	c1 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src1, snk1, q, chain, slog.Default(), nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	c1.Start(ctx1)
	src1.emit(env(1))
	src1.emit(env(2))
	waitFor(t, 3*time.Second, func() bool { return snk1.count() == 2 }, "first run deliveries")
	c1.Stop()
	cancel1()

	// Second run on the same queue: nothing replays (checkpoint held).
	src2 := &fakeSource{}
	snk2 := &pushSink{}
	c2 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src2, snk2, q, chain, slog.Default(), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2.Start(ctx2)
	defer c2.Stop()
	src2.emit(env(3))
	waitFor(t, 3*time.Second, func() bool { return snk2.count() == 1 }, "second run delivery")
	if got := snk2.got[0].PGN; got != 3 {
		t.Fatalf("redelivered old message, pgn=%d", got)
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/connector/ -v`
Expected: FAIL (undefined: New).

- [ ] **Step 3: Implement connector.go**

```go
// Package connector runs the per-connector pipeline: source subscription →
// CEL filter → durable queue → sink delivery with checkpointing.
package connector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
)

const (
	batchSize     = 64
	batchInterval = 50 * time.Millisecond
	readLimit     = 256
	ackInterval   = 500 * time.Millisecond
	pruneInterval = 5 * time.Second
	maxBackoff    = 5 * time.Second
)

type Connector struct {
	cfg   model.Connector
	src   source.Runtime
	snk   sink.Runtime
	q     queue.Queue
	chain *filter.Chain
	log   *slog.Logger
	met   *metrics.Set

	cancel context.CancelFunc
	notify chan struct{}
	wg     sync.WaitGroup
}

func New(cfg model.Connector, src source.Runtime, snk sink.Runtime, q queue.Queue,
	chain *filter.Chain, log *slog.Logger, met *metrics.Set) *Connector {
	return &Connector{cfg: cfg, src: src, snk: snk, q: q, chain: chain,
		log: log.With("connector", cfg.ID), met: met, notify: make(chan struct{}, 1)}
}

func (c *Connector) ID() string { return c.cfg.ID }

func (c *Connector) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	if reg, ok := c.snk.(sink.ConnectorRegistrar); ok {
		reg.RegisterConnector(c.cfg.ID, c.q)
	}
	c.wg.Add(3)
	go c.intake(runCtx)
	go c.deliver(runCtx)
	go c.prune(runCtx)
}

func (c *Connector) Stop() {
	if reg, ok := c.snk.(sink.ConnectorRegistrar); ok {
		reg.UnregisterConnector(c.cfg.ID)
	}
	c.cancel()
	c.wg.Wait()
}

func (c *Connector) wake() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *Connector) intake(ctx context.Context) {
	defer c.wg.Done()
	in, unsub := c.src.Subscribe(readLimit)
	defer unsub()

	batch := make([]*msg.Envelope, 0, batchSize)
	flushTimer := time.NewTicker(batchInterval)
	defer flushTimer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.q.Append(ctx, batch); err != nil {
			if ctx.Err() == nil {
				c.log.Error("queue append failed", "err", err)
			}
		} else {
			c.wake()
		}
		batch = batch[:0]
	}
	defer flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushTimer.C:
			flush()
		case e, ok := <-in:
			if !ok {
				return
			}
			c.met.ConnectorMessages(ctx, c.cfg.ID, "received", 1)
			match, err := c.chain.Match(e)
			if err != nil {
				c.met.ConnectorMessages(ctx, c.cfg.ID, "filter_error", 1)
				c.log.Debug("filter eval error", "err", err)
				continue
			}
			if !match {
				continue
			}
			c.met.ConnectorMessages(ctx, c.cfg.ID, "matched", 1)
			batch = append(batch, e)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
}

func (c *Connector) deliver(ctx context.Context) {
	defer c.wg.Done()
	cursor, err := c.q.Cursor(ctx)
	if err != nil {
		c.log.Error("read checkpoint", "err", err)
	}
	lastAck := time.Now()
	dirty := false
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()

	ack := func(force bool) {
		if !dirty || (!force && time.Since(lastAck) < ackInterval) {
			return
		}
		// use Background: final ack must survive ctx cancellation
		if err := c.q.Ack(context.Background(), cursor); err == nil {
			dirty = false
			lastAck = time.Now()
		}
	}
	defer ack(true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.notify:
		case <-poll.C:
		}
		for {
			entries, err := c.q.Read(ctx, cursor, readLimit)
			if err != nil {
				if ctx.Err() == nil {
					c.log.Error("queue read failed", "err", err)
				}
				break
			}
			if len(entries) == 0 {
				break
			}
			switch snk := c.snk.(type) {
			case sink.Pusher:
				if !c.pushAll(ctx, snk, entries, &cursor) {
					return // ctx cancelled mid-retry
				}
			case sink.Broadcaster:
				snk.Broadcast(entries)
				cursor = entries[len(entries)-1].Seq
				for _, e := range entries {
					c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
					c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
				}
			default:
				c.log.Error("sink implements neither Pusher nor Broadcaster")
				return
			}
			dirty = true
			ack(false)
		}
	}
}

// pushAll delivers entries one-by-one with retry; returns false if ctx ended.
func (c *Connector) pushAll(ctx context.Context, p sink.Pusher, entries []queue.Entry, cursor *int64) bool {
	for _, e := range entries {
		backoff := 250 * time.Millisecond
		for {
			err := p.Push(ctx, e.Env)
			if err == nil || errors.Is(err, sink.ErrSkip) {
				if errors.Is(err, sink.ErrSkip) {
					c.log.Debug("sink skipped message", "pgn", e.Env.PGN)
				}
				c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
				c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
				*cursor = e.Seq
				break
			}
			c.log.Debug("push failed; retrying", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return false
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}
	return true
}

func (c *Connector) prune(ctx context.Context) {
	defer c.wg.Done()
	t := time.NewTicker(pruneInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := c.q.Prune(ctx); err == nil && n > 0 {
				c.met.ConnectorMessages(ctx, c.cfg.ID, "pruned", n)
			}
			if st, err := c.q.Stats(ctx); err == nil {
				c.met.SetQueueDepth(c.cfg.ID, st.Depth, st.Bytes)
			}
		}
	}
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/connector/ -v -timeout 60s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/connector
git commit -m "feat: connector pipeline - filtered intake, durable delivery, checkpoint resume"
```

---

### Task 11: Supervisor (reconciler)

**Files:**
- Create: `internal/supervisor/supervisor.go`
- Test: `internal/supervisor/supervisor_test.go`

**Interfaces:**
- Consumes: `store.Store` (`LoadConfig`), `bus.Manager`, `sink.DataServer`, `source.New`, `sink.New`, `connector.New`, `queue.NewSQLite`, `filter.Compile`, `model.*`, `metrics.Set`
- Produces:

```go
type Supervisor struct{ ... }
func New(st *store.Store, busMgr *bus.Manager, ds *sink.DataServer, log *slog.Logger, met *metrics.Set) *Supervisor
// Reconcile diffs desired config (store) against running components and
// converges. Safe to call repeatedly; returns only on store read errors.
func (s *Supervisor) Reconcile(ctx context.Context) error
func (s *Supervisor) Stop()   // stops everything (connectors → sinks → sources)
type Status struct{ Kind, ID, State string; Err string }
func (s *Supervisor) Statuses() []Status  // for /health
```

Reconcile contract:
- Desired = enabled entities from `store.LoadConfig` (disabled or deleted ⇒ not desired).
- Identity for change detection: marshal the entity to JSON, compare with the running copy's JSON (`hash`). Changed ⇒ stop old, start new.
- Order: stop connectors, then sinks, then sources; start sources, then sinks, then connectors.
- A source/sink whose constructor fails records `Status{State: "error"}` and stays in an error list (retried on next Reconcile); connectors referencing it record `error` too and are skipped. Nothing panics, Reconcile never returns an error for component failures.
- A connector whose CEL fails to compile records `error` (this normally cannot happen — config writes validate — but a hand-edited DB must not crash the process).
- Each connector gets `queue.NewSQLite(st, connectorID, cfg.Buffer)`.

- [ ] **Step 1: Write failing tests**

`internal/supervisor/supervisor_test.go` (uses HTTP/TCP components only — no CAN hardware):

```go
package supervisor

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/store"
)

func setup(t *testing.T) (*store.Store, *Supervisor, *sink.DataServer) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds := sink.NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	sup := New(st, nil, ds, slog.Default(), nil)
	t.Cleanup(sup.Stop)
	return st, sup, ds
}

func find(statuses []Status, kind, id string) *Status {
	for i := range statuses {
		if statuses[i].Kind == kind && statuses[i].ID == id {
			return &statuses[i]
		}
	}
	return nil
}

func baseConfig() model.Config {
	return model.Config{
		Sources: []model.Source{{ID: "up", Name: "Upstream", Type: model.SourceHTTPWS,
			Enabled: true, URL: "ws://127.0.0.1:1/nowhere"}}, // degraded but running
		Sinks: []model.Sink{{ID: "out", Name: "Out", Type: model.SinkHTTPSSE,
			Enabled: true, Path: "/out"}},
		Connectors: []model.Connector{{ID: "link", Name: "Link", SourceID: "up",
			SinkID: "out", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}}},
	}
}

func TestReconcileStartsAndStops(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	if err := st.ReplaceConfig(ctx, baseConfig()); err != nil {
		t.Fatal(err)
	}
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts := sup.Statuses()
	if find(sts, "source", "up") == nil || find(sts, "sink", "out") == nil || find(sts, "connector", "link") == nil {
		t.Fatalf("missing components: %+v", sts)
	}

	// Remove the connector; source+sink stay.
	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts = sup.Statuses()
	if find(sts, "connector", "link") != nil {
		t.Fatal("removed connector still running")
	}
	if find(sts, "source", "up") == nil {
		t.Fatal("source should still be running")
	}
}

func TestReconcileRestartsOnChange(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// Change the sink path; supervisor must swap the route.
	cfg := baseConfig()
	cfg.Sinks[0].Path = "/renamed"
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if s := find(sup.Statuses(), "sink", "out"); s == nil || s.State == "error" {
		t.Fatalf("sink after change: %+v", s)
	}
}

func TestDisabledMeansStopped(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Connectors[0].Enabled = false
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if find(sup.Statuses(), "connector", "link") != nil {
		t.Fatal("disabled connector is running")
	}
}

func TestBrokenComponentIsErrorNotCrash(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	// TCP sink on an unbindable address fails to construct.
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "bad", Name: "Bad", Type: model.SinkTCP,
		Enabled: true, Address: "256.256.256.256:1"})
	cfg.Connectors = append(cfg.Connectors, model.Connector{ID: "cbad", Name: "CBad",
		SourceID: "up", SinkID: "bad", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 5}})
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts := sup.Statuses()
	if s := find(sts, "sink", "bad"); s == nil || s.State != "error" {
		t.Fatalf("bad sink status: %+v", s)
	}
	if s := find(sts, "connector", "cbad"); s == nil || s.State != "error" {
		t.Fatalf("connector on bad sink status: %+v", s)
	}
	// healthy components unaffected
	if s := find(sts, "connector", "link"); s == nil || s.State == "error" {
		t.Fatalf("healthy connector harmed: %+v", s)
	}
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./internal/supervisor/ -v`
Expected: FAIL (undefined: New, Status).

- [ ] **Step 3: Implement supervisor.go**

```go
// Package supervisor reconciles desired configuration (the store) against
// running source/sink/connector components.
package supervisor

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/connector"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
	"github.com/open-ships/beacon/internal/store"
)

type Status struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	State string `json:"state"` // "up" | "degraded" | "error"
	Err   string `json:"err,omitempty"`
}

type runningSource struct {
	hash string
	rt   source.Runtime
}

type runningSink struct {
	hash string
	rt   sink.Runtime
}

type runningConnector struct {
	hash string
	c    *connector.Connector
}

type Supervisor struct {
	st  *store.Store
	bus *bus.Manager
	ds  *sink.DataServer
	log *slog.Logger
	met *metrics.Set

	mu         sync.Mutex
	sources    map[string]*runningSource
	sinks      map[string]*runningSink
	connectors map[string]*runningConnector
	errored    map[string]Status // "kind/id" → error status
}

func New(st *store.Store, busMgr *bus.Manager, ds *sink.DataServer, log *slog.Logger, met *metrics.Set) *Supervisor {
	return &Supervisor{st: st, bus: busMgr, ds: ds, log: log, met: met,
		sources:    map[string]*runningSource{},
		sinks:      map[string]*runningSink{},
		connectors: map[string]*runningConnector{},
		errored:    map[string]Status{},
	}
}

func hashOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (s *Supervisor) Reconcile(ctx context.Context) error {
	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errored = map[string]Status{}

	desiredSources := map[string]model.Source{}
	for _, v := range cfg.Sources {
		if v.Enabled {
			desiredSources[v.ID] = v
		}
	}
	desiredSinks := map[string]model.Sink{}
	for _, v := range cfg.Sinks {
		if v.Enabled {
			desiredSinks[v.ID] = v
		}
	}
	desiredConnectors := map[string]model.Connector{}
	for _, v := range cfg.Connectors {
		if v.Enabled {
			desiredConnectors[v.ID] = v
		}
	}

	// --- Stop phase: connectors first, then sinks, then sources. ---
	for id, rc := range s.connectors {
		want, ok := desiredConnectors[id]
		if ok && hashOf(want) == rc.hash &&
			s.sources[want.SourceID] != nil && hashOf(desiredSources[want.SourceID]) == s.sources[want.SourceID].hash &&
			s.sinks[want.SinkID] != nil && hashOf(desiredSinks[want.SinkID]) == s.sinks[want.SinkID].hash {
			continue // unchanged, endpoints unchanged
		}
		s.log.Info("stopping connector", "id", id)
		rc.c.Stop()
		delete(s.connectors, id)
	}
	for id, rs := range s.sinks {
		if want, ok := desiredSinks[id]; ok && hashOf(want) == rs.hash {
			continue
		}
		s.log.Info("stopping sink", "id", id)
		rs.rt.Stop()
		delete(s.sinks, id)
	}
	for id, rs := range s.sources {
		if want, ok := desiredSources[id]; ok && hashOf(want) == rs.hash {
			continue
		}
		s.log.Info("stopping source", "id", id)
		rs.rt.Stop()
		delete(s.sources, id)
	}

	// --- Start phase: sources, sinks, connectors. ---
	for id, want := range desiredSources {
		if _, running := s.sources[id]; running {
			continue
		}
		rt, err := source.New(ctx, want, s.bus, s.log, s.met)
		if err != nil {
			s.fail("source", id, err)
			continue
		}
		s.log.Info("started source", "id", id)
		s.sources[id] = &runningSource{hash: hashOf(want), rt: rt}
	}
	for id, want := range desiredSinks {
		if _, running := s.sinks[id]; running {
			continue
		}
		rt, err := sink.New(ctx, want, s.bus, s.ds, s.log, s.met)
		if err != nil {
			s.fail("sink", id, err)
			continue
		}
		s.log.Info("started sink", "id", id)
		s.sinks[id] = &runningSink{hash: hashOf(want), rt: rt}
	}
	for id, want := range desiredConnectors {
		if _, running := s.connectors[id]; running {
			continue
		}
		src := s.sources[want.SourceID]
		snk := s.sinks[want.SinkID]
		if src == nil || snk == nil {
			s.fail("connector", id, errUnavailable(want, src == nil))
			continue
		}
		chain, err := filter.Compile(want.Filters)
		if err != nil {
			s.fail("connector", id, err)
			continue
		}
		q := queue.NewSQLite(s.st, id, want.Buffer)
		c := connector.New(want, src.rt, snk.rt, q, chain, s.log, s.met)
		c.Start(ctx)
		s.log.Info("started connector", "id", id)
		s.connectors[id] = &runningConnector{hash: hashOf(want), c: c}
	}
	return nil
}

func errUnavailable(c model.Connector, sourceMissing bool) error {
	if sourceMissing {
		return &unavailableError{kind: "source", id: c.SourceID}
	}
	return &unavailableError{kind: "sink", id: c.SinkID}
}

type unavailableError struct{ kind, id string }

func (e *unavailableError) Error() string {
	return e.kind + " " + e.id + " is not running"
}

func (s *Supervisor) fail(kind, id string, err error) {
	s.log.Error("component failed to start", "kind", kind, "id", id, "err", err)
	s.errored[kind+"/"+id] = Status{Kind: kind, ID: id, State: "error", Err: err.Error()}
	s.met.SetComponentState(kind, id, 0)
}

func (s *Supervisor) Statuses() []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Status
	for id, r := range s.sources {
		state, err := r.rt.State()
		st := Status{Kind: "source", ID: id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for id, r := range s.sinks {
		state, err := r.rt.State()
		st := Status{Kind: "sink", ID: id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for id := range s.connectors {
		out = append(out, Status{Kind: "connector", ID: id, State: "up"})
	}
	for _, st := range s.errored {
		out = append(out, st)
	}
	return out
}

func (s *Supervisor) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, rc := range s.connectors {
		rc.c.Stop()
		delete(s.connectors, id)
	}
	for id, rs := range s.sinks {
		rs.rt.Stop()
		delete(s.sinks, id)
	}
	for id, rs := range s.sources {
		rs.rt.Stop()
		delete(s.sources, id)
	}
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/supervisor/ -v -timeout 60s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/supervisor
git commit -m "feat: reconciling supervisor for hot config apply"
```

---

### Task 12: main wiring, seed, admin endpoints, end-to-end test

**Files:**
- Create: `internal/bus/busfake/busfake.go` (exported fake bus for tests), `internal/e2e/e2e_test.go`, `internal/app/app.go` (composition root usable from tests and main)
- Modify: `cmd/beacon/main.go`
- Test: `internal/e2e/e2e_test.go`

**Interfaces:**
- Produces:

```go
// internal/app
type Options struct {
    DBPath      string
    DataAddr    string   // e.g. "0.0.0.0:8080"
    AdminAddr   string   // e.g. "0.0.0.0:2112"
    SeedPath    string   // optional JSON config; applied only when store is empty
    Log         *slog.Logger
    ExtraN2KOpts []n2k.Option // test injection (fake bus, claim timeout)
}
type App struct{ ... }
func Run(ctx context.Context, opts Options) (*App, error) // starts everything
func (a *App) DataAddr() string   // bound data address
func (a *App) AdminAddr() string  // bound admin address
func (a *App) Reconcile(ctx context.Context) error // re-runs supervisor (Phase 2 API hook)
func (a *App) Close(ctx context.Context) error     // graceful shutdown
// internal/bus/busfake
type FakeBus struct{ ... }              // implements n2k.Bus
func New() *FakeBus
func (f *FakeBus) Inject(frame can.Frame)
func (f *FakeBus) Written() []can.Frame
```

Admin endpoints in Phase 1 (plain `net/http` mux, replaced by huma in Phase 2): `GET /metrics` (Prometheus), `GET /health` → `{"status":"ok"|"degraded","components":[Status...]}` (degraded when any component state ≠ "up"; still HTTP 200 unless every component errored — a boat gateway with one dead source is degraded, not down; use 503 only when the store is unreachable).

Seed behavior: `--seed config.json` loads the file, `Config.Validate()`, compiles every connector's filters, then `ReplaceConfig` — only when `store.IsEmpty()`. A non-empty store ignores the seed (log it).

- [ ] **Step 1: Extract fake bus into busfake**

`internal/bus/busfake/busfake.go` — move the `fakeBus` from `internal/bus/manager_test.go` here as an exported type (same code, exported methods `Inject`/`Written`, constructor `New()`), and update `manager_test.go` to use `busfake.New()`. Keep `vesselHeadingFrame()` in a shared spot: put it in busfake too as `busfake.VesselHeadingFrame()` since both bus and e2e tests need it.

Run: `go test ./internal/bus/ -v`
Expected: PASS (behavior unchanged).

- [ ] **Step 2: Write the failing e2e test**

`internal/e2e/e2e_test.go`:

```go
package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/app"
	"github.com/open-ships/beacon/internal/bus/busfake"
)

const seedJSON = `{
  "sources": [{"id": "can0", "name": "Main bus", "type": "socketcan", "enabled": true, "interface": "can0"}],
  "sinks": [{"id": "nav", "name": "Nav stream", "type": "http_sse", "enabled": true, "path": "/nav"}],
  "connectors": [{"id": "heading", "name": "Heading only", "source_id": "can0", "sink_id": "nav",
    "enabled": true, "filters": ["msg.pgn == 127250"], "buffer": {"max_messages": 1000}}]
}`

func startApp(t *testing.T, fake *busfake.FakeBus) *app.App {
	t.Helper()
	dir := t.TempDir()
	seed := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(seed, []byte(seedJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a, err := app.Run(ctx, app.Options{
		DBPath:    filepath.Join(dir, "beacon.db"),
		DataAddr:  "127.0.0.1:0",
		AdminAddr: "127.0.0.1:0",
		SeedPath:  seed,
		Log:       slog.Default(),
		ExtraN2KOpts: []n2k.Option{
			n2k.WithBus(fake), n2k.WithClaimTimeout(50 * time.Millisecond)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a
}

func sseEvents(t *testing.T, resp *http.Response, n int) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(resp.Body)
	done := time.After(5 * time.Second)
	lines := make(chan string, 64)
	go func() {
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	for len(out) < n {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed at %d/%d events", len(out), n)
			}
			if strings.HasPrefix(line, "data:") {
				var m map[string]any
				_ = json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &m)
				out = append(out, m)
			}
		case <-done:
			t.Fatalf("timeout at %d/%d events", len(out), n)
		}
	}
	return out
}

func TestEndToEndFilteredSSEWithReplay(t *testing.T) {
	fake := busfake.New()
	a := startApp(t, fake)

	resp, err := http.Get(fmt.Sprintf("http://%s/nav", a.DataAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	time.Sleep(300 * time.Millisecond) // client registered, bus client up
	fake.Inject(busfake.VesselHeadingFrame()) // PGN 127250 — passes filter
	fake.Inject(busfake.WaterDepthFrame())    // PGN 128267 — filtered out
	fake.Inject(busfake.VesselHeadingFrame())

	events := sseEvents(t, resp, 2)
	for _, e := range events {
		if e["pgn"].(float64) != 127250 {
			t.Fatalf("filter leaked pgn %v", e["pgn"])
		}
	}
	resp.Body.Close()

	// Replay: reconnect from before the second heading.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/nav", a.DataAddr()), nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("heading:%d", int64(events[0]["id"].(float64))))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	replayed := sseEvents(t, resp2, 1)
	if replayed[0]["pgn"].(float64) != 127250 {
		t.Fatalf("replayed pgn %v", replayed[0]["pgn"])
	}

	// Health endpoint reports components.
	hresp, err := http.Get(fmt.Sprintf("http://%s/health", a.AdminAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer hresp.Body.Close()
	var health struct {
		Status     string `json:"status"`
		Components []struct{ Kind, ID, State string } `json:"components"`
	}
	if err := json.NewDecoder(hresp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if len(health.Components) != 3 {
		t.Fatalf("components = %+v", health.Components)
	}

	// Metrics endpoint exposes connector counters.
	mresp, _ := http.Get(fmt.Sprintf("http://%s/metrics", a.AdminAddr()))
	defer mresp.Body.Close()
	var buf strings.Builder
	sc := bufio.NewScanner(mresp.Body)
	for sc.Scan() {
		buf.WriteString(sc.Text() + "\n")
	}
	if !strings.Contains(buf.String(), "beacon_connector_messages_total") {
		t.Fatal("metrics missing beacon_connector_messages_total")
	}
}
```

Add `busfake.WaterDepthFrame()` — PGN 128267 single-frame (priority 3, source 5, any valid payload; consult n2k's test fixtures for exact bytes the same way as VesselHeadingFrame).

- [ ] **Step 3: Run e2e, verify failure**

Run: `go test ./internal/e2e/ -v`
Expected: FAIL (undefined: app.Run).

- [ ] **Step 4: Implement internal/app/app.go**

```go
// Package app is beacon's composition root: it owns the store, bus
// manager, data server, supervisor, and admin HTTP server.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

type Options struct {
	DBPath       string
	DataAddr     string
	AdminAddr    string
	SeedPath     string
	Log          *slog.Logger
	ExtraN2KOpts []n2k.Option
}

type App struct {
	log      *slog.Logger
	st       *store.Store
	ds       *sink.DataServer
	sup      *supervisor.Supervisor
	adminSrv *http.Server
	adminLn  net.Listener
}

func Run(ctx context.Context, opts Options) (*App, error) {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	if opts.SeedPath != "" {
		if err := seed(ctx, st, opts.SeedPath, log); err != nil {
			_ = st.Close()
			return nil, err
		}
	}

	met, promHandler, err := metrics.New()
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init metrics: %w", err)
	}

	busMgr := bus.NewManager(log, met, opts.ExtraN2KOpts...)
	ds := sink.NewDataServer(opts.DataAddr, log)
	if err := ds.Start(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start data server: %w", err)
	}

	sup := supervisor.New(st, busMgr, ds, log, met)
	if err := sup.Reconcile(ctx); err != nil {
		_ = ds.Stop(ctx)
		_ = st.Close()
		return nil, fmt.Errorf("initial reconcile: %w", err)
	}

	a := &App{log: log, st: st, ds: ds, sup: sup}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promHandler)
	mux.HandleFunc("GET /health", a.handleHealth)
	a.adminSrv = &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", opts.AdminAddr)
	if err != nil {
		_ = a.Close(ctx)
		return nil, fmt.Errorf("bind admin server: %w", err)
	}
	a.adminLn = ln
	go func() {
		if err := a.adminSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server error", "err", err)
		}
	}()

	log.Info("beacon ready", "data", a.DataAddr(), "admin", a.AdminAddr())
	return a, nil
}

func seed(ctx context.Context, st *store.Store, path string, log *slog.Logger) error {
	empty, err := st.IsEmpty(ctx)
	if err != nil {
		return err
	}
	if !empty {
		log.Info("store not empty; ignoring seed file", "seed", path)
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read seed: %w", err)
	}
	var cfg model.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse seed: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("seed config invalid: %w", err)
	}
	for _, c := range cfg.Connectors {
		if _, err := filter.Compile(c.Filters); err != nil {
			return fmt.Errorf("seed connector %q: %w", c.ID, err)
		}
	}
	log.Info("seeding configuration", "seed", path,
		"sources", len(cfg.Sources), "sinks", len(cfg.Sinks), "connectors", len(cfg.Connectors))
	return st.ReplaceConfig(ctx, cfg)
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	statuses := a.sup.Statuses()
	status := "ok"
	for _, s := range statuses {
		if s.State != "up" {
			status = "degraded"
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": status, "components": statuses,
	})
}

func (a *App) DataAddr() string { return a.ds.Addr() }
func (a *App) AdminAddr() string {
	if a.adminLn == nil {
		return ""
	}
	return a.adminLn.Addr().String()
}

// Reconcile re-converges running components with stored config (Phase 2 API hook).
func (a *App) Reconcile(ctx context.Context) error { return a.sup.Reconcile(ctx) }

func (a *App) Close(ctx context.Context) error {
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	a.sup.Stop()          // connectors flush final checkpoints
	_ = a.ds.Stop(ctx)
	return a.st.Close()
}
```

- [ ] **Step 5: Rewrite cmd/beacon/main.go**

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/open-ships/beacon/internal/app"
)

var (
	version   = "dev"
	dbPath    string
	dataAddr  string
	adminAddr string
	seedPath  string
	logLevel  string
)

func main() {
	root := &cobra.Command{
		Use:     "beacon",
		Short:   "NMEA 2000 gateway: sources, sinks, connectors",
		Version: version,
		RunE:    run,
	}
	root.Flags().StringVar(&dbPath, "db", "beacon.db", "SQLite database path (config + buffers)")
	root.Flags().StringVar(&dataAddr, "data-address", "0.0.0.0:8080", "data server bind address (sink endpoints)")
	root.Flags().StringVar(&adminAddr, "admin-address", "0.0.0.0:2112", "admin server bind address (/health, /metrics)")
	root.Flags().StringVar(&seedPath, "seed", "", "JSON config to seed an empty database")
	root.Flags().StringVar(&logLevel, "log-level", "info", "debug | info | warn | error")
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	log := buildLogger(logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.Run(ctx, app.Options{
		DBPath: dbPath, DataAddr: dataAddr, AdminAddr: adminAddr,
		SeedPath: seedPath, Log: log,
	})
	if err != nil {
		return err
	}

	<-ctx.Done()
	log.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Close(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
	}
	log.Info("shutdown complete")
	return nil
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
```

(Add the missing `"time"` import.)

- [ ] **Step 6: Run everything**

Run: `go build ./... && go test ./... -timeout 300s && go test -race ./... -timeout 600s && go vet ./...`
Expected: all PASS, vet clean.

- [ ] **Step 7: Update justfile if stale, commit**

Check `justfile` targets still work (`just build`, `just test`). Fix any that referenced deleted packages.

```bash
git add -A
git commit -m "feat: composition root, seed config, admin endpoints, end-to-end coverage"
```

---

## Plan Self-Review Notes

- **Spec coverage (Phase 1 scope):** envelope §2.1 → Task 2; sources §2.2 → Task 8; sinks §2.3 → Task 9; connectors §2.4 + limits → Tasks 4, 10; CEL §2.5 → Task 5; bus manager §3.1 → Task 7; hub §3.2 → Task 8; pipeline §3.3 → Task 10; queue interface §3.4 → Task 4; delivery semantics §3.5 → Tasks 9, 10; reconciler §3.6 → Task 11; CAN write path §3.7 → Tasks 2, 7; persistence §4 → Task 3; seed §4 → Task 12; metrics §7 → Task 6; error handling §8 → Tasks 7, 8, 10, 11; testing §9 → every task + Task 12 e2e. Export/import API, OpenAPI, UI, /docs are Phases 2–4 by design.
- **Known deferred items (intentional, Phase 2+):** `beacon export`/`import` CLI verbs (API phase), rolling-rate calculator for UI tiles (UI phase), `/api/v1/system` interface enumeration (API phase).
- **Type consistency check:** `queue.Queue.Read(ctx, after int64, limit int) ([]Entry, error)` == `sink.ReplayReader.Read` signature — connectors pass the queue directly to `RegisterConnector`. `source.Runtime.State()` and `sink.Runtime.State()` both return `(string, error)`. Envelope JSON field names match between Task 2 tests, SSE/WS wire assertions (Task 9), and the SSE dialer (Task 8).
- **Frame fixtures:** the exact CAN ID/EFF encoding for `busfake.VesselHeadingFrame` must be validated against n2k's own test fixtures at implementation time (noted inline in Tasks 7 and 12); the tests are written to fail loudly if the fixture is wrong.




