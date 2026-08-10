// Package config is the single validation+persist+reconcile choke point for
// beacon's configuration entities (sources, sinks, connectors). The HTTP
// config API and, later, the Phase 3 UI both sit on Service rather than
// touching store.Store directly, so every write goes through the same
// whole-config structural validation and CEL filter compile check before
// anything is persisted.
package config

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

// reconcileApplyTimeout bounds the synchronous fast path after a durable
// config write. The Supervisor owns continuous convergence, so timing out a
// request-side apply does not abandon desired state.
var reconcileApplyTimeout = 5 * time.Second

// Sentinel errors returned by Service methods. Callers use errors.Is; the
// HTTP layer (Phase 2 Task 4) maps them to specific status codes.
var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already exists")
	ErrInUse    = errors.New("referenced by a connector")
)

// ValidationError reports a structural or CEL-compile problem with a
// would-be configuration — as opposed to a store/IO failure. The HTTP layer
// maps a *ValidationError to 422 and anything else to 500.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// ValidateConfig checks structural rules (model.Config.Validate: ids,
// types, references, sink path collisions) and then CEL-compiles every
// connector's filters. It is exported standalone so the CLI's import
// command (Phase 4) can validate a file before ever constructing a Service;
// Service.Import and every write path funnel through it too.
func ValidateConfig(cfg model.Config) error {
	if err := cfg.Validate(); err != nil {
		return &ValidationError{Msg: err.Error()}
	}
	for _, c := range cfg.Connectors {
		if _, err := filter.Compile(c.Filters); err != nil {
			return &ValidationError{Msg: err.Error()}
		}
	}
	return nil
}

// Reconciler is what Service triggers after committing a config change.
// *supervisor.Supervisor satisfies it; tests use a fake.
type Reconciler interface {
	Reconcile(ctx context.Context) error
	Statuses() []supervisor.Status
}

// Service is beacon's config service layer. Every write is serialized by mu
// (a read-modify-write against the store must not interleave with another
// writer), validates the WOULD-BE whole config before touching the store,
// and — on a successful store write — triggers a supervisor reconcile. A
// reconcile failure is logged and left visible via Statuses(); it does not
// roll back the store write and is not returned to the write's caller
// (spec §3.6: visible, not fatal).
type Service struct {
	st  *store.Store
	rec Reconciler
	log *slog.Logger

	// mu serializes every write method so a read-modify-write against the
	// store (load current config, validate the mutated copy, persist) never
	// interleaves with a concurrent writer racing a stale read.
	mu sync.Mutex

	// reconcileGate permits at most one detached reconcile call. If an
	// implementation ignores its context and wedges, later writes remain
	// bounded without accumulating one stuck goroutine per request.
	reconcileGate chan struct{}
}

// NewService builds a Service over st, triggering rec after every
// successful write. log defaults to slog.Default() when nil.
func NewService(st *store.Store, rec Reconciler, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{st: st, rec: rec, log: log, reconcileGate: make(chan struct{}, 1)}
}

// --- Reads ---

func (s *Service) ListSources(ctx context.Context) ([]model.Source, error) {
	return s.st.ListSources(ctx)
}

func (s *Service) ListSinks(ctx context.Context) ([]model.Sink, error) {
	return s.st.ListSinks(ctx)
}

func (s *Service) ListConnectors(ctx context.Context) ([]model.Connector, error) {
	return s.st.ListConnectors(ctx)
}

func (s *Service) GetSource(ctx context.Context, id string) (model.Source, error) {
	out, err := s.st.GetSource(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Source{}, ErrNotFound
	}
	return out, err
}

func (s *Service) GetSink(ctx context.Context, id string) (model.Sink, error) {
	out, err := s.st.GetSink(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Sink{}, ErrNotFound
	}
	return out, err
}

func (s *Service) GetConnector(ctx context.Context, id string) (model.Connector, error) {
	out, err := s.st.GetConnector(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Connector{}, ErrNotFound
	}
	return out, err
}

// Export returns the whole current configuration (used by the CLI/UI export
// and as the base for a merge Import).
func (s *Service) Export(ctx context.Context) (model.Config, error) {
	return s.st.LoadConfig(ctx)
}

// Statuses passes through the reconciler's live component statuses (for
// /api/status and the UI dashboard).
func (s *Service) Statuses() []supervisor.Status {
	return s.rec.Statuses()
}

// ValidateFilters CEL-compiles exprs without persisting anything, so the
// UI/CLI can check filter syntax before submitting a connector write.
func (s *Service) ValidateFilters(exprs []string) error {
	if _, err := filter.Compile(exprs); err != nil {
		return &ValidationError{Msg: err.Error()}
	}
	return nil
}

// --- Writes ---

// PutSource creates or updates a source. isCreate=true requires v.ID to not
// already exist (ErrExists otherwise); isCreate=false requires it to
// already exist (ErrNotFound otherwise).
func (s *Service) PutSource(ctx context.Context, v model.Source, isCreate bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	id := func(x model.Source) string { return x.ID }
	i := indexOf(cfg.Sources, v.ID, id)
	if isCreate && i >= 0 {
		return ErrExists
	}
	if !isCreate && i < 0 {
		return ErrNotFound
	}
	if i >= 0 {
		cfg.Sources[i] = v
	} else {
		cfg.Sources = append(cfg.Sources, v)
	}
	if err := ValidateConfig(cfg); err != nil {
		return err
	}
	if err := s.st.PutSource(ctx, v); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// PutSink creates or updates a sink, following the same isCreate semantics
// as PutSource.
func (s *Service) PutSink(ctx context.Context, v model.Sink, isCreate bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	id := func(x model.Sink) string { return x.ID }
	i := indexOf(cfg.Sinks, v.ID, id)
	if isCreate && i >= 0 {
		return ErrExists
	}
	if !isCreate && i < 0 {
		return ErrNotFound
	}
	if i >= 0 {
		cfg.Sinks[i] = v
	} else {
		cfg.Sinks = append(cfg.Sinks, v)
	}
	if err := ValidateConfig(cfg); err != nil {
		return err
	}
	if err := s.st.PutSink(ctx, v); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// PutConnector creates or updates a connector, following the same isCreate
// semantics as PutSource.
func (s *Service) PutConnector(ctx context.Context, v model.Connector, isCreate bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	id := func(x model.Connector) string { return x.ID }
	i := indexOf(cfg.Connectors, v.ID, id)
	if isCreate && i >= 0 {
		return ErrExists
	}
	if !isCreate && i < 0 {
		return ErrNotFound
	}
	if i >= 0 {
		cfg.Connectors[i] = v
	} else {
		cfg.Connectors = append(cfg.Connectors, v)
	}
	if err := ValidateConfig(cfg); err != nil {
		return err
	}
	if err := s.st.PutConnector(ctx, v); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// DeleteSource removes a source. ErrNotFound if it doesn't exist; ErrInUse
// if any connector (enabled or not) still references it as its source.
func (s *Service) DeleteSource(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	i := indexOf(cfg.Sources, id, func(v model.Source) string { return v.ID })
	if i < 0 {
		return ErrNotFound
	}
	for _, c := range cfg.Connectors {
		if c.SourceID == id {
			return ErrInUse
		}
	}
	if err := s.st.DeleteSource(ctx, id); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// DeleteSink removes a sink. ErrNotFound if it doesn't exist; ErrInUse if
// any connector (enabled or not) still references it as its sink.
func (s *Service) DeleteSink(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	i := indexOf(cfg.Sinks, id, func(v model.Sink) string { return v.ID })
	if i < 0 {
		return ErrNotFound
	}
	for _, c := range cfg.Connectors {
		if c.SinkID == id {
			return ErrInUse
		}
	}
	if err := s.st.DeleteSink(ctx, id); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// DeleteConnector removes a connector. ErrNotFound if it doesn't exist.
// Nothing else references a connector, so there is no ErrInUse case.
func (s *Service) DeleteConnector(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	i := indexOf(cfg.Connectors, id, func(v model.Connector) string { return v.ID })
	if i < 0 {
		return ErrNotFound
	}
	if err := s.st.DeleteConnector(ctx, id); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// Import replaces or merges cfg into the stored configuration.
//
// replace=true: cfg becomes the whole configuration wholesale (validated
// standalone, then a transactional ReplaceConfig).
//
// replace=false (merge): cfg's entities are upserted by id into the
// currently loaded configuration — entities not mentioned in cfg are left
// untouched — and the merged result is validated and persisted via
// ReplaceConfig.
//
// Either way, validation runs against the full would-be result before any
// store write, so an invalid import leaves the store untouched.
func (s *Service) Import(ctx context.Context, cfg model.Config, replace bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := cfg
	if !replace {
		current, err := s.st.LoadConfig(ctx)
		if err != nil {
			return err
		}
		result = MergeConfig(current, cfg)
	}
	if err := ValidateConfig(result); err != nil {
		return err
	}
	if err := s.st.ReplaceConfig(ctx, result); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// MergeConfig upserts incoming's entities by id onto base, leaving any
// base entity not mentioned in incoming untouched. It is exported standalone
// (like ValidateConfig) so the CLI's import --merge command (Phase 4) can
// build the same merged result Service.Import would, before validating and
// writing it directly to the store.
func MergeConfig(base, incoming model.Config) model.Config {
	settings := cloneSettings(base.Settings)
	if incoming.Settings != nil {
		if settings == nil {
			settings = &model.Settings{}
		}
		// Settings sections are independent merge units. An observability-only
		// import must not silently reset the appliance's physical storage budget,
		// and a resource-only import must preserve the metrics-cardinality gate.
		if incoming.Settings.Observability != nil {
			observability := *incoming.Settings.Observability
			settings.Observability = &observability
		}
		if incoming.Settings.Resources != nil {
			resources := *incoming.Settings.Resources
			settings.Resources = &resources
		}
	}
	return model.Config{
		Sources:    upsert(base.Sources, incoming.Sources, func(v model.Source) string { return v.ID }),
		Sinks:      upsert(base.Sinks, incoming.Sinks, func(v model.Sink) string { return v.ID }),
		Connectors: upsert(base.Connectors, incoming.Connectors, func(v model.Connector) string { return v.ID }),
		Settings:   settings,
	}
}

func cloneSettings(in *model.Settings) *model.Settings {
	if in == nil {
		return nil
	}
	out := *in
	if in.Observability != nil {
		observability := *in.Observability
		out.Observability = &observability
	}
	if in.Resources != nil {
		resources := *in.Resources
		out.Resources = &resources
	}
	return &out
}

// upsert returns a copy of base with each incoming element replacing the
// base element sharing its id, or appended when no such element exists.
func upsert[T any](base, incoming []T, id func(T) string) []T {
	out := append([]T(nil), base...)
	for _, v := range incoming {
		if i := indexOf(out, id(v), id); i >= 0 {
			out[i] = v
		} else {
			out = append(out, v)
		}
	}
	return out
}

// indexOf returns the index of the element of items whose id matches
// target, or -1 if none matches.
func indexOf[T any](items []T, target string, id func(T) string) int {
	for i, v := range items {
		if id(v) == target {
			return i
		}
	}
	return -1
}

// reconcile triggers the reconciler after a successful store write. Failure
// is logged and left for Statuses() to surface; per spec §3.6 it is
// deliberately NOT returned to the write's caller and NOT rolled back.
//
// It detaches from inbound request cancellation because the desired state is
// already durable, then adds its own deadline. The Supervisor's independent
// convergence loop picks up any work that outlives this synchronous fast
// path. The gate bounds even a broken Reconciler that ignores cancellation to
// one outstanding goroutine rather than one per config request.
func (s *Service) reconcile(ctx context.Context) {
	applyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileApplyTimeout)
	defer cancel()
	select {
	case s.reconcileGate <- struct{}{}:
	case <-applyCtx.Done():
		s.log.Warn("reconcile after config write deferred to convergence loop", "err", applyCtx.Err())
		return
	}

	done := make(chan error, 1)
	go func() {
		defer func() { <-s.reconcileGate }()
		done <- s.rec.Reconcile(applyCtx)
	}()
	select {
	case err := <-done:
		if err != nil {
			s.log.Error("reconcile after config write failed", "err", err)
		}
	case <-applyCtx.Done():
		s.log.Warn("reconcile after config write exceeded request apply budget", "err", applyCtx.Err())
	}
}
