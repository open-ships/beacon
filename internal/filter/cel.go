package filter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/n2k"
)

// CELFilter evaluates a CEL expression against a ParsedMessage.
type CELFilter struct {
	expr    string
	prg     cel.Program
	log     *slog.Logger
	metrics *admin.Metrics
	sink    string // sink name for metrics labels
}

var celEnv *cel.Env

func init() {
	var err error
	celEnv, err = cel.NewEnv(
		cel.Variable("msg", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		panic(fmt.Sprintf("CEL env init: %v", err))
	}
}

// NewCELFilter compiles the given CEL expression. Returns an error if the expression is invalid.
func NewCELFilter(expr string, log *slog.Logger, opts ...CELOption) (*CELFilter, error) {
	ast, issues := celEnv.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile CEL expression %q: %w", expr, issues.Err())
	}
	prg, err := celEnv.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("create CEL program %q: %w", expr, err)
	}
	f := &CELFilter{expr: expr, prg: prg, log: log}
	for _, opt := range opts {
		opt(f)
	}
	return f, nil
}

// CELOption is a functional option for CELFilter.
type CELOption func(*CELFilter)

// WithFilterMetrics sets the metrics and sink name for filter metric tracking.
func WithFilterMetrics(m *admin.Metrics, sink string) CELOption {
	return func(f *CELFilter) {
		f.metrics = m
		f.sink = sink
	}
}

// Match evaluates the compiled CEL expression against msg.
// Evaluation errors return false and are logged at debug level.
func (f *CELFilter) Match(msg *n2k.ParsedMessage) bool {
	msgMap := buildMsgMap(msg)
	out, _, err := f.prg.Eval(map[string]any{"msg": msgMap})
	if err != nil {
		if f.log != nil {
			f.log.Debug("CEL eval error", "expr", f.expr, "err", err)
		}
		if f.metrics != nil {
			f.metrics.FilterErrorsTotal.Add(context.Background(), 1, admin.AttrSink(f.sink))
		}
		return false
	}
	result, ok := out.Value().(bool)
	return ok && result
}

// buildMsgMap converts a ParsedMessage into the map passed to CEL.
func buildMsgMap(msg *n2k.ParsedMessage) map[string]any {
	var payloadMap map[string]any
	if len(msg.Payload) > 0 && string(msg.Payload) != "null" {
		_ = json.Unmarshal(msg.Payload, &payloadMap)
	}
	if payloadMap == nil {
		payloadMap = make(map[string]any)
	}

	ts := ""
	if !msg.Timestamp.IsZero() {
		ts = msg.Timestamp.Format(time.RFC3339)
	}

	return map[string]any{
		"pgn":       int64(msg.PGN),
		"source":    int64(msg.Source),
		"dest":      int64(msg.Dest),
		"priority":  int64(msg.Priority),
		"timestamp": ts,
		"payload":   payloadMap,
	}
}
