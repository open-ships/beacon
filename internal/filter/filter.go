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
	env, err := newEnvironment()
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

func newEnvironment() (*cel.Env, error) {
	return cel.NewEnv(cel.Variable("msg", cel.MapType(cel.StringType, cel.DynType)))
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
