package filter

import (
	"log/slog"

	"github.com/open-ships/beacon/internal/admin"
)

// Parse compiles a slice of CEL expression strings into a Chain.
// Any compilation failure returns an error immediately (fail-fast at startup).
func Parse(exprs []string, log *slog.Logger, metrics *admin.Metrics, sinkName string) (Chain, error) {
	chain := make(Chain, 0, len(exprs))
	var opts []CELOption
	if metrics != nil {
		opts = append(opts, WithFilterMetrics(metrics, sinkName))
	}
	for _, expr := range exprs {
		f, err := NewCELFilter(expr, log, opts...)
		if err != nil {
			return nil, err
		}
		chain = append(chain, f)
	}
	return chain, nil
}
