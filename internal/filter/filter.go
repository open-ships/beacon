// Package filter implements the CEL-based filter engine for N2K messages.
package filter

import (
	"github.com/open-ships/beacon/internal/n2k"
)

// Filter decides whether a ParsedMessage should be passed to a sink.
type Filter interface {
	Match(msg *n2k.ParsedMessage) bool
}

// Chain is an ordered slice of filters. All filters must match (AND semantics).
type Chain []Filter

// Match returns true only if all filters in the chain match.
// An empty chain matches all messages.
func (c Chain) Match(msg *n2k.ParsedMessage) bool {
	for _, f := range c {
		if !f.Match(msg) {
			return false
		}
	}
	return true
}
