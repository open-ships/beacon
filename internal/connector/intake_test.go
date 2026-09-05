package connector

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/stats"
)

type intakeQueue struct {
	queue.Queue
	appendFn func(context.Context, []*msg.Envelope) (queue.PruneResult, error)
}

func (q *intakeQueue) Append(ctx context.Context, batch []*msg.Envelope) (queue.PruneResult, error) {
	return q.appendFn(ctx, batch)
}

func TestIntakeUnsubscribesBeforeFlushingCanceledBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan *msg.Envelope, readLimit)
	for range cap(in) {
		in <- env(127250)
	}
	unsubscribed, unsubCalls, calls, persisted := false, 0, 0, 0
	q := &intakeQueue{appendFn: func(appendCtx context.Context, batch []*msg.Envelope) (queue.PruneResult, error) {
		calls++
		if len(batch) > batchSize {
			t.Fatalf("batch grew to %d", len(batch))
		}
		if calls == 1 {
			// Stop during a normal full-batch append while the source refills.
			for len(in) < cap(in) {
				in <- env(127250)
			}
			cancel()
			return queue.PruneResult{}, appendCtx.Err()
		}
		if !unsubscribed {
			t.Fatal("shutdown append ran with live subscription")
		}
		if appendCtx.Err() != nil {
			t.Fatal("shutdown reused canceled context")
		}
		if _, ok := appendCtx.Deadline(); !ok {
			t.Fatal("shutdown append has no deadline")
		}
		persisted += len(batch)
		return queue.PruneResult{}, nil
	}}
	chain, err := filter.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "drain"}, nil, nil, q, chain, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, reg)
	c.wg.Add(1)
	c.intake(ctx, in, func() { unsubscribed = true; unsubCalls++; close(in) })
	if persisted != batchSize+readLimit || unsubCalls != 1 {
		t.Fatalf("persisted %d, unsubscribed %d times", persisted, unsubCalls)
	}
	snapshot, _ := reg.Snapshot("drain")
	if snapshot.StageTotals["received"] != int64(persisted) || snapshot.StageTotals["intake_loss"] != 0 {
		t.Fatalf("stages = %v", snapshot.StageTotals)
	}
}

func TestIntakeFailedShutdownBoundsBatchAndReportsAllLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := make(chan *msg.Envelope, batchSize*2+3)
	for range cap(in) {
		in <- env(127250)
	}
	var deadline time.Time
	q := &intakeQueue{appendFn: func(appendCtx context.Context, batch []*msg.Envelope) (queue.PruneResult, error) {
		if len(batch) > batchSize {
			t.Fatalf("batch grew to %d", len(batch))
		}
		var ok bool
		deadline, ok = appendCtx.Deadline()
		if !ok {
			t.Fatal("shutdown has no deadline")
		}
		<-appendCtx.Done()
		return queue.PruneResult{}, appendCtx.Err()
	}}
	chain, err := filter.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "failed-drain"}, nil, nil, q, chain, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, reg)
	c.wg.Add(1)
	start := time.Now()
	c.intake(ctx, in, func() { close(in) })
	if deadline.Sub(start) > appendTimeout+time.Second || time.Since(start) > appendTimeout+2*time.Second {
		t.Fatal("shutdown exceeded its bounded window")
	}
	snapshot, _ := reg.Snapshot("failed-drain")
	if snapshot.StageTotals["intake_loss"] != int64(cap(in)) {
		t.Fatalf("lost %d, want %d", snapshot.StageTotals["intake_loss"], cap(in))
	}
}
