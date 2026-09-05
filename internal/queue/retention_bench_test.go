package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

// Exercise a saturated queue, where every append must prune. Setup creates
// realistic retained rows and is excluded from timing. The hot path should
// scale with the excess being removed, not the retained history length.
func BenchmarkAppendAtCountLimit(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000, 300000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			st, err := store.Open(filepath.Join(b.TempDir(), "queue.db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = st.Close() })
			e := env(127250, time.Now())
			wire, err := e.WireBytes()
			if err != nil {
				b.Fatal(err)
			}
			_, err = st.DB().Exec(`WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM x WHERE i < ?)
				INSERT INTO queue(connector_id,ts,envelope,bytes) SELECT 'bench',?,?,? FROM x`, count, time.Now().UnixNano(), string(wire), len(wire))
			if err != nil {
				b.Fatal(err)
			}
			q := NewSQLite(st, "bench", model.BufferLimits{MaxMessages: int64(count), MaxBytes: 512 << 20})
			if _, err := q.Stats(context.Background()); err != nil {
				b.Fatal(err)
			}
			batch := []*msg.Envelope{e}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pruned, err := q.Append(context.Background(), batch)
				if err != nil {
					b.Fatal(err)
				}
				if pruned.Total != 1 {
					b.Fatalf("pruned = %+v", pruned)
				}
			}
		})
	}
}
