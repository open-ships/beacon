package ui

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

type controlledStreamWriter struct {
	header      http.Header
	deadline    time.Time
	flushes     int
	flushErr    error
	deadlineErr error
}

func (w *controlledStreamWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *controlledStreamWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *controlledStreamWriter) WriteHeader(int)             {}
func (w *controlledStreamWriter) FlushError() error {
	w.flushes++
	return w.flushErr
}
func (w *controlledStreamWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return w.deadlineErr
}

func TestWriteStreamChunkSetsDeadlineAndReturnsFlushError(t *testing.T) {
	flushErr := errors.New("peer stopped reading")
	w := &controlledStreamWriter{flushErr: flushErr}
	controller := http.NewResponseController(w)
	started := time.Now()
	wrote := false
	err := writeStreamChunk(controller, func() error {
		wrote = true
		_, err := w.Write([]byte("data: test\n\n"))
		return err
	})
	if !errors.Is(err, flushErr) {
		t.Fatalf("writeStreamChunk error = %v, want flush error %v", err, flushErr)
	}
	if !wrote || w.flushes != 1 {
		t.Fatalf("wrote=%v flushes=%d, want one write and flush", wrote, w.flushes)
	}
	if w.deadline.Before(started) || w.deadline.After(started.Add(streamClientWriteLimit+time.Second)) {
		t.Fatalf("write deadline = %s, want a fresh deadline near %s", w.deadline, started.Add(streamClientWriteLimit))
	}
}

func TestWriteStreamChunkStopsBeforeWriteWhenDeadlineCannotBeSet(t *testing.T) {
	deadlineErr := errors.New("deadlines unsupported")
	w := &controlledStreamWriter{deadlineErr: deadlineErr}
	wrote := false
	err := writeStreamChunk(http.NewResponseController(w), func() error {
		wrote = true
		return nil
	})
	if !errors.Is(err, deadlineErr) || wrote || w.flushes != 0 {
		t.Fatalf("error=%v wrote=%v flushes=%d, want deadline error before write", err, wrote, w.flushes)
	}
}
