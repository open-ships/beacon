package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/store"
)

func seedStore(t *testing.T, dbPath string, cfg model.Config) {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.ReplaceConfig(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
}

func sampleConfig() model.Config {
	return model.Config{
		Sources: []model.Source{
			{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"},
		},
		Sinks: []model.Sink{
			{ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a"},
		},
		Connectors: []model.Connector{
			{ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true, Filters: []string{"msg.pgn == 127250"}},
		},
	}
}

func TestRunExport(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	seedStore(t, dbPath, sampleConfig())

	var buf bytes.Buffer
	if err := runExport(dbPath, &buf); err != nil {
		t.Fatalf("runExport: %v", err)
	}

	var got model.Config
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode export output: %v; output: %s", err, buf.String())
	}
	if len(got.Sources) != 1 || got.Sources[0].ID != "s1" {
		t.Fatalf("exported sources = %+v", got.Sources)
	}
	if len(got.Sinks) != 1 || got.Sinks[0].ID != "k1" {
		t.Fatalf("exported sinks = %+v", got.Sinks)
	}
	if len(got.Connectors) != 1 || got.Connectors[0].ID != "c1" {
		t.Fatalf("exported connectors = %+v", got.Connectors)
	}
}

func TestRunExportOpenErrorOnUnwritableDir(t *testing.T) {
	// A db path inside a directory that does not exist and cannot be
	// created (parent is a file, not a dir) should surface an error rather
	// than panicking.
	parent := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(parent, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(parent, "beacon.db")

	var buf bytes.Buffer
	if err := runExport(dbPath, &buf); err == nil {
		t.Fatal("runExport with unopenable db path: want error, got nil")
	}
}

func TestRunImportReplace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	seedStore(t, dbPath, sampleConfig())

	// Replace with a config that drops the connector and adds a new sink.
	repl := model.Config{
		Sources: []model.Source{
			{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"},
		},
		Sinks: []model.Sink{
			{ID: "k2", Name: "K2", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:9000"},
		},
	}
	raw, err := json.Marshal(repl)
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(t.TempDir(), "import.json")
	if err := os.WriteFile(filePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runImport(dbPath, filePath, false, &buf); err != nil {
		t.Fatalf("runImport: %v", err)
	}
	if !strings.Contains(buf.String(), "1 sources") && !strings.Contains(buf.String(), "sources") {
		t.Fatalf("runImport summary output = %q, want it to mention sources", buf.String())
	}

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cfg, err := st.LoadConfig(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Connectors) != 0 {
		t.Fatalf("connectors after replace = %+v, want none", cfg.Connectors)
	}
	if len(cfg.Sinks) != 1 || cfg.Sinks[0].ID != "k2" {
		t.Fatalf("sinks after replace = %+v, want only k2", cfg.Sinks)
	}
}

func TestRunImportMerge(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	seedStore(t, dbPath, sampleConfig())

	partial := model.Config{
		Sources: []model.Source{
			{ID: "s2", Name: "S2", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"},
		},
	}
	raw, err := json.Marshal(partial)
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(t.TempDir(), "import.json")
	if err := os.WriteFile(filePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runImport(dbPath, filePath, true, &buf); err != nil {
		t.Fatalf("runImport merge: %v", err)
	}

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cfg, err := st.LoadConfig(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sources) != 2 {
		t.Fatalf("sources after merge = %+v, want 2", cfg.Sources)
	}
	if len(cfg.Connectors) != 1 {
		t.Fatalf("connectors after merge = %+v, want untouched 1", cfg.Connectors)
	}
}

func TestRunImportInvalidLeavesStoreUnchanged(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	seedStore(t, dbPath, sampleConfig())

	bad := model.Config{
		Sources: []model.Source{
			{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"},
		},
		Sinks: []model.Sink{
			{ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a"},
		},
		Connectors: []model.Connector{
			{ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true, Filters: []string{"msg.pgn =="}},
		},
	}
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(filePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runImport(dbPath, filePath, false, &buf); err == nil {
		t.Fatal("runImport with invalid CEL filter: want error, got nil")
	}

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cfg, err := st.LoadConfig(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Connectors) != 1 || cfg.Connectors[0].Filters[0] != "msg.pgn == 127250" {
		t.Fatalf("config after invalid import = %+v, want unchanged", cfg)
	}
}

func TestRunImportMissingFile(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	var buf bytes.Buffer
	err := runImport(dbPath, filepath.Join(t.TempDir(), "nope.json"), false, &buf)
	if err == nil {
		t.Fatal("runImport with missing file: want error, got nil")
	}
}

func TestRunImportInvalidJSON(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	filePath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(filePath, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := runImport(dbPath, filePath, false, &buf); err == nil {
		t.Fatal("runImport with malformed JSON: want error, got nil")
	}
}
