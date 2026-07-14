package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/open-ships/beacon/internal/model"
)

// TestExamplesValidate walks examples/*.json (the importable configs README.md
// and examples/README.md point users at) and runs each one through the exact
// same ValidateConfig every API write, CLI import, and --seed boot uses. An
// example that fails to parse or validate here would fail identically for a
// user running `beacon import examples/whatever.json` or `--seed`, so this
// test is what keeps the examples directory honest as model.Config's shape
// evolves.
//
// The examples directory lives at the repo root, two levels above this
// package (internal/config), not under it — go:embed cannot reach outside
// its module subtree in a useful way here since examples/ is meant to be
// copied/edited by users, not compiled in. runtime.Caller(0) resolves this
// file's own path so the walk works regardless of the directory `go test`
// happens to be invoked from.
func TestExamplesValidate(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	examplesDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "examples")

	entries, err := os.ReadDir(examplesDir)
	if err != nil {
		t.Fatalf("read %s: %v", examplesDir, err)
	}

	var jsonFiles []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		jsonFiles = append(jsonFiles, e.Name())
	}
	if len(jsonFiles) == 0 {
		t.Fatalf("no *.json files found in %s", examplesDir)
	}

	for _, name := range jsonFiles {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(examplesDir, name)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}

			var cfg model.Config
			if err := json.Unmarshal(raw, &cfg); err != nil {
				t.Fatalf("unmarshal %s: %v", path, err)
			}

			if err := ValidateConfig(cfg); err != nil {
				t.Fatalf("%s failed ValidateConfig: %v", path, err)
			}

			if len(cfg.Sources) == 0 && len(cfg.Sinks) == 0 && len(cfg.Connectors) == 0 {
				t.Fatalf("%s parsed to an empty config; likely a shape mismatch silently dropping every field", path)
			}
		})
	}
}
