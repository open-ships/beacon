package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/open-ships/beacon/internal/model"
)

const (
	crashHelperEnv      = "BEACON_STORE_CRASH_HELPER"
	crashHelperPathEnv  = "BEACON_STORE_CRASH_PATH"
	crashHelperExitCode = 23
)

func assertSQLiteIntegrity(t *testing.T, s *Store) {
	t.Helper()
	var result string
	if err := s.DB().QueryRowContext(context.Background(), `PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if result != "ok" {
		t.Fatalf("integrity_check = %q, want ok", result)
	}
}

func TestSQLiteFullLeavesStoreReadableAndIntact(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "full.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	baseline := model.Source{ID: "baseline", Name: "committed before SQLITE_FULL", Type: model.SourceFile, FilePath: "/capture.ndjson"}
	if err := s.PutSource(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `CREATE TABLE full_probe (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	before, err := s.StorageStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately tiny test-only headroom reaches SQLite's real page ceiling
	// quickly. Production configuration validation enforces the much larger
	// model.MinDatabaseBytes floor.
	budget := (before.PageCount + 8) * before.PageSize
	if err := s.ConfigureResources(ctx, model.ResourceConfig{MaxDatabaseBytes: budget}); err != nil {
		t.Fatal(err)
	}

	var fullErr error
	for i := 0; i < 256; i++ {
		_, err := s.DB().ExecContext(ctx,
			`INSERT INTO full_probe(payload) VALUES (zeroblob(?))`, before.PageSize*2)
		if err != nil {
			fullErr = err
			break
		}
	}
	if fullErr == nil {
		t.Fatal("writes did not reach SQLite's configured max_page_count")
	}
	var sqliteErr *modernsqlite.Error
	if !errors.As(fullErr, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_FULL {
		t.Fatalf("full write error = %T %v, want SQLITE_FULL", fullErr, fullErr)
	}

	var rows int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM full_probe`).Scan(&rows); err != nil {
		t.Fatalf("read after SQLITE_FULL: %v", err)
	}
	if rows == 0 {
		t.Fatal("test reached SQLITE_FULL before committing any probe rows")
	}
	got, err := s.GetSource(ctx, baseline.ID)
	if err != nil || got.Name != baseline.Name {
		t.Fatalf("baseline read after SQLITE_FULL = %+v, %v", got, err)
	}
	assertSQLiteIntegrity(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after SQLITE_FULL: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertSQLiteIntegrity(t, reopened)
	got, err = reopened.GetSource(ctx, baseline.ID)
	if err != nil || got.Name != baseline.Name {
		t.Fatalf("baseline read after reopen = %+v, %v", got, err)
	}
}

func TestAbruptProcessExitRecoversCommittedWAL(t *testing.T) {
	if os.Getenv(crashHelperEnv) == "1" {
		runAbruptStoreWriter()
		return
	}

	path := filepath.Join(t.TempDir(), "crash.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAbruptProcessExitRecoversCommittedWAL$") // #nosec G204 -- fixed current test binary and fixed arguments.
	cmd.Env = append(os.Environ(), crashHelperEnv+"=1", crashHelperPathEnv+"="+path)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != crashHelperExitCode {
		t.Fatalf("crash helper = %v, output %s; want exit %d", err, output, crashHelperExitCode)
	}
	walInfo, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("stat WAL left by abrupt process exit: %v", err)
	}
	if walInfo.Size() == 0 {
		t.Fatal("abrupt process exit left an empty WAL; recovery path was not exercised")
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after abrupt process exit: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertSQLiteIntegrity(t, reopened)
	got, err := reopened.GetSource(context.Background(), "crash-probe")
	if err != nil {
		t.Fatalf("read committed WAL row after recovery: %v", err)
	}
	if len(got.Name) != 256<<10 || !strings.HasPrefix(got.Name, "committed-before-abrupt-exit") {
		t.Fatalf("recovered source marker is incomplete: name length %d", len(got.Name))
	}
}

// runAbruptStoreWriter is intentionally a child process. os.Exit bypasses
// Store.Close and every deferred cleanup, approximating a process crash while
// leaving a committed WAL for the parent to recover. It is not a power-cut
// durability test: the host kernel and storage stack still flush normally.
func runAbruptStoreWriter() {
	fail := func(format string, args ...any) {
		_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(2)
	}
	path := os.Getenv(crashHelperPathEnv)
	if path == "" {
		fail("%s is empty", crashHelperPathEnv)
	}
	s, err := Open(path)
	if err != nil {
		fail("open crash store: %v", err)
	}
	if _, err := s.DB().Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		fail("disable WAL autocheckpoint: %v", err)
	}
	marker := "committed-before-abrupt-exit" + strings.Repeat("x", (256<<10)-len("committed-before-abrupt-exit"))
	if err := s.PutSource(context.Background(), model.Source{
		ID: "crash-probe", Name: marker, Type: model.SourceFile, FilePath: "/capture.ndjson",
	}); err != nil {
		fail("commit crash probe: %v", err)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sources WHERE id = 'crash-probe'`).Scan(&count); err != nil || count != 1 {
		fail("verify committed crash probe: count=%d err=%v", count, err)
	}
	os.Exit(crashHelperExitCode)
}
