# Vessel release gates

Beacon's automated vessel gate provides a small, repeatable baseline for every
candidate build. It runs without network services or marine hardware, fails on
resource regressions, and exercises two SQLite recovery boundaries that matter
when a vessel is disconnected for long periods.

Run it on Linux from the repository root:

```bash
just vessel-gate
# or, without just
scripts/vessel-release-gate.sh
```

The gate uses `/proc` for process measurements and therefore intentionally
rejects non-Linux hosts. It creates its binary, database, and logs in a uniquely
named temporary directory, stops the child process on every exit path, and
removes the directory. It does not write a database or build artifact into the
working tree. On target hardware, set `BEACON_GATE_BINARY` to the executable
candidate to exercise that exact artifact instead of the script's local static
build. Besides Bash and `/proc`, the script checks for every command it invokes:
`go`, `curl`, `awk`, `sed`, `find`, `mktemp`, `tail`, `wc`, `sleep`, `rm`,
`cat`, `dirname`, and `uname`.

## Enforced baseline

The script performs these checks in order:

1. Run the focused SQLite full-database and abrupt-exit recovery tests.
2. Build the same stripped, static Beacon shape used for a Linux release.
3. Start Beacon with isolated storage and ephemeral admin and data ports.
4. Wait for readiness, allow the process to settle, then sample idle resident
   memory and open file descriptors five times.
5. Hot-apply a local captured-file → null-sink route replaying 50 Envelopes per
   second, require its labeled `beacon_source_messages_total` counter to become
   positive, sample RSS and descriptors again, and require the counter to
   advance across the sampling window so an idle or stalled route cannot pass.
6. Require readiness throughout both phases and a clean bounded shutdown on
   `SIGTERM`.

The conservative defaults are:

| Setting | Default | Meaning |
|---|---:|---|
| `BEACON_IDLE_RSS_MAX_KIB` | `65536` | Maximum sampled idle resident set (64 MiB) |
| `BEACON_IDLE_FD_MAX` | `32` | Maximum sampled open file descriptors |
| `BEACON_LOAD_RSS_MAX_KIB` | `98304` | Maximum sampled resident set under the replay workload (96 MiB) |
| `BEACON_LOAD_FD_MAX` | `32` | Maximum sampled descriptors under the replay workload |
| `BEACON_LOAD_RATE_PER_SECOND` | `50` | Local replay rate used during workload sampling |
| `BEACON_GATE_BINARY` | unset | Optional exact executable to qualify instead of building one |
| `BEACON_IDLE_SAMPLE_COUNT` | `5` | Number of samples in each idle and workload phase (legacy variable name) |
| `BEACON_IDLE_SAMPLE_INTERVAL_SECONDS` | `1` | Seconds between samples in each phase |
| `BEACON_IDLE_SETTLE_SECONDS` | `3` | Warm-up time before each phase's sampling |
| `BEACON_STARTUP_TIMEOUT_SECONDS` | `20` | Maximum readiness wait |
| `BEACON_SHUTDOWN_TIMEOUT_SECONDS` | `15` | Maximum graceful-shutdown wait |

All numeric settings must be positive integers. Override them to match a
target's approved budget, for example:

```bash
BEACON_IDLE_RSS_MAX_KIB=49152 \
BEACON_IDLE_FD_MAX=24 \
BEACON_LOAD_RSS_MAX_KIB=65536 \
BEACON_LOAD_FD_MAX=24 \
BEACON_LOAD_RATE_PER_SECOND=100 \
scripts/vessel-release-gate.sh
```

CI runs the same script on Linux and makes its result part of the aggregate
release gate. Record both idle and workload maxima for release-to-release trend
review. The built-in route is deliberately deterministic and dependency-free;
it does not replace a vessel-specific fan-out/outage soak. Idle and workload
budgets are independently configurable. Their default FD ceilings are both 32;
the workload RSS ceiling intentionally permits 32 MiB more than idle (96 MiB
versus 64 MiB).

## Upgrade preflight for an existing vessel

Do not let the candidate binary open the only copy of a vessel database. Store
opening performs schema migrations before the persisted resource policy is
reconciled, so a migration can succeed and startup can then reject legacy
configuration or a database above its desired ceiling. Use this preflight:

1. While the old Beacon is still running, export configuration from its live
   admin API to durable storage. This JSON is useful for validation and repair,
   but it is **not** a full backup: it excludes durable queues, appliance
   identity, inventory, diagnostic history, and file-sink output.
2. Stop Beacon cleanly. Snapshot or copy the complete SQLite storage directory,
   including the main database and any existing `-wal` and `-shm` sidecars.
   Back up file-sink paths separately. Keep this untouched rollback copy until
   the upgraded appliance has passed replay and integrity checks. Never use an
   offline CLI command against a database that a Beacon process still has open.
3. Exercise the candidate against a second, disposable copy of that database.
   A candidate `beacon export --db <copy>` safely tests store opening and schema
   migration on the copy. Separately, import the configuration export into a
   new throwaway database with the candidate:

   ```bash
   candidate-beacon import --db /tmp/beacon-preflight.db beacon-config.json
   ```

   This invokes current whole-document validation without starting sources or
   sinks. Repeat after repairing every reported issue. Current entity IDs must
   match the lowercase slug grammar and be no more than 128 **bytes**. For a
   legacy ID above that limit, choose a shorter ID and update every connector's
   `source_id`/`sink_id` reference before importing. Renaming a connector creates
   a new route identity: its old queue and checkpoint are not transferred and
   are eligible for purge, so drain that route first or explicitly accept the
   buffered-data loss.
4. Determine the desired `settings.resources.max_database_bytes` from the
   export; omission means 1 GiB. Compare it with the migrated copy's SQLite main
   file high-water mark, excluding the separate WAL. If the copy is above the
   desired ceiling, trial an offline compaction on that disposable copy:

   ```bash
   candidate-beacon compact --db /path/to/disposable-copy/beacon.db
   ```

   `compact` reports main-database bytes before and after. It runs `VACUUM`, so
   its filesystem needs temporary free space for approximately one additional
   database copy, plus WAL and filesystem overhead. If the compacted copy still
   exceeds the desired ceiling, live data—not merely free pages—does not fit:
   drain/prune data using supported Beacon operations or raise the desired
   budget. Do not deploy expecting the ceiling to delete live rows.
5. After the trial succeeds, preserve the rollback backup, apply the repaired
   configuration and any required offline compaction to the stopped production
   database, then start the candidate. Verify configuration, retained queues,
   sinks, `/health/ready`, and storage metrics before declaring the upgrade
   complete.

Before migrations, Beacon temporarily raises SQLite `max_page_count` to the
larger of the 1 GiB default or the current main-database high-water mark plus
128 MiB. This bounded migration ceiling lets a legacy database pinned at its
old limit add schema and index pages. It is capacity only: it does not
preallocate or reserve bytes, provide free filesystem space, include transient
WAL growth, compact the file, or change the persisted desired resource setting.
After migrations and configuration validation, Beacon applies that desired
setting. Startup still fails if the migrated main database occupies more pages
than the desired ceiling. This ordering is why the disposable-copy trial and
rollback backup are required.

## What the recovery checks prove

The `SQLITE_FULL` test configures a deliberately small SQLite
`max_page_count` through `Store.ConfigureResources`, writes until SQLite returns
`SQLITE_FULL`, and then verifies existing configuration remains readable and
the database passes `PRAGMA integrity_check` both before and after reopen. This
uses the same page-budget mechanism as Beacon's production database limit, but
it is not an operating-system `ENOSPC` test and does not simulate the WAL and
database files competing for the final free filesystem blocks.

The abrupt-exit test commits a marker into the WAL in a child process and calls
`os.Exit` without `Store.Close` or deferred cleanup. The parent requires a
non-empty WAL, reopens the database, reads the complete committed marker, and
runs `PRAGMA integrity_check`. This is a process-crash and power-loss proxy
only. The host kernel and storage stack are still running and can flush writes
normally; it cannot reproduce torn writes, volatile device caches, filesystem
failure modes, or the exact timing of power removal.

A file sink's confirmed boundary is a successful buffered `Flush` into the
host operating system's write path. It is not an `fsync` and does not claim
that bytes have reached stable media. Beacon intentionally avoids a synchronous
media flush for every delivered record or batch because that would amplify
writes and wear on vessel flash storage. Durability of the most recent file
records across sudden power loss therefore depends on the host, filesystem,
mount options, controller cache, and storage device, and must be measured in
the target qualification below.

## Vessel hardware qualification

Before deployment, repeat the resource gate on the actual CPU architecture,
kernel, filesystem, and storage medium, then supplement it with:

- a representative CAN/USB or captured-file input rate and the intended
  connector-route fan-out;
- a soak covering the longest credible disconnected-sink interval while
  watching RSS, CPU, file descriptors, database/WAL size, and storage writes;
- recovery after filling the real data filesystem to its reserved-space
  boundary; and
- controlled hard power cuts at varied write phases, followed by configuration
  reads, queue replay checks, and `PRAGMA integrity_check` after every restart.

Those hardware results define the deployable vessel envelope. The CI values
are regression guards for Beacon itself, not a substitute for storage and
power qualification of the host.
