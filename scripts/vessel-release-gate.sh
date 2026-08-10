#!/usr/bin/env bash

set -euo pipefail

for tool in go curl awk sed find mktemp tail wc sleep rm cat dirname uname; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "required tool is unavailable: $tool" >&2
    exit 2
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "vessel release gate requires Linux (/proc is used for RSS and FD measurements)" >&2
  exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gate_dir="$(mktemp -d "${TMPDIR:-/tmp}/beacon-vessel-gate.XXXXXX")"
gate_log="$gate_dir/beacon.log"
beacon_pid=""

rss_limit_kib="${BEACON_IDLE_RSS_MAX_KIB:-65536}"
fd_limit="${BEACON_IDLE_FD_MAX:-32}"
load_rss_limit_kib="${BEACON_LOAD_RSS_MAX_KIB:-98304}"
load_fd_limit="${BEACON_LOAD_FD_MAX:-32}"
load_rate="${BEACON_LOAD_RATE_PER_SECOND:-50}"
sample_count="${BEACON_IDLE_SAMPLE_COUNT:-5}"
sample_interval="${BEACON_IDLE_SAMPLE_INTERVAL_SECONDS:-1}"
settle_seconds="${BEACON_IDLE_SETTLE_SECONDS:-3}"
startup_timeout="${BEACON_STARTUP_TIMEOUT_SECONDS:-20}"
shutdown_timeout="${BEACON_SHUTDOWN_TIMEOUT_SECONDS:-15}"

show_log() {
  if [[ -s "$gate_log" ]]; then
    echo "--- beacon gate log ---" >&2
    tail -n 100 "$gate_log" >&2
  fi
}

die() {
  echo "vessel release gate failed: $*" >&2
  show_log
  exit 1
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
    die "$name must be a positive integer, got '$value'"
  fi
}

for setting in \
  "BEACON_IDLE_RSS_MAX_KIB:$rss_limit_kib" \
  "BEACON_IDLE_FD_MAX:$fd_limit" \
  "BEACON_LOAD_RSS_MAX_KIB:$load_rss_limit_kib" \
  "BEACON_LOAD_FD_MAX:$load_fd_limit" \
  "BEACON_LOAD_RATE_PER_SECOND:$load_rate" \
  "BEACON_IDLE_SAMPLE_COUNT:$sample_count" \
  "BEACON_IDLE_SAMPLE_INTERVAL_SECONDS:$sample_interval" \
  "BEACON_IDLE_SETTLE_SECONDS:$settle_seconds" \
  "BEACON_STARTUP_TIMEOUT_SECONDS:$startup_timeout" \
  "BEACON_SHUTDOWN_TIMEOUT_SECONDS:$shutdown_timeout"; do
  require_positive_integer "${setting%%:*}" "${setting#*:}"
done

process_running() {
  local pid="$1"
  local state
  [[ -r "/proc/$pid/stat" ]] || return 1
  state="$(awk '{print $3}' "/proc/$pid/stat")" || return 1
  [[ "$state" != "Z" ]]
}

cleanup() {
  local status=$?
  local i
  trap - EXIT INT TERM
  if [[ -n "$beacon_pid" ]]; then
    if process_running "$beacon_pid"; then
      kill -TERM "$beacon_pid" 2>/dev/null || true
      for ((i = 0; i < shutdown_timeout * 10; i++)); do
        if ! process_running "$beacon_pid"; then
          break
        fi
        sleep 0.1
      done
      if process_running "$beacon_pid"; then
        kill -KILL "$beacon_pid" 2>/dev/null || true
      fi
    fi
    wait "$beacon_pid" 2>/dev/null || true
  fi
  if [[ -d "$gate_dir" && "${gate_dir##*/}" == beacon-vessel-gate.* ]]; then
    rm -rf -- "$gate_dir"
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

cd "$repo_dir"

echo "running SQLite full/crash recovery gates"
store_gate_tests=(
  TestSQLiteFullLeavesStoreReadableAndIntact
  TestAbruptProcessExitRecoversCommittedWAL
)
listed_store_tests="$(
  go test ./internal/store \
    -list '^(TestSQLiteFullLeavesStoreReadableAndIntact|TestAbruptProcessExitRecoversCommittedWAL)$'
)"
for gate_test in "${store_gate_tests[@]}"; do
  if [[ "$listed_store_tests" != *"$gate_test"* ]]; then
    die "required store recovery test is missing: $gate_test"
  fi
done
go test ./internal/store \
  -run '^(TestSQLiteFullLeavesStoreReadableAndIntact|TestAbruptProcessExitRecoversCommittedWAL)$' \
  -count=1 -timeout=60s

binary="${BEACON_GATE_BINARY:-}"
if [[ -n "$binary" ]]; then
  if [[ "$binary" != /* ]]; then
    binary="$(pwd)/$binary"
  fi
  [[ -x "$binary" ]] || die "BEACON_GATE_BINARY is not executable: $binary"
  echo "using supplied release binary on $(uname -m): $binary"
else
  binary="$gate_dir/beacon"
  echo "building Linux release-proxy binary for $(uname -m)"
  CGO_ENABLED=0 go build -trimpath \
    -ldflags '-s -w -X main.version=vessel-release-gate' \
    -o "$binary" ./cmd/beacon
fi

"$binary" \
  --db "$gate_dir/beacon.db" \
  --data-address 127.0.0.1:0 \
  --admin-address 127.0.0.1:0 \
  --log-level info >"$gate_log" 2>&1 &
beacon_pid=$!

admin_addr=""
startup_deadline=$((SECONDS + startup_timeout))
while ((SECONDS < startup_deadline)); do
  if ! process_running "$beacon_pid"; then
    die "Beacon exited before becoming ready"
  fi
  admin_addr="$(sed -n 's/.*"msg":"beacon ready".*"admin":"\([^"]*\)".*/\1/p' "$gate_log" | tail -n 1)"
  if [[ -n "$admin_addr" ]] &&
    curl --fail --silent --show-error --max-time 2 "http://$admin_addr/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if [[ -z "$admin_addr" ]] ||
  ! curl --fail --silent --show-error --max-time 2 "http://$admin_addr/health/ready" >/dev/null; then
  die "Beacon did not become ready within ${startup_timeout}s"
fi

sleep "$settle_seconds"

max_rss_kib=0
max_fds=0
for ((sample = 1; sample <= sample_count; sample++)); do
  if ! process_running "$beacon_pid"; then
    die "Beacon exited during idle sampling"
  fi
  rss_kib="$(awk '$1 == "VmRSS:" {print $2}' "/proc/$beacon_pid/status")"
  fd_count="$(find "/proc/$beacon_pid/fd" -mindepth 1 -maxdepth 1 -type l -printf '%f\n' | wc -l)"
  fd_count="${fd_count//[[:space:]]/}"
  [[ "$rss_kib" =~ ^[0-9]+$ ]] || die "could not read VmRSS for pid $beacon_pid"
  [[ "$fd_count" =~ ^[0-9]+$ ]] || die "could not count FDs for pid $beacon_pid"
  if ((rss_kib > max_rss_kib)); then
    max_rss_kib=$rss_kib
  fi
  if ((fd_count > max_fds)); then
    max_fds=$fd_count
  fi
  printf 'idle sample %d/%d: RSS=%d KiB FDs=%d\n' "$sample" "$sample_count" "$rss_kib" "$fd_count"
  if ((sample < sample_count)); then
    sleep "$sample_interval"
  fi
done

if ((max_rss_kib > rss_limit_kib)); then
  die "maximum idle RSS ${max_rss_kib} KiB exceeds ${rss_limit_kib} KiB"
fi
if ((max_fds > fd_limit)); then
  die "maximum idle FD count $max_fds exceeds $fd_limit"
fi
if ! curl --fail --silent --show-error --max-time 2 "http://$admin_addr/health/ready" >/dev/null; then
  die "Beacon lost readiness during idle sampling"
fi

load_window_seconds=$((settle_seconds + sample_count * sample_interval + 5))
load_records=$((load_rate * load_window_seconds))
load_capture="$gate_dir/load.candump"
load_config="$gate_dir/load-config.json"
awk -v rate="$load_rate" -v records="$load_records" 'BEGIN {
  for (i = 0; i < records; i++) {
    seconds = 1720000000 + int(i / rate)
    micros = int((i % rate) * 1000000 / rate)
    printf "(%d.%06d) can0 09F11201#015C3DFF7FFF7FFC\n", seconds, micros
  }
}' >"$load_capture"
cat >"$load_config" <<EOF
{
  "sources": [{
    "id": "release-replay", "name": "Release replay", "type": "file",
    "enabled": true, "file_path": "$load_capture"
  }],
  "sinks": [{
    "id": "release-null", "name": "Release null", "type": "null", "enabled": true
  }],
  "connectors": [{
    "id": "release-route", "name": "Release route", "source_id": "release-replay",
    "sink_id": "release-null", "enabled": true, "filters": [], "buffer": {}
  }]
}
EOF

echo "applying ${load_rate}-message/s replay workload"
curl --fail --silent --show-error --max-time 5 \
  -H 'Content-Type: application/json' \
  --data-binary "@$load_config" \
  "http://$admin_addr/api/v1/config/import?mode=replace" >/dev/null ||
  die "could not apply representative replay workload"

load_messages=""
load_deadline=$((SECONDS + startup_timeout))
while ((SECONDS < load_deadline)); do
  if ! process_running "$beacon_pid"; then
    die "Beacon exited while starting representative workload"
  fi
  if curl --fail --silent --show-error --max-time 2 \
    "http://$admin_addr/health/ready" >/dev/null 2>&1; then
    load_messages="$(
      curl --fail --silent --show-error --max-time 2 "http://$admin_addr/metrics" 2>/dev/null |
        awk '$1 ~ /^beacon_source_messages_total\{/ &&
             $1 ~ /[,{]source="release-replay"[,}]/ && ($2 + 0) > 0 { print $2; exit }'
    )" || load_messages=""
  fi
  if [[ -n "$load_messages" ]]; then
    break
  fi
  sleep 0.2
done
if [[ -z "$load_messages" ]]; then
  die "representative workload produced no release-replay source messages within ${startup_timeout}s"
fi
printf 'representative workload observed: beacon_source_messages_total{source="release-replay"}=%s\n' \
  "$load_messages"

sleep "$settle_seconds"
max_load_rss_kib=0
max_load_fds=0
for ((sample = 1; sample <= sample_count; sample++)); do
  if ! process_running "$beacon_pid"; then
    die "Beacon exited during workload sampling"
  fi
  load_rss_kib="$(awk '$1 == "VmRSS:" {print $2}' "/proc/$beacon_pid/status")"
  load_fd_count="$(find "/proc/$beacon_pid/fd" -mindepth 1 -maxdepth 1 -type l -printf '%f\n' | wc -l)"
  load_fd_count="${load_fd_count//[[:space:]]/}"
  [[ "$load_rss_kib" =~ ^[0-9]+$ ]] || die "could not read workload VmRSS for pid $beacon_pid"
  [[ "$load_fd_count" =~ ^[0-9]+$ ]] || die "could not count workload FDs for pid $beacon_pid"
  if ((load_rss_kib > max_load_rss_kib)); then
    max_load_rss_kib=$load_rss_kib
  fi
  if ((load_fd_count > max_load_fds)); then
    max_load_fds=$load_fd_count
  fi
  printf 'workload sample %d/%d: RSS=%d KiB FDs=%d\n' \
    "$sample" "$sample_count" "$load_rss_kib" "$load_fd_count"
  if ((sample < sample_count)); then
    sleep "$sample_interval"
  fi
done

if ((max_load_rss_kib > load_rss_limit_kib)); then
  die "maximum workload RSS ${max_load_rss_kib} KiB exceeds ${load_rss_limit_kib} KiB"
fi
if ((max_load_fds > load_fd_limit)); then
  die "maximum workload FD count $max_load_fds exceeds $load_fd_limit"
fi
if ! curl --fail --silent --show-error --max-time 2 "http://$admin_addr/health/ready" >/dev/null; then
  die "Beacon lost readiness during workload sampling"
fi
load_messages_after="$(
  curl --fail --silent --show-error --max-time 2 "http://$admin_addr/metrics" 2>/dev/null |
    awk '$1 ~ /^beacon_source_messages_total\{/ &&
         $1 ~ /[,{]source="release-replay"[,}]/ { print $2; exit }'
)" || load_messages_after=""
if [[ -z "$load_messages_after" ]]; then
  die "representative workload counter disappeared during workload sampling"
fi
if ! awk -v before="$load_messages" -v after="$load_messages_after" \
  'BEGIN { exit !((after + 0) > (before + 0)) }'; then
  die "representative workload stalled during sampling (${load_messages} -> ${load_messages_after})"
fi
printf 'representative workload progressed during sampling: %s -> %s messages\n' \
  "$load_messages" "$load_messages_after"

kill -TERM "$beacon_pid"
for ((i = 0; i < shutdown_timeout * 10; i++)); do
  if ! process_running "$beacon_pid"; then
    break
  fi
  sleep 0.1
done
if process_running "$beacon_pid"; then
  die "Beacon did not shut down within ${shutdown_timeout}s"
fi
set +e
wait "$beacon_pid"
beacon_status=$?
set -e
beacon_pid=""
if ((beacon_status != 0)); then
  die "Beacon exited with status $beacon_status after SIGTERM"
fi

printf 'vessel release gate passed: idle RSS=%d/%d KiB FDs=%d/%d; workload RSS=%d/%d KiB FDs=%d/%d\n' \
  "$max_rss_kib" "$rss_limit_kib" "$max_fds" "$fd_limit" \
  "$max_load_rss_kib" "$load_rss_limit_kib" "$max_load_fds" "$load_fd_limit"
