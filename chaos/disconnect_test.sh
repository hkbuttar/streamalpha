#!/usr/bin/env bash
# Disconnect test: block stream.data.alpaca.markets at the network layer
# (pfctl) before ingestion's first connection attempt can succeed, then
# unblock it, and confirm ingestion/run.py's own connect-timeout watchdog
# and backoff/retry loop (see that file's module docstring) actually logs
# the gap instead of silently absorbing it, and reconnects cleanly once
# the network is restored.
#
# Blocking before the first successful connect, not mid-stream: a
# mid-stream drop is caught and silently retried *inside* alpaca-py's own
# internal loop without ever returning control to our code (see the
# "Transient drops" section of ingestion/run.py's docstring), so it
# wouldn't exercise our own gap-logging/backoff code at all. A failure to
# ever connect is what our own _run_with_watchdog()/backoff loop is built
# to detect and log -- and unlike waiting for live trade ticks, it doesn't
# depend on the market being open right now.
#
# Uses a temporary pf ruleset, not a permanent rule: the entire current pf
# ruleset is saved before any change and reloaded byte-for-byte on exit
# (including on Ctrl+C or an error, via the trap below), and pf is left
# disabled afterward if it was disabled before this script ran. Requires
# sudo.

set -uo pipefail

HOST="stream.data.alpaca.markets"
BLOCK_SECONDS=30
POST_UNBLOCK_WAIT_SECONDS=30
LOG_FILE="$(mktemp -t streamalpha-disconnect-test.XXXXXX)"
PF_BACKUP="$(mktemp -t streamalpha-pf-backup.XXXXXX)"
PF_RULES="$(mktemp -t streamalpha-pf-rules.XXXXXX)"
INGESTION_PID=""
PF_WAS_ENABLED="no"
BLOCK_ACTIVE="no"

cleanup() {
  echo "--- cleanup ---"
  if [[ -n "$INGESTION_PID" ]] && kill -0 "$INGESTION_PID" 2>/dev/null; then
    kill -TERM "$INGESTION_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$INGESTION_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -0 "$INGESTION_PID" 2>/dev/null && kill -KILL "$INGESTION_PID" 2>/dev/null || true
  fi
  if [[ "$BLOCK_ACTIVE" == "yes" ]]; then
    echo "restoring original pf ruleset"
    sudo pfctl -f "$PF_BACKUP" 2>/dev/null || true
    if [[ "$PF_WAS_ENABLED" == "no" ]]; then
      sudo pfctl -d 2>/dev/null || true
    fi
  fi
  rm -f "$PF_BACKUP" "$PF_RULES"
  echo "cleanup done. ingestion log kept at: $LOG_FILE"
}
trap cleanup EXIT INT TERM

echo "resolving $HOST ..."
IPS=$(python3 -c "
import socket
_, _, ips = socket.gethostbyname_ex('$HOST')
print(' '.join(sorted(set(ips))))
")
if [[ -z "$IPS" ]]; then
  echo "could not resolve $HOST, aborting" >&2
  exit 1
fi
echo "resolved IPs: $IPS"

echo "saving current pf state ..."
if sudo pfctl -s info 2>/dev/null | grep -q "Status: Enabled"; then
  PF_WAS_ENABLED="yes"
fi
sudo pfctl -sr > "$PF_BACKUP" 2>/dev/null || true
echo "pf was enabled before this script: $PF_WAS_ENABLED"

cp "$PF_BACKUP" "$PF_RULES"
for ip in $IPS; do
  echo "block drop quick inet from any to $ip" >> "$PF_RULES"
  echo "block drop quick inet from $ip to any" >> "$PF_RULES"
done

echo "validating pf rules before applying ..."
if ! sudo pfctl -nf "$PF_RULES"; then
  echo "pf rules failed to validate, aborting before anything was blocked" >&2
  exit 1
fi

echo "blocking $HOST ($IPS) now, before ingestion starts"
sudo pfctl -f "$PF_RULES"
sudo pfctl -e 2>/dev/null || true
BLOCK_ACTIVE="yes"

first_ip=$(echo $IPS | awk '{print $1}')
if nc -z -w 3 "$first_ip" 443 2>/dev/null; then
  echo "WARNING: connection to $first_ip:443 still succeeded -- block may not be effective"
else
  echo "confirmed: connection to $first_ip:443 is blocked"
fi

cd "$(dirname "$0")/.."
source .venv/bin/activate
echo "starting ingestion (log: $LOG_FILE) ..."
python -m ingestion > "$LOG_FILE" 2>&1 &
INGESTION_PID=$!

echo "letting it run blocked for ${BLOCK_SECONDS}s ..."
sleep "$BLOCK_SECONDS"
echo "--- log at t=${BLOCK_SECONDS}s (still blocked) ---"
cat "$LOG_FILE"

echo "unblocking (restoring original pf ruleset) ..."
sudo pfctl -f "$PF_BACKUP"
if [[ "$PF_WAS_ENABLED" == "no" ]]; then
  sudo pfctl -d 2>/dev/null || true
fi
BLOCK_ACTIVE="no"

echo "waiting ${POST_UNBLOCK_WAIT_SECONDS}s for reconnect ..."
sleep "$POST_UNBLOCK_WAIT_SECONDS"
echo "--- final ingestion log ---"
cat "$LOG_FILE"

echo "shutting down ingestion cleanly ..."
kill -TERM "$INGESTION_PID"
for _ in $(seq 1 20); do
  kill -0 "$INGESTION_PID" 2>/dev/null || break
  sleep 0.5
done
INGESTION_PID=""

echo "done. full log at: $LOG_FILE"
