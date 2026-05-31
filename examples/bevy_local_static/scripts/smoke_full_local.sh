#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CONTAINER_TOOL="${CONTAINER_TOOL:-}"
NATS_IMAGE="${NATS_IMAGE:-nats:2.11-alpine}"
NATS_CONTAINER_NAME="${NATS_CONTAINER_NAME:-lightyear-matchmaker-nats-smoke-$$}"
NATS_PORT="${NATS_PORT:-14222}"
NATS_MONITOR_PORT="${NATS_MONITOR_PORT:-18222}"
NATS_URL="${NATS_SMOKE_URL:-nats://127.0.0.1:${NATS_PORT}}"
NATS_NAMESPACE="${NATS_SMOKE_NAMESPACE:-full_smoke_$$}"
SMOKE_ALLOCATION_SOURCE="${LIGHTYEAR_MATCHMAKER_NATS_SMOKE_ALLOCATION_SOURCE:-nats_static}"
SMOKE_REQUIRE_PREPARE="${LIGHTYEAR_MATCHMAKER_NATS_SMOKE_REQUIRE_PREPARE:-true}"
SMOKE_EXPECT_ACTIVE="${LIGHTYEAR_MATCHMAKER_NATS_SMOKE_EXPECT_ACTIVE:-true}"
GAME_SERVER_RUN_SECONDS="${GAME_SERVER_RUN_SECONDS:-30}"
GAME_SERVER_LOG="${GAME_SERVER_LOG:-${ROOT}/target/bevy-local-static-server-smoke.log}"
START_NATS="${START_NATS:-true}"

if [[ -z "${CONTAINER_TOOL}" ]]; then
  if command -v podman >/dev/null 2>&1; then
    CONTAINER_TOOL=podman
  elif command -v docker >/dev/null 2>&1; then
    CONTAINER_TOOL=docker
  else
    echo "Neither podman nor docker is available. Set START_NATS=false and provide NATS_SMOKE_URL." >&2
    exit 1
  fi
fi

cleanup() {
  if [[ -n "${GAME_SERVER_PID:-}" ]] && kill -0 "${GAME_SERVER_PID}" >/dev/null 2>&1; then
    kill "${GAME_SERVER_PID}" >/dev/null 2>&1 || true
    wait "${GAME_SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${START_NATS}" == "true" ]]; then
    "${CONTAINER_TOOL}" rm -f "${NATS_CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_port() {
  local host="$1"
  local port="$2"
  local attempts="${3:-100}"
  for _ in $(seq 1 "${attempts}"); do
    if command -v nc >/dev/null 2>&1; then
      if nc -z "${host}" "${port}" >/dev/null 2>&1; then
        return 0
      fi
    elif (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local attempts="${3:-100}"
  for _ in $(seq 1 "${attempts}"); do
    if grep -q "${pattern}" "${file}"; then
      return 0
    fi
    if [[ -n "${GAME_SERVER_PID:-}" ]] && ! kill -0 "${GAME_SERVER_PID}" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.2
  done
  return 1
}

cd "${ROOT}"

if [[ "${START_NATS}" == "true" ]]; then
  "${CONTAINER_TOOL}" rm -f "${NATS_CONTAINER_NAME}" >/dev/null 2>&1 || true
  "${CONTAINER_TOOL}" run --rm -d \
    --name "${NATS_CONTAINER_NAME}" \
    -p "127.0.0.1:${NATS_PORT}:4222" \
    -p "127.0.0.1:${NATS_MONITOR_PORT}:8222" \
    "${NATS_IMAGE}" -js -m 8222 >/dev/null

  if ! wait_for_port 127.0.0.1 "${NATS_PORT}"; then
    echo "NATS did not become ready on 127.0.0.1:${NATS_PORT}" >&2
    "${CONTAINER_TOOL}" logs "${NATS_CONTAINER_NAME}" >&2 || true
    exit 1
  fi
fi

cargo build -p bevy_local_static_server -j 4
cargo test -p lightyear_matchmaker_server --test nats_smoke -j 4 --no-run

mkdir -p "$(dirname "${GAME_SERVER_LOG}")"
: >"${GAME_SERVER_LOG}"
cargo run -p bevy_local_static_server -- \
  --config examples/bevy_local_static/config/game-server.local.toml \
  --nats-url "${NATS_URL}" \
  --nats-namespace "${NATS_NAMESPACE}" \
  --run-seconds "${GAME_SERVER_RUN_SECONDS}" \
  >"${GAME_SERVER_LOG}" 2>&1 &
GAME_SERVER_PID=$!

if ! wait_for_log "${GAME_SERVER_LOG}" "bevy local static server started"; then
  echo "Game server did not report startup." >&2
  cat "${GAME_SERVER_LOG}" >&2
  exit 1
fi
sleep "${GAME_SERVER_READY_DELAY_SECONDS:-1}"

LIGHTYEAR_MATCHMAKER_NATS_SMOKE_URL="${NATS_URL}" \
LIGHTYEAR_MATCHMAKER_NATS_SMOKE_NAMESPACE="${NATS_NAMESPACE}" \
LIGHTYEAR_MATCHMAKER_NATS_SMOKE_ALLOCATION_SOURCE="${SMOKE_ALLOCATION_SOURCE}" \
LIGHTYEAR_MATCHMAKER_NATS_SMOKE_REQUIRE_PREPARE="${SMOKE_REQUIRE_PREPARE}" \
LIGHTYEAR_MATCHMAKER_NATS_SMOKE_EXPECT_ACTIVE="${SMOKE_EXPECT_ACTIVE}" \
  cargo test -p lightyear_matchmaker_server --test nats_smoke -j 4 -- --ignored --nocapture

echo "Full local smoke passed with namespace ${NATS_NAMESPACE}."
