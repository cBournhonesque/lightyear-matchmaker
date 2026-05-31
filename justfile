set dotenv-load := true

CONTAINER_TOOL := env_var_or_default("CONTAINER_TOOL", "docker")
COMPOSE_FILE := env_var_or_default("COMPOSE_FILE", "examples/bevy_local_static/docker-compose/compose.local.yml")
CONFIG := env_var_or_default("MATCHMAKER_CONFIG", "examples/bevy_local_static/config/matchmaker.local.toml")
GAME_SERVER_CONFIG := env_var_or_default("GAME_SERVER_CONFIG", "examples/bevy_local_static/config/game-server.local.toml")
NATS_SMOKE_URL := env_var_or_default("NATS_SMOKE_URL", "nats://127.0.0.1:4222")
NATS_SMOKE_NAMESPACE := env_var_or_default("NATS_SMOKE_NAMESPACE", "lightyear_local")

check:
    cargo check --workspace -j 4

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -j 4 -- -D warnings

test:
    cargo test --workspace -j 4

run-matchmaker:
    cargo run -p lightyear_matchmaker_server -- --config {{CONFIG}}

run-matchmaker-nats:
    cargo run -p lightyear_matchmaker_server -- --config examples/bevy_local_static/config/matchmaker.nats.local.toml

run-game-server:
    cargo run -p bevy_local_static_server -- --config {{GAME_SERVER_CONFIG}}

compose-up:
    {{CONTAINER_TOOL}} compose -f {{COMPOSE_FILE}} up --build

compose-down:
    {{CONTAINER_TOOL}} compose -f {{COMPOSE_FILE}} down

compose-logs:
    {{CONTAINER_TOOL}} compose -f {{COMPOSE_FILE}} logs -f

smoke-local:
    cargo test --workspace -j 4

smoke-nats:
    LIGHTYEAR_MATCHMAKER_NATS_SMOKE_URL={{NATS_SMOKE_URL}} cargo test -p lightyear_matchmaker_server --test nats_smoke -j 4 -- --ignored --nocapture

smoke-nats-active:
    LIGHTYEAR_MATCHMAKER_NATS_SMOKE_URL={{NATS_SMOKE_URL}} LIGHTYEAR_MATCHMAKER_NATS_SMOKE_NAMESPACE={{NATS_SMOKE_NAMESPACE}} LIGHTYEAR_MATCHMAKER_NATS_SMOKE_ALLOCATION_SOURCE=nats_static LIGHTYEAR_MATCHMAKER_NATS_SMOKE_REQUIRE_PREPARE=true LIGHTYEAR_MATCHMAKER_NATS_SMOKE_EXPECT_ACTIVE=true cargo test -p lightyear_matchmaker_server --test nats_smoke -j 4 -- --ignored --nocapture

smoke-full-local:
    examples/bevy_local_static/scripts/smoke_full_local.sh

smoke-full-edgegap-mock:
    LIGHTYEAR_MATCHMAKER_NATS_SMOKE_ALLOCATION_SOURCE=edgegap_mock examples/bevy_local_static/scripts/smoke_full_local.sh
