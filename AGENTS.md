# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/pgoutbox` — a transactional-outbox / WAL-listener service for PostgreSQL. Subscribes to PostgreSQL's logical decoding stream (via the standard `pgoutput` plugin) to convert WAL records into a logical replication feed, filters out the events you actually care about, and **publishes them to a message broker** so downstream services can consume them. The transactional outbox pattern lets domain-model changes and event publishing share a single Postgres transaction.

Supported message brokers (per `README.md`):
- `nats` — NATS JetStream.
- `kafka` — Apache Kafka.
- `rabbitmq` — RabbitMQ.

The produced binary is `pgoutbox`.

## Architecture

- `cmd/pgoutbox/`:
  - `main.go` — entry point.
  - `init.go` — startup init.
- `apis/`:
  - `config.go`, `config_test.go` — runtime configuration types.
  - `event.go` — event payload types (the shape published to brokers).
  - `logger.go` — logger configuration.
- `internal/`:
  - `listener/` — Postgres-side:
    - `listener.go` — `pgoutput` decoder + WAL reader.
    - `repository.go` — Postgres repository interface (slot management, etc.).
    - `transaction/` — transaction assembly from individual change records.
    - `*_test.go`, `*_mock*.go` — unit-test mocks.
  - `publisher/` — broker-side; **one file per broker**:
    - `nats.go`, `kafka.go`, `rabbit.go` — driver implementations.
    - `pubsub.go`, `pubsub_connection.go` — shared interfaces.
  - `telemetry/`:
    - `telemetry.go` — OpenTelemetry/Prometheus wiring.
    - `metrics/` — counter/histogram definitions.
  - `util/signal.go` — signal handling.
- `docker/` — runtime Docker assets.
- `Dockerfile.in` (PROD, distroless), `Dockerfile.dbg` (debian), `Dockerfile.ubi` (Red Hat certified) — three image variants.
- `hack/`, `Makefile` — AppsCode build harness.
- `vendor/` — checked-in deps.
- `wal-listener.png` — architecture diagram referenced from README.

This repo was forked from a community wal-listener project; the domain logic in `internal/listener/` follows pgoutput conventions, not custom.

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev` — Docker must be running.

- `make ci` — CI pipeline.
- `make build` / `make all-build` — build host or all-platform binaries.
- `make fmt`, `make lint`, `make unit-tests` / `make test` — standard.
- `make verify` — `verify-gen verify-modules`; `go mod tidy && go mod vendor` must leave the tree clean.
- `make container` — build PROD, DBG, and UBI images.
- `make push` — push all three; `make docker-manifest` writes multi-arch manifests; `make release` is the full publish flow.
- `make push-to-kind` / `make deploy-to-kind` — load into Kind and Helm-install.
- `make add-license` / `make check-license` — manage license headers.

Run a single Go test (requires a local Go toolchain):

```
go test ./internal/listener/... -run TestName -v
```

## Conventions

- Module path is `kubeops.dev/pgoutbox` (vanity URL). Imports must use that.
- License: `LICENSE`. Sign off commits (`git commit -s`); contributions follow the DCO (`DCO`).
- Vendor directory is checked in — `go mod tidy && go mod vendor` must leave the tree clean (enforced by `verify-modules`).
- Adding a new message broker: drop a `internal/publisher/<name>.go` implementing the publisher interface from `pubsub.go`. Don't fan out broker-specific logic across `internal/listener/`.
- The Postgres logical-decoding side relies on the **`pgoutput`** plugin (standard with PostgreSQL >= 10). Don't replace it with a custom output plugin — that breaks compatibility with managed Postgres.
- Event filtering happens in `internal/listener/transaction/`; broker selection happens in `internal/publisher/`. Keep those two surfaces separate.
- Three Dockerfiles, one binary — keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.
