# pgoutbox - AI Coding Instructions

## Architecture & Data Flow

**pgoutbox** captures PostgreSQL changes via logical decoding and publishes to message brokers (NATS, Kafka, RabbitMQ, Google Pub/Sub).

```
PostgreSQL WAL → pgx replication → Parser.ParseWalMessage() → WAL object → filter → Event → Publisher.Publish() → broker
```

### Key Components
- [internal/listener/listener.go](../internal/listener/listener.go) - Replication orchestration, slot/publication lifecycle
- [internal/listener/transaction/parser.go](../internal/listener/transaction/parser.go) - WAL binary protocol decoding
- [internal/publisher/](../internal/publisher/) - Broker adapters implementing `eventPublisher` interface
- [apis/config.go](../apis/config.go) - Config types with Viper loading (env prefix: `WAL_`)
- [apis/event.go](../apis/event.go) - Event struct with `SubjectName()` for topic construction

## Developer Workflows

```bash
make build          # Build for current OS/ARCH (Docker containerized)
make test           # Unit tests with race detector
make lint           # golangci-lint with gofmt, goimports, unparam
make ci             # verify + lint + build + unit-tests
make fmt            # Format source code
```

All commands run in Docker (`ghcr.io/appscode/golang-dev:1.24`). Output: `bin/pgoutbox-{OS}-{ARCH}`.

**Local PostgreSQL**: `docker/docker-compose.yml` + `docker/scripts/` for test DB setup.

## Critical Patterns

### Interface-Based Mocking
Listener depends on interfaces (`eventPublisher`, `parser`, `replication`, `repository`). Mock implementations in `*_mock_test.go` files:
```go
// internal/listener/listener.go
type eventPublisher interface {
    Publish(context.Context, string, *apis.Event) error
}
```

### Configuration
- YAML config + environment overrides (prefix `WAL_`, e.g., `WAL_DATABASE_HOST`)
- Struct tags: both `mapstructure` and `json` required
- Publisher types: `nats`, `kafka`, `rabbitmq`, `google_pubsub`

### Topic Naming
Format: `{publisher.topic}.{publisher.topicPrefix}schemas.{schema}.tables.{table}`
Override via `listener.topicsMap` in config.

### Logging
Use `log/slog` with injected `*slog.Logger`:
```go
l.log.Debug("message", slog.String("key", value))
```

## Common Tasks

| Task | Files to Modify |
|------|-----------------|
| Add publisher | `internal/publisher/{broker}.go` (implement `eventPublisher`), `cmd/pgoutbox/init.go` |
| Add filter logic | `apis/config.go` (`FilterStruct`), `internal/listener/transaction/parser.go` |
| Extend WAL parsing | `internal/listener/transaction/parser.go`, `transaction/data.go` |
| Add metrics | `apis/metrics.go` (Prometheus counters) |

## Standards

- **License**: Apache 2.0 headers required (see `hack/license/`)
- **Errors**: Wrap with `fmt.Errorf("context: %w", err)`
- **Publication name**: Hardcoded as `_pgoutbox_`
- **Replication slot**: Configured via `listener.slotName`, auto-created on startup
