# pgoutbox - AI Coding Instructions

## Architecture & Data Flow

**pgoutbox** captures PostgreSQL changes via logical decoding and publishes to message brokers (NATS JetStream, Kafka, RabbitMQ, Google Pub/Sub).

```
PostgreSQL WAL → pgx replication → Parser.ParseWalMessage() → WAL object → filter → Event → Publisher.Publish() → broker
```

### Key Components
- [internal/listener/listener.go](../internal/listener/listener.go) - Replication orchestration, slot/publication lifecycle, health probes
- [internal/listener/transaction/parser.go](../internal/listener/transaction/parser.go) - WAL message parsing using `pglogrepl.Parse()` (`pgoutput` plugin)
- [internal/listener/transaction/wal.go](../internal/listener/transaction/wal.go) - Transaction state, event filtering via `CreateEventsWithFilter()`
- [internal/publisher/](../internal/publisher/) - Broker adapters implementing `eventPublisher` interface
- [apis/config.go](../apis/config.go) - Config types with Viper loading (env prefix: `WAL_`)
- [apis/event.go](../apis/event.go) - Event struct with `SubjectName()` for topic construction
- [cmd/pgoutbox/init.go](../cmd/pgoutbox/init.go) - Publisher factory, connection setup with retry

## Developer Workflows

```bash
make build          # Build for current OS/ARCH (Docker containerized)
make test           # Unit tests with race detector
make lint           # golangci-lint with gofmt, goimports, unparam
make ci             # verify + lint + build + unit-tests
make fmt            # Format source code
```

All commands run in Docker (`ghcr.io/appscode/golang-dev:1.25`). Output: `bin/pgoutbox-{OS}-{ARCH}`.

**Local PostgreSQL**: `docker/docker-compose.yml` + `docker/scripts/` for test DB setup.

## Critical Patterns

### Interface-Based Testing
Listener depends on four interfaces defined in [listener.go](../internal/listener/listener.go):
```go
type eventPublisher interface { Publish(context.Context, string, *apis.Event) error }
type parser interface { ParseWalMessage([]byte, *tx.WAL) error }
type replication interface { CreateReplicationSlot(...); StartReplication(...); ReceiveMessage(...) }
type repository interface { CreatePublication(...); GetSlotLSN(...); IsReplicationActive(...) }
```
Mock implementations in `*_mock_test.go` files use `github.com/stretchr/testify/mock`.

### Adding a New Publisher
1. Create `internal/publisher/{broker}.go` implementing `Publish(ctx, subject, *Event) error` and `Close() error`
2. Add type constant in [apis/config.go](../apis/config.go): `PublisherType{Name} PublisherType = "{name}"`
3. Add case in `factoryPublisher()` in [cmd/pgoutbox/init.go](../cmd/pgoutbox/init.go)

### Configuration
- YAML config + environment overrides (prefix `WAL_`, e.g., `WAL_DATABASE_HOST`, `WAL_LISTENER_SLOTNAME`)
- Struct tags: both `mapstructure` and `json` required on all config fields
- Publisher types: `nats`, `kafka`, `rabbitmq`, `google_pubsub`

### Topic Naming
Format: `{publisher.topic}.{publisher.topicPrefix}schemas.{schema}.tables.{table}`
Override specific topics via `listener.topicsMap` in config.

### WAL Message Parsing
Uses `github.com/jackc/pglogrepl` for WAL message parsing. The `Parser.ParseWalMessage()` method uses `pglogrepl.Parse()` to decode messages:
- Message types: `BeginMessage`, `CommitMessage`, `RelationMessage`, `InsertMessage`, `UpdateMessage`, `DeleteMessage`
- Types: `pglogrepl.LSN` for LSN values, `uint32` for relation IDs, `*pglogrepl.TupleData` for row data
- Column flags: `Flags == 1` indicates the column is part of the primary key

### Logging
Use `log/slog` with injected `*slog.Logger`:
```go
l.log.Debug("message", slog.String("key", value))
l.log.Info("event was sent", slog.String("subject", name), slog.String("table", table))
```

## Common Tasks

| Task | Files to Modify |
|------|-----------------|
| Add publisher | `internal/publisher/{broker}.go`, `apis/config.go` (type const), `cmd/pgoutbox/init.go` (factory) |
| Add filter logic | `apis/config.go` (`FilterStruct`), `internal/listener/transaction/wal.go` (`CreateEventsWithFilter`) |
| Extend WAL parsing | `internal/listener/transaction/parser.go`, `transaction/data.go` (uses `pglogrepl.Parse()`) |
| Add metrics | `apis/metrics.go` (Prometheus counters), update `monitor` interface |
| Add config field | `apis/config.go` (add both `mapstructure` and `json` tags) |

## Standards

- **License**: Apache 2.0 headers required (see `hack/license/`)
- **Errors**: Wrap with `fmt.Errorf("context: %w", err)`
- **Publication name**: Hardcoded as `pgoutbox` (auto-created for all tables)
- **Replication slot**: Configured via `listener.slotName`, auto-created on startup
- **Tests**: Table-driven tests with `testify/assert` and `testify/mock`
