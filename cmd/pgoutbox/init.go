/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kubeops.dev/pgoutbox/apis"
	"kubeops.dev/pgoutbox/internal/listener"
	"kubeops.dev/pgoutbox/internal/pgpool"
	"kubeops.dev/pgoutbox/internal/publisher"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/util/wait"
)

// replicationConn wraps pgconn.PgConn to implement the replication interface expected by the listener.
type replicationConn struct {
	conn *pgconn.PgConn
}

func newReplicationConn(conn *pgconn.PgConn) *replicationConn {
	return &replicationConn{conn: conn}
}

func (r *replicationConn) CreateReplicationSlot(ctx context.Context, slotName, outputPlugin string) (pglogrepl.CreateReplicationSlotResult, error) {
	return pglogrepl.CreateReplicationSlot(ctx, r.conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
}

func (r *replicationConn) DropReplicationSlot(ctx context.Context, slotName string) error {
	return pglogrepl.DropReplicationSlot(ctx, r.conn, slotName, pglogrepl.DropReplicationSlotOptions{})
}

func (r *replicationConn) StartReplication(ctx context.Context, slotName string, startLsn pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error {
	return pglogrepl.StartReplication(ctx, r.conn, slotName, startLsn, options)
}

func (r *replicationConn) ReceiveMessage(ctx context.Context) ([]byte, error) {
	rawMsg, err := r.conn.ReceiveMessage(ctx)
	if err != nil {
		return nil, err
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("received error from postgres: %s", errMsg.Message)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, nil
	}

	return msg.Data, nil
}

func (r *replicationConn) SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, status)
}

func (r *replicationConn) IsAlive() bool {
	return !r.conn.IsClosed()
}

func (r *replicationConn) Close() error {
	return r.conn.Close(context.Background())
}

// connectAttemptTimeout bounds one attempt at establishing the connections: up to
// three dials plus the recovery check on the pgpool path.
const connectAttemptTimeout = 10 * time.Second

// initPgxConnections initialise db and replication connections.
//
// Both connections are opened directly against the PostgreSQL primary, never
// through a connection pooler. pgpool-II has no support for the logical
// replication protocol — it implements neither the `replication=database`
// startup option nor the CopyBoth mode that START_REPLICATION switches the
// connection into — so a stream routed through it stalls and is torn down. Slot
// and publication statements have to reach the primary as well, which a
// read/write splitting pooler does not guarantee for reads.
//
// The primary is resolved and then verified on every attempt, so a failover in
// progress is waited out rather than latched onto.
func initPgxConnections(
	ctx context.Context,
	cfg *apis.DatabaseCfg,
	logger *slog.Logger,
	timeout time.Duration,
) (*pgx.Conn, *pgconn.PgConn, error) {
	var pgConn *pgx.Conn
	var pgReplConn *pgconn.PgConn

	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		// pgx sets no dial timeout of its own, so without a per attempt deadline a
		// single unreachable address blocks on the kernel's SYN retries and eats
		// most of the overall budget, and the retry interval never gets to apply.
		// The deadline is safe to release once connected: pgconn stops watching the
		// context when Connect returns.
		ctx, cancel := context.WithTimeout(ctx, connectAttemptTimeout)
		defer cancel()

		// Resolved on every attempt so a failover during the wait is picked up.
		host, port, err := resolvePrimary(ctx, cfg, logger)
		if err != nil {
			logger.Error("resolve primary:", slog.String("error", err.Error()))
			return false, nil
		}

		dsn := connString(cfg, host, port)

		pgConn, err = pgx.Connect(ctx, dsn)
		if err != nil {
			logger.Error("db connection:", slog.String("error", err.Error()))
			return false, nil
		}

		// Ask the node itself, whichever way its address was obtained. A plain
		// PostgreSQL host may be a standby, and pgpool's view of which backend
		// is the primary is refreshed by a background poll, so it can name a
		// node that has already been demoted. Either way the answer here is
		// authoritative, and a standby is a wait-and-retry, not a hard failure:
		// the endpoint follows the promotion.
		if err = verifyNotInRecovery(ctx, pgConn); err != nil {
			logger.Error("verify primary:", slog.String("host", host), slog.String("error", err.Error()))
			closeConn(ctx, pgConn, logger)

			return false, nil
		}

		pgReplConn, err = pgconn.Connect(ctx, dsn+" replication=database")
		if err != nil {
			logger.Error("db replication connection:", slog.String("error", err.Error()))
			closeConn(ctx, pgConn, logger)

			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("wait for db connection: %w", err)
	}

	return pgConn, pgReplConn, nil
}

// errInRecovery reports a connection that landed on a standby. Publications,
// slots and the replication stream all require the primary.
var errInRecovery = errors.New("node is in recovery, it is a standby and not the primary")

// verifyNotInRecovery fails when the connected node is a standby.
func verifyNotInRecovery(ctx context.Context, conn *pgx.Conn) error {
	inRecovery, err := listener.NewRepository(conn).IsInRecovery(ctx)
	if err != nil {
		return fmt.Errorf("query recovery state: %w", err)
	}

	if inRecovery {
		return errInRecovery
	}

	return nil
}

func closeConn(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) {
	if err := conn.Close(ctx); err != nil {
		logger.Warn("close db connection", "err", err)
	}
}

// resolvePrimary returns the address of the PostgreSQL primary to connect to.
// When cfg.Host turns out to be a pgpool endpoint, pgpool is asked which of its
// backends is currently the primary so that it can be dialled directly.
func resolvePrimary(ctx context.Context, cfg *apis.DatabaseCfg, logger *slog.Logger) (string, uint16, error) {
	conn, err := pgx.Connect(ctx, connString(cfg, cfg.Host, cfg.Port))
	if err != nil {
		return "", 0, fmt.Errorf("connect to %s:%d: %w", cfg.Host, cfg.Port, err)
	}

	defer func() {
		if err := conn.Close(ctx); err != nil {
			logger.Warn("close primary lookup connection", "err", err)
		}
	}()

	node, err := pgpool.Primary(ctx, conn)

	var pgErr *pgconn.PgError

	switch {
	case errors.Is(err, pgpool.ErrNotPgpool):
		// Plain PostgreSQL: the configured host is the one to talk to, and the
		// caller's recovery check decides whether it is the primary.
		return cfg.Host, cfg.Port, nil
	case errors.As(err, &pgErr):
		// The endpoint speaks SQL but rejected the pgpool command for an
		// unexpected reason. Fall back to it rather than refuse to start.
		logger.Warn("pgpool detection failed, treating the endpoint as plain PostgreSQL", "err", err)

		return cfg.Host, cfg.Port, nil
	case err != nil:
		return "", 0, err
	}

	logger.Info(
		"pgpool detected, bypassing it and connecting to the primary backend directly",
		slog.String("pgpool", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
		slog.String("primary", node.Addr()),
	)

	return node.Host, node.Port, nil
}

// connValueEscaper escapes the characters libpq treats specially inside a single
// quoted connection string value.
var connValueEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// connString builds the libpq keyword/value connection string for the given
// endpoint. Values are quoted so that a password containing whitespace or a
// quote does not shift the meaning of the string.
func connString(cfg *apis.DatabaseCfg, host string, port uint16) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		quoteConnValue(host), port,
		quoteConnValue(cfg.Name), quoteConnValue(cfg.User), quoteConnValue(cfg.Password))
}

func quoteConnValue(value string) string {
	return "'" + connValueEscaper.Replace(value) + "'"
}

func configureReplicaIdentityToFull(ctx context.Context, pgConn *pgx.Conn, filterTables apis.FilterStruct) error {
	for table := range filterTables.Tables {
		_, err := pgConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", table))
		if err != nil {
			return fmt.Errorf("change replica identity to FULL for table %s: %w", table, err)
		}
	}

	return nil
}

type eventPublisher interface {
	Publish(context.Context, string, *apis.Event) error
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(ctx context.Context, cfg *apis.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
	switch cfg.Type {
	case apis.PublisherTypeKafka:
		producer, err := publisher.NewProducer(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka producer: %w", err)
		}

		return publisher.NewKafkaPublisher(producer), nil
	case apis.PublisherTypeNats:
		conn, err := nats.Connect(cfg.Address, nats.UserCredentials(cfg.NatsCredPath))
		if err != nil {
			return nil, fmt.Errorf("nats connection: %w", err)
		}

		pub, err := publisher.NewNatsPublisher(conn, logger)
		if err != nil {
			return nil, fmt.Errorf("new nats publisher: %w", err)
		}

		if err = pub.WaitForStreamToBeCreated(context.TODO(), cfg.Topic); err != nil {
			return nil, fmt.Errorf("wait for stream to be created: %w", err)
		}

		return pub, nil
	case apis.PublisherTypeRabbitMQ:
		conn, err := publisher.NewConnection(cfg)
		if err != nil {
			return nil, fmt.Errorf("new connection: %w", err)
		}

		p, err := publisher.NewPublisher(cfg.Topic, conn)
		if err != nil {
			return nil, fmt.Errorf("new publisher: %w", err)
		}

		pub, err := publisher.NewRabbitPublisher(cfg.Topic, conn, p)
		if err != nil {
			return nil, fmt.Errorf("new rabbit publisher: %w", err)
		}

		return pub, nil
	case apis.PublisherTypeGooglePubSub:
		pubSubConn, err := publisher.NewPubSubConnection(ctx, logger, cfg.PubSubProjectID)
		if err != nil {
			return nil, fmt.Errorf("could not create pubsub connection: %w", err)
		}

		return publisher.NewGooglePubSubPublisher(pubSubConn), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
