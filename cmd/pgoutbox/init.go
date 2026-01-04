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
	"fmt"
	"log/slog"
	"time"

	"kubeops.dev/pgoutbox/apis"
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

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *apis.DatabaseCfg, logger *slog.Logger, timeout time.Duration) (*pgx.Conn, *pgconn.PgConn, error) {
	var pgConn *pgx.Conn
	var pgReplConn *pgconn.PgConn

	connString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		cfg.Host, cfg.Port, cfg.Name, cfg.User, cfg.Password)
	replConnString := connString + " replication=database"

	err := wait.PollUntilContextTimeout(context.TODO(), 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var err error
		pgConn, err = pgx.Connect(ctx, connString)
		if err != nil {
			logger.Error("db connection:", slog.String("error", err.Error()))
			return false, nil
		}

		pgReplConn, err = pgconn.Connect(ctx, replConnString)
		if err != nil {
			logger.Error("db replication connection:", slog.String("error", err.Error()))
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("wait for db connection: %w", err)
	}

	return pgConn, pgReplConn, nil
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
