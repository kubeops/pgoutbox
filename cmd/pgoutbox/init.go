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

	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/util/wait"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *apis.DatabaseCfg, logger *slog.Logger, timeout time.Duration) (*pgx.Conn, *pgx.ReplicationConn, error) {
	var pgConn *pgx.Conn
	var pgReplicationConn *pgx.ReplicationConn

	pgxConf := pgx.ConnConfig{
		LogLevel: pgx.LogLevelInfo,
		Logger:   pgxLogger{logger},
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
	}

	err := wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var err error
		pgConn, err = pgx.Connect(pgxConf)
		if err != nil {
			return false, fmt.Errorf("db connection: %w", err)
		}

		pgReplicationConn, err = pgx.ReplicationConnect(pgxConf)
		if err != nil {
			return false, fmt.Errorf("replication connect: %w", err)
		}

		return true, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("wait for db connection: %w", err)
	}

	return pgConn, pgReplicationConn, nil
}

func configureReplicaIdentityToFull(pgConn *pgx.Conn, filterTables apis.FilterStruct) error {
	for table := range filterTables.Tables {
		_, err := pgConn.Exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", table))
		if err != nil {
			return fmt.Errorf("change replica identity to FULL for table %s: %w", table, err)
		}
	}

	return nil
}

type pgxLogger struct {
	logger *slog.Logger
}

// Log DB message.
func (l pgxLogger) Log(_ pgx.LogLevel, msg string, _ map[string]any) {
	l.logger.Debug(msg)
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
		conn, err := nats.Connect(cfg.Address, nats.UserCredentials(cfg.NatsAdminCredentialPath))
		if err != nil {
			return nil, fmt.Errorf("nats connection: %w", err)
		}

		pub, err := publisher.NewNatsPublisher(conn, logger)
		if err != nil {
			return nil, fmt.Errorf("new nats publisher: %w", err)
		}

		if err := pub.CreateStream(cfg.Topic); err != nil {
			return nil, fmt.Errorf("create stream: %w", err)
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
