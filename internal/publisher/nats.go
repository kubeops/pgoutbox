package publisher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"kubeops.dev/pgoutbox/utils"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	logger *slog.Logger
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(conn *nats.Conn, logger *slog.Logger) (*NatsPublisher, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jet stream: %w", err)
	}

	return &NatsPublisher{conn: conn, js: js, logger: logger}, nil
}

// Close connection.
func (n NatsPublisher) Close() error {
	n.conn.Close()
	return nil
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(_ context.Context, subject string, event *apis.Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}

	if _, err := n.js.Publish(subject, msg); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

// WaitForStreamToBeCreated polls every 2 seconds until the stream with the given name is found.
// It returns an error if the context is canceled or times out.
func (n NatsPublisher) WaitForStreamToBeCreated(ctx context.Context, streamName string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Error("context canceled while waiting for stream to be created", "stream", streamName)
			return ctx.Err()
		case <-ticker.C:
			stream, err := n.js.StreamInfo(streamName)
			if err != nil {
				n.logger.Warn("failed to get stream info", "stream", streamName, "error", err)
				continue
			}
			if stream != nil {
				n.logger.Info("stream exists", "stream", streamName)
				return nil
			}
			n.logger.Info("waiting for stream to be created", "stream", streamName)
		}
	}
}
