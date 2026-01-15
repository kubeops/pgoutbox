package apis

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		Logger    *Logger
		Listener  *ListenerCfg
		Database  *DatabaseCfg
		Publisher *PublisherCfg
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "success",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: nil,
		},
		{
			name: "bad listener cfg - missing SlotName",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Listener.SlotName' Error:Field validation for 'SlotName' failed on the 'required' tag"),
		},
		{
			name: "bad listener cfg - missing RefreshConnection",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Listener.RefreshConnection' Error:Field validation for 'RefreshConnection' failed on the 'required' tag"),
		},
		{
			name: "bad listener cfg - missing HeartbeatInterval",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Listener.HeartbeatInterval' Error:Field validation for 'HeartbeatInterval' failed on the 'required' tag"),
		},
		{
			name: "bad db cfg - missing Host",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Database.Host' Error:Field validation for 'Host' failed on the 'required' tag"),
		},
		{
			name: "bad db cfg - missing Port",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Database.Port' Error:Field validation for 'Port' failed on the 'required' tag"),
		},
		{
			name: "empty publisher Type",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.Type' Error:Field validation for 'Type' failed on the 'required' tag"),
		},
		{
			name: "empty publisher Address",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.Address' Error:Field validation for 'Address' failed on the 'required' tag"),
		},
		{
			name: "empty publisher Topic",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "addr",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.Topic' Error:Field validation for 'Topic' failed on the 'required' tag"),
		},
		{
			name: "nats publisher missing NatsCredPath",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeNats,
					Address:     "nats://localhost:4222",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.NatsCredPath' Error:Field validation for 'NatsCredPath' failed on the 'required_if' tag"),
		},
		{
			name: "nats publisher with NatsCredPath - success",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:         PublisherTypeNats,
					Address:      "nats://localhost:4222",
					Topic:        "stream",
					TopicPrefix:  "prefix",
					NatsCredPath: "/path/to/creds",
				},
			},
			wantErr: nil,
		},
		{
			name: "google_pubsub publisher missing PubSubProjectID",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeGooglePubSub,
					Address:     "pubsub.googleapis.com",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.PubSubProjectID' Error:Field validation for 'PubSubProjectID' failed on the 'required_if' tag"),
		},
		{
			name: "google_pubsub publisher with PubSubProjectID - success",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:            PublisherTypeGooglePubSub,
					Address:         "pubsub.googleapis.com",
					Topic:           "stream",
					TopicPrefix:     "prefix",
					PubSubProjectID: "my-project-id",
				},
			},
			wantErr: nil,
		},
		{
			name: "publisher with TLS enabled missing ClientCert",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "localhost:9092",
					Topic:       "stream",
					TopicPrefix: "prefix",
					EnableTLS:   true,
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.ClientCert' Error:Field validation for 'ClientCert' failed on the 'required_if' tag"),
		},
		{
			name: "publisher with TLS enabled missing ClientKey",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "localhost:9092",
					Topic:       "stream",
					TopicPrefix: "prefix",
					EnableTLS:   true,
					ClientCert:  "/path/to/client.crt",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.ClientKey' Error:Field validation for 'ClientKey' failed on the 'required_if' tag"),
		},
		{
			name: "publisher with TLS enabled missing CACert",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "localhost:9092",
					Topic:       "stream",
					TopicPrefix: "prefix",
					EnableTLS:   true,
					ClientCert:  "/path/to/client.crt",
					ClientKey:   "/path/to/client.key",
				},
			},
			wantErr: errors.New("Key: 'Config.Publisher.CACert' Error:Field validation for 'CACert' failed on the 'required_if' tag"),
		},
		{
			name: "publisher with TLS enabled and all certs - success",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        PublisherTypeKafka,
					Address:     "localhost:9092",
					Topic:       "stream",
					TopicPrefix: "prefix",
					EnableTLS:   true,
					ClientCert:  "/path/to/client.crt",
					ClientKey:   "/path/to/client.key",
					CACert:      "/path/to/ca.crt",
				},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Logger:    tt.fields.Logger,
				Listener:  tt.fields.Listener,
				Database:  tt.fields.Database,
				Publisher: tt.fields.Publisher,
			}
			err := c.Validate()
			if tt.wantErr != nil {
				if assert.Error(t, err) {
					// Check if error contains the expected message (validator may return multiple errors)
					assert.True(t, strings.Contains(err.Error(), tt.wantErr.Error()),
						"expected error to contain '%s', but got: %v", tt.wantErr.Error(), err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
