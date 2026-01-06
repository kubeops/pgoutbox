package apis

import (
	"errors"
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
					Type:         "kafka",
					Address:      "addr",
					Topic:        "stream",
					TopicPrefix:  "prefix",
					NatsCredPath: "/etc/nats/creds/admin.creds",
				},
			},
			wantErr: nil,
		},
		{
			name: "bad listener cfg",
			fields: fields{
				Logger: &Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "",
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
					Type:         "kafka",
					Address:      "addr",
					Topic:        "stream",
					TopicPrefix:  "prefix",
					NatsCredPath: "/etc/nats/creds/admin.creds",
				},
			},
			wantErr: errors.New("Listener.refreshConnection: non zero value required;Listener.slotName: non zero value required"),
		},
		{
			name: "bad db cfg",
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
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:         "kafka",
					Address:      "addr",
					Topic:        "stream",
					TopicPrefix:  "prefix",
					NatsCredPath: "/etc/nats/creds/admin.creds",
				},
			},
			wantErr: errors.New("Database.host: non zero value required;Database.port: non zero value required"),
		},
		{
			name: "empty publisher kind",
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
					Topic:        "stream",
					TopicPrefix:  "prefix",
					NatsCredPath: "/etc/nats/creds/admin.creds",
				},
			},
			wantErr: errors.New("Publisher.address: non zero value required;Publisher.type: non zero value required"),
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
			if err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Nil(t, tt.wantErr)
			}
		})
	}
}
