package apis

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type PublisherType string

const (
	PublisherTypeNats         PublisherType = "nats"
	PublisherTypeKafka        PublisherType = "kafka"
	PublisherTypeRabbitMQ     PublisherType = "rabbitmq"
	PublisherTypeGooglePubSub PublisherType = "google_pubsub"
)

// Config for pgoutbox.
type Config struct {
	Listener  *ListenerCfg  `validate:"required" json:"listener" mapstructure:"listener"`
	Database  *DatabaseCfg  `validate:"required" json:"database" mapstructure:"database"`
	Publisher *PublisherCfg `validate:"required" json:"publisher" mapstructure:"publisher"`
	Logger    *Logger       `validate:"required" json:"logger" mapstructure:"logger"`
	Telemetry *TelemetryCfg `json:"telemetry" mapstructure:"telemetry"`
}

// TelemetryCfg holds telemetry and metrics configuration.
type TelemetryCfg struct {
	// Enabled enables the Prometheus metrics endpoint at /metrics.
	// Defaults to true if not specified.
	Enabled bool `json:"enabled" mapstructure:"enabled"`
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string            `validate:"required" json:"slotName" mapstructure:"slotName"`
	Failover          bool              `json:"failover" mapstructure:"failover"`
	ServerPort        int               `json:"serverPort" mapstructure:"serverPort"`
	AckTimeout        time.Duration     `json:"ackTimeout" mapstructure:"ackTimeout"`
	RefreshConnection time.Duration     `validate:"required" json:"refreshConnection" mapstructure:"refreshConnection"`
	HeartbeatInterval time.Duration     `validate:"required" json:"heartbeatInterval" mapstructure:"heartbeatInterval"`
	Filter            FilterStruct      `json:"filter" mapstructure:"filter"`
	TopicsMap         map[string]string `json:"topicsMap" mapstructure:"topicsMap"`
}

// PublisherCfg represent configuration for any publisher types.
type PublisherCfg struct {
	Type            PublisherType `validate:"required,oneof=nats kafka rabbitmq google_pubsub" json:"type" mapstructure:"type"`
	Address         string        `validate:"required" json:"address" mapstructure:"address"`
	NatsCredPath    string        `validate:"required_if=Type nats" json:"natsCredPath" mapstructure:"natsCredPath"`
	Topic           string        `validate:"required" json:"topic" mapstructure:"topic"`
	TopicPrefix     string        `json:"topicPrefix" mapstructure:"topicPrefix"`
	EnableTLS       bool          `json:"enableTLS" mapstructure:"enableTlS"`
	ClientCert      string        `validate:"required_if=EnableTLS true" json:"clientCert" mapstructure:"clientCert"`
	ClientKey       string        `validate:"required_if=EnableTLS true" json:"clientKey" mapstructure:"clientKey"`
	CACert          string        `validate:"required_if=EnableTLS true" json:"CACert" mapstructure:"caCert"`
	PubSubProjectID string        `validate:"required_if=Type google_pubsub" json:"pubSubProjectID" mapstructure:"pubSubProductId"`
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	// Host should address the PostgreSQL primary. It may also point at a
	// pgpool-II endpoint: since a pooler cannot proxy the logical replication
	// protocol, pgoutbox then asks pgpool which backend is the primary and
	// connects to that node directly.
	Host     string `validate:"required" json:"host" mapstructure:"host"`
	Port     uint16 `validate:"required" json:"port" mapstructure:"port"`
	Name     string `validate:"required" json:"name" mapstructure:"name"`
	User     string `validate:"required" json:"user" mapstructure:"user"`
	Password string `validate:"required" json:"password" mapstructure:"password"`
	Debug    bool   `json:"debug" mapstructure:"debug"`
}

// FilterStruct incoming WAL message filter.
type FilterStruct struct {
	Tables map[string][]string `json:"tables" yaml:"tables" mapstructure:"tables"`
}

var validate = validator.New()

// Validate config data.
func (c Config) Validate() error {
	return validate.Struct(c)
}

// InitConfig load config from file.
func InitConfig(path string) (*Config, error) {
	const envPrefix = "WAL"

	var conf Config

	vp := viper.New()

	vp.SetEnvPrefix(envPrefix)
	vp.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	vp.AutomaticEnv()
	vp.SetConfigFile(path)

	if err := vp.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	if err := vp.Unmarshal(&conf); err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &conf, nil
}
