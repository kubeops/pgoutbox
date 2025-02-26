package apis

import (
	"fmt"
	"strings"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/spf13/viper"

	cfg "github.com/ihippik/config"
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
	Listener   *ListenerCfg   `valid:"required" mapstructure:"listener"`
	Database   *DatabaseCfg   `valid:"required" mapstructure:"database"`
	Publisher  *PublisherCfg  `valid:"required" mapstructure:"publisher"`
	Logger     *cfg.Logger    `valid:"required" mapstructure:"logger"`
	Monitoring cfg.Monitoring `valid:"required" mapstructure:"monitoring"`
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string            `valid:"required" mapstructure:"slotName"`
	ServerPort        int               `mapstructure:"serverPort"`
	AckTimeout        time.Duration     `mapstructure:"ackTimeout"`
	RefreshConnection time.Duration     `valid:"required" mapstructure:"refreshConnection"`
	HeartbeatInterval time.Duration     `valid:"required" mapstructure:"heartbeatInterval"`
	Filter            FilterStruct      `mapstructure:"filter"`
	TopicsMap         map[string]string `mapstructure:"topicsMap"`
}

// PublisherCfg represent configuration for any publisher types.
type PublisherCfg struct {
	Type                    PublisherType `valid:"required" mapstructure:"type"`
	Address                 string        `valid:"required" mapstructure:"address"`
	NatsAdminCredentialPath string        `valid:"required" mapstructure:"natsAdminCredPath"`
	Topic                   string        `valid:"required" mapstructure:"topic"`
	TopicPrefix             string        `mapstructure:"topicPrefix"`
	EnableTLS               bool          `mapstructure:"enableTlS"`
	ClientCert              string        `mapstructure:"clientCert"`
	ClientKey               string        `mapstructure:"clientKey"`
	CACert                  string        `mapstructure:"caCert"`
	PubSubProjectID         string        `mapstructure:"pubSubProductId"`
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required" mapstructure:"host"`
	Port     uint16 `valid:"required" mapstructure:"port"`
	Name     string `valid:"required" mapstructure:"name"`
	User     string `valid:"required" mapstructure:"user"`
	Password string `valid:"required" mapstructure:"password"`
	Debug    bool   `mapstructure:"debug"`
}

// FilterStruct incoming WAL message filter.
type FilterStruct struct {
	Tables map[string][]string
}

// Validate config data.
func (c Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
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
