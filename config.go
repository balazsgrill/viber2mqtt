package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type MQTTConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type ViberConfig struct {
	AuthToken string `yaml:"auth_token"`
	BotName   string `yaml:"bot_name"`
	Webhook   string `yaml:"webhook_url"`
}

type StorageConfig struct {
	Backend string `yaml:"backend"`
	Path    string `yaml:"path"`
	DSN     string `yaml:"dsn"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type Notification struct {
	ID          string `yaml:"id"`
	Topic       string `yaml:"topic"`
	Message     string `yaml:"message"`
	Description string `yaml:"description"`
}

type Action struct {
	ID          string `yaml:"id"`
	Trigger     string `yaml:"trigger"`
	Topic       string `yaml:"topic"`
	Payload     string `yaml:"payload"`
	Description string `yaml:"description"`
}

type Config struct {
	MQTT          MQTTConfig     `yaml:"mqtt"`
	Viber         ViberConfig    `yaml:"viber"`
	Storage       StorageConfig  `yaml:"storage"`
	Server        ServerConfig   `yaml:"server"`
	Notifications []Notification `yaml:"notifications"`
	Actions       []Action       `yaml:"actions"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate() error {
	if c.MQTT.Host == "" {
		return fmt.Errorf("config: mqtt.host is required")
	}
	if c.MQTT.Port == 0 {
		c.MQTT.Port = 1883
	}
	if c.Viber.AuthToken == "" {
		return fmt.Errorf("config: viber.auth_token is required")
	}
	if c.Viber.BotName == "" {
		c.Viber.BotName = "viber2mqtt"
	}
	if c.Viber.Webhook == "" {
		return fmt.Errorf("config: viber.webhook_url is required")
	}
	switch c.Storage.Backend {
	case "json", "sqlite":
		if c.Storage.Path == "" {
			c.Storage.Path = "./data/viber2mqtt"
			if c.Storage.Backend == "sqlite" {
				c.Storage.Path += ".db"
			}
		}
	case "postgres":
		if c.Storage.DSN == "" {
			return fmt.Errorf("config: storage.dsn is required for backend postgres")
		}
	case "":
		return fmt.Errorf("config: storage.backend is required (one of: json, sqlite, postgres)")
	default:
		return fmt.Errorf("config: storage.backend must be one of json, sqlite, postgres (got %q)", c.Storage.Backend)
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	for i, n := range c.Notifications {
		if n.ID == "" {
			return fmt.Errorf("config: notification at index %d has no id", i)
		}
		if n.Topic == "" {
			return fmt.Errorf("config: notification %q has no topic", n.ID)
		}
		if n.Message == "" {
			return fmt.Errorf("config: notification %q has no message", n.ID)
		}
	}
	for i, a := range c.Actions {
		if a.ID == "" {
			return fmt.Errorf("config: action at index %d has no id", i)
		}
		if a.Trigger == "" {
			return fmt.Errorf("config: action at index %d has no trigger", i)
		}
		if strings.HasPrefix(strings.TrimSpace(a.Trigger), "/") {
			return fmt.Errorf("config: action %q trigger %q must not start with /", a.ID, a.Trigger)
		}
		if a.Topic == "" {
			return fmt.Errorf("config: action %q has no topic", a.ID)
		}
		if a.Payload == "" {
			return fmt.Errorf("config: action %q has no payload", a.ID)
		}
	}
	seen := map[string]bool{}
	for _, n := range c.Notifications {
		if seen[n.ID] {
			return fmt.Errorf("config: duplicate notification id %q", n.ID)
		}
		seen[n.ID] = true
	}
	seen = map[string]bool{}
	for _, a := range c.Actions {
		if seen[a.ID] {
			return fmt.Errorf("config: duplicate action id %q", a.ID)
		}
		seen[a.ID] = true
		key := strings.ToLower(strings.TrimSpace(a.Trigger))
		if seen[key] {
			return fmt.Errorf("config: duplicate action trigger %q (case-insensitive)", a.Trigger)
		}
		seen[key] = true
	}
	return nil
}
