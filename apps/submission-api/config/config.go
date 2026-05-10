// Package config loads submission-api environment.
package config

import "github.com/kelseyhightower/envconfig"

// Config holds runtime parameters supplied via env vars (IRONBOOK_*).
type Config struct {
	HTTPAddr       string `envconfig:"HTTP_ADDR" default:":8080"`
	GRPCAddr       string `envconfig:"GRPC_ADDR" default:":9090"`
	PostgresDSN    string `envconfig:"POSTGRES_DSN" required:"true"`
	MinIOEndpoint  string `envconfig:"MINIO_ENDPOINT" required:"true"`
	MinIOAccessKey string `envconfig:"MINIO_ACCESS_KEY" required:"true"`
	MinIOSecretKey string `envconfig:"MINIO_SECRET_KEY" required:"true"`
	MinIOBucket    string `envconfig:"MINIO_BUCKET" default:"submissions"`
	MinIOUseSSL    bool   `envconfig:"MINIO_USE_SSL" default:"false"`
}

// Load reads env vars (IRONBOOK_*) into a Config.
func Load() (Config, error) {
	var c Config
	err := envconfig.Process("ironbook", &c)
	return c, err
}
