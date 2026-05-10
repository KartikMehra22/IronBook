// Package config loads build-runner environment.
package config

import "github.com/kelseyhightower/envconfig"

// Config holds the per-job parameters supplied by the dispatching K8s Job.
type Config struct {
	// Submission metadata
	SubmissionID     string `envconfig:"SUBMISSION_ID" required:"true"`
	SubmissionSha256 string `envconfig:"SUBMISSION_SHA256" required:"true"`   // hex-encoded
	Language         string `envconfig:"SUBMISSION_LANGUAGE" required:"true"` // rust|go|cpp

	// Where to read source from
	MinIOEndpoint  string `envconfig:"MINIO_ENDPOINT" required:"true"`
	MinIOAccessKey string `envconfig:"MINIO_ACCESS_KEY" required:"true"`
	MinIOSecretKey string `envconfig:"MINIO_SECRET_KEY" required:"true"`
	MinIOBucket    string `envconfig:"MINIO_BUCKET" default:"submissions"`
	MinIOUseSSL    bool   `envconfig:"MINIO_USE_SSL" default:"false"`

	// Where to push the resulting image
	Registry string `envconfig:"REGISTRY" default:"registry.ironbook.svc.cluster.local:5000"`

	// Postgres for status updates
	PostgresDSN string `envconfig:"POSTGRES_DSN" required:"true"`

	// Local working dir for source extraction + build
	WorkDir string `envconfig:"WORK_DIR" default:"/work"`
}

// Load reads env vars into a Config.
func Load() (Config, error) {
	var c Config
	err := envconfig.Process("ironbook", &c)
	return c, err
}
