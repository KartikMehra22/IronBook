// Package config loads fairness-gateway env.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// Config holds the gateway's runtime parameters.
type Config struct {
	HTTPAddr       string `envconfig:"HTTP_ADDR" default:":8080"`
	TimeService    string `envconfig:"TIME_SERVICE" default:"time-service.ironbook.svc.cluster.local:7070"`
	SubmissionEnd  string `envconfig:"SUBMISSION_ENDPOINT" required:"true"`
	OracleEnd      string `envconfig:"ORACLE_ENDPOINT" required:"true"`
	StampBatchSize uint32 `envconfig:"STAMP_BATCH_SIZE" default:"10000"`
	EventLogPath   string `envconfig:"EVENT_LOG_PATH" default:"/var/log/ironbook/events.jsonl"`
	RunSecret      string `envconfig:"RUN_SECRET" default:""`
}

// Load reads env vars (IRONBOOK_*) and fills any missing run secret with a
// fresh random 32-byte value.
func Load() (Config, error) {
	var c Config
	if err := envconfig.Process("ironbook", &c); err != nil {
		return c, err
	}
	if c.RunSecret == "" {
		var buf [32]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return c, fmt.Errorf("generate run secret: %w", err)
		}
		c.RunSecret = hex.EncodeToString(buf[:])
	}
	return c, nil
}
