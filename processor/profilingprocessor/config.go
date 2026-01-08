// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/profilingprocessor"

import (
	"errors"
)

type Config struct {
	// Label identifies the processor being measured and is included in exported metrics
	Label string `mapstructure:"label"`
	// Metrics configures which metrics to collect and export
	Metrics MetricConfig `mapstructure:"metrics"`
}

type MetricConfig struct {
	SuccessRate   bool `mapstructure:"success_rate"`
	Throughput    bool `mapstructure:"throughput"`
	PayloadSize   bool `mapstructure:"payload_size"`
	ErrorCount    bool `mapstructure:"error_count"`
	EnableLogging bool `mapstructure:"enable_logging"`
}

func (c *Config) Validate() error {
	if c.Label == "" {
		return errors.New("label must be specified to identify the processor being measured")
	}

	return c.Metrics.validate()
}

func (mc *MetricConfig) validate() error {
	if !mc.PayloadSize && !mc.ErrorCount && !mc.SuccessRate && !mc.Throughput {
		return errors.New("at least one metric must be enabled")
	}
	return nil
}
