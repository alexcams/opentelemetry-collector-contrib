// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

type invalidConfig struct{}

func (*invalidConfig) Validate() error { return nil }

func TestCreateProcessor_Errors(t *testing.T) {
	factory := NewFactory()
	ctx := t.Context()
	settings := processortest.NewNopSettings(component.MustNewType(typeStr))

	tests := []struct {
		name        string
		config      component.Config
		expectedErr string
	}{
		{
			name:        "invalid config type",
			config:      &invalidConfig{},
			expectedErr: "invalid config type",
		},
		{
			name: "validation error - metrics and logging disabled",
			config: &Config{
				Label:   "test",
				Metrics: MetricConfig{},
			},
			expectedErr: "requires a metrics exporter to be configured or enable_logging",
		},
	}

	processorTypes := []struct {
		name   string
		create func(context.Context, component.Config) error
	}{
		{
			name: "logs",
			create: func(ctx context.Context, cfg component.Config) error {
				_, err := factory.CreateLogs(ctx, settings, cfg, consumertest.NewNop())
				return err
			},
		},
		{
			name: "traces",
			create: func(ctx context.Context, cfg component.Config) error {
				_, err := factory.CreateTraces(ctx, settings, cfg, consumertest.NewNop())
				return err
			},
		},
		{
			name: "metrics",
			create: func(ctx context.Context, cfg component.Config) error {
				_, err := factory.CreateMetrics(ctx, settings, cfg, consumertest.NewNop())
				return err
			},
		},
	}

	for _, pt := range processorTypes {
		t.Run(pt.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					err := pt.create(ctx, tt.config)
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.expectedErr)
				})
			}
		})
	}
}
