// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectedErr string
	}{
		{
			name: "valid config with all metrics enabled",
			config: Config{
				Label: "test-processor",
				Metrics: MetricConfig{
					PayloadSize:   true,
					ErrorCount:    true,
					SuccessRate:   true,
					Throughput:    true,
					EnableLogging: true,
				},
			},
			expectedErr: "",
		},
		{
			name: "valid config with single metric",
			config: Config{
				Label: "test-processor",
				Metrics: MetricConfig{
					PayloadSize: true,
				},
			},
			expectedErr: "",
		},
		{
			name: "valid config with logging only should fail",
			config: Config{
				Label: "test-processor",
				Metrics: MetricConfig{
					EnableLogging: true,
				},
			},
			expectedErr: "at least one metric must be enabled",
		},
		{
			name: "empty label",
			config: Config{
				Label: "",
				Metrics: MetricConfig{
					PayloadSize: true,
				},
			},
			expectedErr: "label must be specified to identify the processor being measured",
		},
		{
			name: "no metrics enabled",
			config: Config{
				Label: "test-processor",
			},
			expectedErr: "at least one metric must be enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}
