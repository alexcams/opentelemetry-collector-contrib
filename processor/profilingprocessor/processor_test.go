// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func setupTest(t *testing.T) (processor.Settings, *sdkmetric.ManualReader, context.Context) {
	set := processortest.NewNopSettings(component.MustNewType(typeStr))
	reader := sdkmetric.NewManualReader()
	set.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	ctx := t.Context()
	return set, reader, ctx
}

func collectAndFindMetric(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader, metricName string) metricdata.Metrics {
	var rm metricdata.ResourceMetrics
	err := reader.Collect(ctx, &rm)
	require.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)

	metric := rm.ScopeMetrics[0].Metrics[0]
	require.Equal(t, metricName, metric.Name)
	return metric
}

func TestPayloadSizeMetric(t *testing.T) {
	tests := []struct {
		name         string
		generateLogs func() plog.Logs
	}{
		{
			name: "single small log",
			generateLogs: func() plog.Logs {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				sl := rl.ScopeLogs().AppendEmpty()
				log := sl.LogRecords().AppendEmpty()
				log.Body().SetStr("test")
				return ld
			},
		},
		{
			name: "multiple logs",
			generateLogs: func() plog.Logs {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				sl := rl.ScopeLogs().AppendEmpty()
				for i := range 10 {
					log := sl.LogRecords().AppendEmpty()
					log.Body().SetStr("test message" + strconv.Itoa(i))
				}
				return ld
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Label: "test_processor",
				Metrics: MetricConfig{
					PayloadSize: true,
				},
			}

			set, reader, ctx := setupTest(t)

			profProc, err := newProfilingProcessor(cfg, set)
			require.NoError(t, err)

			wrappedConsumer := newWrappedLogsConsumer(consumertest.NewNop(), profProc)

			ld := tt.generateLogs()
			marshaler := &plog.ProtoMarshaler{}
			expectedSize := marshaler.LogsSize(ld)

			err = wrappedConsumer.ConsumeLogs(ctx, ld)
			require.NoError(t, err)

			metric := collectAndFindMetric(ctx, t, reader, "otelcol_processor_profiling_payload_size")

			histogram, ok := metric.Data.(metricdata.Histogram[int64])
			require.True(t, ok, "metric should be a histogram")

			require.Len(t, histogram.DataPoints, 1)

			dataPoint := histogram.DataPoints[0]

			assert.Equal(t, uint64(1), dataPoint.Count)
			assert.Equal(t, int64(expectedSize), dataPoint.Sum)
		})
	}
}

func TestSuccessRateMetric(t *testing.T) {
	tests := []struct {
		name           string
		generateTraces func() ptrace.Traces
		consumeCount   int
		errorCount     int
		expectedRate   float64
	}{
		{
			name: "single trace - 100% success",
			generateTraces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				ss := rs.ScopeSpans().AppendEmpty()
				span := ss.Spans().AppendEmpty()
				span.SetName("test span")
				return td
			},
			consumeCount: 1,
			errorCount:   0,
			expectedRate: 100.0,
		},
		{
			name: "multiple traces - 100% success",
			generateTraces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				ss := rs.ScopeSpans().AppendEmpty()
				for i := range 5 {
					span := ss.Spans().AppendEmpty()
					span.SetName("test span " + strconv.Itoa(i))
				}
				return td
			},
			consumeCount: 3,
			errorCount:   0,
			expectedRate: 100.0,
		},
		{
			name: "partial failures - 60% success",
			generateTraces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				ss := rs.ScopeSpans().AppendEmpty()
				span := ss.Spans().AppendEmpty()
				span.SetName("test span")
				return td
			},
			consumeCount: 5,
			errorCount:   2,
			expectedRate: 60.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Label: "test_processor",
				Metrics: MetricConfig{
					SuccessRate: true,
				},
			}

			set, reader, ctx := setupTest(t)

			var nextConsumer consumer.Traces
			if tt.errorCount > 0 {
				nextConsumer = &errorTracesConsumer{
					successCount: tt.consumeCount - tt.errorCount,
					totalCount:   tt.consumeCount,
				}
			} else {
				nextConsumer = consumertest.NewNop()
			}

			profProc, err := newProfilingProcessor(cfg, set)
			require.NoError(t, err)

			wrappedConsumer := newWrappedTracesConsumer(nextConsumer, profProc)

			td := tt.generateTraces()
			for range tt.consumeCount {
				_ = wrappedConsumer.ConsumeTraces(ctx, td)
			}

			metric := collectAndFindMetric(ctx, t, reader, "otelcol_processor_profiling_success_rate")

			gauge, ok := metric.Data.(metricdata.Gauge[float64])
			require.True(t, ok, "metric should be a gauge")
			require.Len(t, gauge.DataPoints, 1)

			dataPoint := gauge.DataPoints[0]
			assert.Equal(t, tt.expectedRate, dataPoint.Value)
		})
	}
}

func TestErrorCountMetric(t *testing.T) {
	tests := []struct {
		name            string
		generateMetrics func() pmetric.Metrics
		consumeCount    int
		permanentErrors int
		transientErrors int
	}{
		{
			name: "transient errors only",
			generateMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				sm := rm.ScopeMetrics().AppendEmpty()
				m := sm.Metrics().AppendEmpty()
				m.SetName("test_metric")
				return md
			},
			consumeCount:    3,
			transientErrors: 2,
		},
		{
			name: "permanent errors only",
			generateMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				sm := rm.ScopeMetrics().AppendEmpty()
				m := sm.Metrics().AppendEmpty()
				m.SetName("test_metric")
				return md
			},
			consumeCount:    3,
			permanentErrors: 2,
		},
		{
			name: "mixed errors",
			generateMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				sm := rm.ScopeMetrics().AppendEmpty()
				m := sm.Metrics().AppendEmpty()
				m.SetName("test_metric")
				return md
			},
			consumeCount:    5,
			permanentErrors: 2,
			transientErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Label: "test_processor",
				Metrics: MetricConfig{
					ErrorCount: true,
				},
			}

			set, reader, ctx := setupTest(t)

			nextConsumer := &errorMetricsConsumer{
				successCount:    tt.consumeCount - tt.permanentErrors - tt.transientErrors,
				permanentErrors: tt.permanentErrors,
				transientErrors: tt.transientErrors,
			}

			profProc, err := newProfilingProcessor(cfg, set)
			require.NoError(t, err)

			wrappedConsumer := newWrappedMetricsConsumer(nextConsumer, profProc)

			md := tt.generateMetrics()
			for range tt.consumeCount {
				_ = wrappedConsumer.ConsumeMetrics(ctx, md)
			}

			metric := collectAndFindMetric(ctx, t, reader, "otelcol_processor_profiling_error_count")

			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric should be a counter")

			var permanentCount, transientCount int64
			for _, dp := range sum.DataPoints {
				var errorType string
				for _, attr := range dp.Attributes.ToSlice() {
					if string(attr.Key) == "error_type" {
						errorType = attr.Value.AsString()
					}
				}

				switch errorType {
				case "permanent":
					permanentCount = dp.Value
				case "transient":
					transientCount = dp.Value
				}
			}

			assert.Equal(t, int64(tt.permanentErrors), permanentCount, "permanent error count mismatch")
			assert.Equal(t, int64(tt.transientErrors), transientCount, "transient error count mismatch")
		})
	}
}

func TestThroughputMetric(t *testing.T) {
	tests := []struct {
		name         string
		generateLogs func() plog.Logs
		consumeCount int
	}{
		{
			name: "single log batch",
			generateLogs: func() plog.Logs {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				sl := rl.ScopeLogs().AppendEmpty()
				log := sl.LogRecords().AppendEmpty()
				log.Body().SetStr("test log for throughput")
				return ld
			},
			consumeCount: 5,
		},
		{
			name: "multiple log batches",
			generateLogs: func() plog.Logs {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				sl := rl.ScopeLogs().AppendEmpty()
				for i := range 10 {
					log := sl.LogRecords().AppendEmpty()
					log.Body().SetStr("test log " + strconv.Itoa(i))
				}
				return ld
			},
			consumeCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Label: "test_processor",
				Metrics: MetricConfig{
					Throughput: true,
				},
			}

			set, reader, ctx := setupTest(t)

			profProc, err := newProfilingProcessor(cfg, set)
			require.NoError(t, err)

			err = profProc.start(ctx, nil)
			require.NoError(t, err)
			defer func() {
				err = profProc.shutdown(ctx)
				require.NoError(t, err)
			}()

			wrappedConsumer := newWrappedLogsConsumer(consumertest.NewNop(), profProc)

			ld := tt.generateLogs()

			for i := 0; i < tt.consumeCount; i++ {
				err = wrappedConsumer.ConsumeLogs(ctx, ld)
				require.NoError(t, err)
			}
			// Manually trigger throughput to avoid waiting for ticker.
			profProc.exportThroughput(ctx)

			metric := collectAndFindMetric(ctx, t, reader, "otelcol_processor_profiling_throughput")

			gauge, ok := metric.Data.(metricdata.Gauge[float64])
			require.True(t, ok, "metric should be a gauge")
			require.Len(t, gauge.DataPoints, 1)

			dataPoint := gauge.DataPoints[0]
			assert.Greater(t, dataPoint.Value, 0.0, "throughput should be greater than 0")
		})
	}
}

type errorMetricsConsumer struct {
	callCount       int
	successCount    int
	permanentErrors int
	transientErrors int
}

func (e *errorMetricsConsumer) ConsumeMetrics(_ context.Context, _ pmetric.Metrics) error {
	e.callCount++

	switch {
	case e.callCount <= e.successCount:
		return nil
	case e.callCount <= e.successCount+e.permanentErrors:
		return consumererror.NewPermanent(errors.New("permanent error"))
	case e.callCount <= e.successCount+e.permanentErrors+e.transientErrors:
		return errors.New("transient error")
	default:
		return nil
	}
}

func (*errorMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type errorTracesConsumer struct {
	callCount    int
	successCount int
	totalCount   int
}

func (e *errorTracesConsumer) ConsumeTraces(_ context.Context, _ ptrace.Traces) error {
	e.callCount++

	if e.callCount > e.successCount && e.callCount <= e.totalCount {
		return errors.New("simulated error")
	}
	return nil
}

func (*errorTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
