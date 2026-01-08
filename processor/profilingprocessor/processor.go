// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor // import "go.opentelemetry.io/opentelemetry-collector-contrib/processor/profilingprocessor"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/profilingprocessor/internal/metadata"
)

const (
	attrLabel          = "label"
	attrErrorType      = "error_type"
	attrPayloadBytes   = "payload_bytes"
	attrSuccessRate    = "success_rate"
	attrThroughputKbps = "throughput_kb_per_sec"
	errorTypePermanent = "permanent"
	errorTypeTransient = "transient"
)

type profilingProcessor struct {
	logger          *zap.Logger
	config          *Config
	telemetry       *metadata.TelemetryBuilder
	logsMarshaler   *plog.ProtoMarshaler
	traceMarshaler  *ptrace.ProtoMarshaler
	metricMarshaler *pmetric.ProtoMarshaler
	records         processorRecords
}

type processorRecords struct {
	mu             sync.Mutex
	ticker         *time.Ticker
	executionCount int64
	errorCount     int64
	payloadSizeSum int64
	done           chan struct{}
}

func newProfilingProcessor(cfg *Config, set processor.Settings) (*profilingProcessor, error) {
	// Check if MeterProvider is no-op - profiling processor is useless without metrics
	if _, isNoop := set.MeterProvider.(noop.MeterProvider); isNoop && !cfg.Metrics.EnableLogging {
		return nil, errors.New("profiling processor requires a metrics exporter to be configured or enable_logging to be true")
	}

	telemetry, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry builder: %w", err)
	}
	return &profilingProcessor{
		logger:          set.Logger,
		config:          cfg,
		telemetry:       telemetry,
		logsMarshaler:   &plog.ProtoMarshaler{},
		traceMarshaler:  &ptrace.ProtoMarshaler{},
		metricMarshaler: &pmetric.ProtoMarshaler{},
	}, nil
}

func (p *profilingProcessor) start(ctx context.Context, _ component.Host) error {
	if p.config.Metrics.Throughput {
		p.records.ticker = time.NewTicker(time.Second)
		p.records.done = make(chan struct{})
		go p.exportThroughputPeriodically(ctx)
	}
	return nil
}

func (p *profilingProcessor) shutdown(_ context.Context) error {
	if p.config.Metrics.Throughput {
		close(p.records.done)
		p.records.ticker.Stop()
	}
	return nil
}

func (*profilingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *profilingProcessor) needsPayloadSize() bool {
	return p.config.Metrics.PayloadSize || p.config.Metrics.Throughput
}

// process functions are pass-through functions - actual measurement happens in wrapped consumer
func (*profilingProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	return ld, nil
}

func (*profilingProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return td, nil
}

func (*profilingProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return md, nil
}

func (p *profilingProcessor) recordMetrics(ctx context.Context, payloadBytes int, err error) {
	successRate := p.calculateMetrics(payloadBytes, err)
	p.exportMetrics(ctx, payloadBytes, successRate, err)
	if p.config.Metrics.EnableLogging {
		p.logMetrics(payloadBytes, successRate, err)
	}
}

func (p *profilingProcessor) calculateMetrics(payloadBytes int, err error) float64 {
	p.records.mu.Lock()
	defer p.records.mu.Unlock()

	p.records.executionCount++
	if err != nil {
		p.records.errorCount++
	}

	var successRate float64
	if p.config.Metrics.SuccessRate {
		successRate = float64(p.records.executionCount-p.records.errorCount) / float64(p.records.executionCount) * 100
	}

	if p.config.Metrics.Throughput {
		p.records.payloadSizeSum += int64(payloadBytes)
	}

	return successRate
}

func (p *profilingProcessor) exportMetrics(ctx context.Context, payloadBytes int, successRate float64, err error) {
	labelAttr := attribute.String(attrLabel, p.config.Label)

	if p.config.Metrics.SuccessRate {
		p.telemetry.ProcessorProfilingSuccessRate.Record(ctx, successRate, metric.WithAttributes(labelAttr))
	}

	if p.config.Metrics.PayloadSize {
		p.telemetry.ProcessorProfilingPayloadSize.Record(ctx, int64(payloadBytes), metric.WithAttributes(labelAttr))
	}

	if err != nil && p.config.Metrics.ErrorCount {
		errorType := errorTypeTransient
		if consumererror.IsPermanent(err) {
			errorType = errorTypePermanent
		}
		p.telemetry.ProcessorProfilingErrorCount.Add(ctx, 1, metric.WithAttributes(
			labelAttr,
			attribute.String(attrErrorType, errorType)))
	}
}

func (p *profilingProcessor) logMetrics(payloadBytes int, successRate float64, err error) {
	logFields := []zap.Field{zap.String(attrLabel, p.config.Label)}

	if p.config.Metrics.PayloadSize {
		logFields = append(logFields, zap.Int(attrPayloadBytes, payloadBytes))
	}

	if p.config.Metrics.SuccessRate {
		logFields = append(logFields, zap.Float64(attrSuccessRate, successRate))
	}

	if err != nil && p.config.Metrics.ErrorCount {
		logFields = append(logFields, zap.Error(err))
	}

	p.logger.Info("profiling metrics", logFields...)
}

func (p *profilingProcessor) exportThroughput(ctx context.Context) {
	p.records.mu.Lock()
	payloadSize := p.records.payloadSizeSum
	p.records.payloadSizeSum = 0
	p.records.mu.Unlock()

	throughput := float64(payloadSize) / 1024.0

	p.telemetry.ProcessorProfilingThroughput.Record(ctx, throughput, metric.WithAttributes(
		attribute.String(attrLabel, p.config.Label)))

	if p.config.Metrics.EnableLogging && throughput > 0 {
		p.logger.Info("profiling metrics", zap.String(attrLabel, p.config.Label), zap.Float64(attrThroughputKbps, throughput))
	}
}

func (p *profilingProcessor) exportThroughputPeriodically(ctx context.Context) {
	for {
		select {
		case <-p.records.ticker.C:
			p.exportThroughput(ctx)
		case <-p.records.done:
			return
		}
	}
}

// wrappedConsumer wraps the next consumer to measure its execution
type wrappedConsumer struct {
	nextLogs    consumer.Logs
	nextTraces  consumer.Traces
	nextMetrics consumer.Metrics
	profProc    *profilingProcessor
}

func newWrappedLogsConsumer(next consumer.Logs, profProc *profilingProcessor) *wrappedConsumer {
	return &wrappedConsumer{
		nextLogs: next,
		profProc: profProc,
	}
}

func newWrappedTracesConsumer(next consumer.Traces, profProc *profilingProcessor) *wrappedConsumer {
	return &wrappedConsumer{
		nextTraces: next,
		profProc:   profProc,
	}
}

func newWrappedMetricsConsumer(next consumer.Metrics, profProc *profilingProcessor) *wrappedConsumer {
	return &wrappedConsumer{
		nextMetrics: next,
		profProc:    profProc,
	}
}

func (w *wrappedConsumer) getPayloadSize(sizeFunc func() int) int {
	if w.profProc.needsPayloadSize() {
		return sizeFunc()
	}
	return 0
}

func (w *wrappedConsumer) measureExecution(ctx context.Context, payloadBytes int, fn func() error) error {
	err := fn()
	w.profProc.recordMetrics(ctx, payloadBytes, err)
	return err
}

func (w *wrappedConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return w.measureExecution(ctx, w.getPayloadSize(func() int { return w.profProc.metricMarshaler.MetricsSize(md) }), func() error {
		return w.nextMetrics.ConsumeMetrics(ctx, md)
	})
}

func (w *wrappedConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return w.measureExecution(ctx, w.getPayloadSize(func() int { return w.profProc.logsMarshaler.LogsSize(ld) }), func() error {
		return w.nextLogs.ConsumeLogs(ctx, ld)
	})
}

func (w *wrappedConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return w.measureExecution(ctx, w.getPayloadSize(func() int { return w.profProc.traceMarshaler.TracesSize(td) }), func() error {
		return w.nextTraces.ConsumeTraces(ctx, td)
	})
}

func (w *wrappedConsumer) Capabilities() consumer.Capabilities {
	if w.nextLogs != nil {
		return w.nextLogs.Capabilities()
	}
	if w.nextTraces != nil {
		return w.nextTraces.Capabilities()
	}
	if w.nextMetrics != nil {
		return w.nextMetrics.Capabilities()
	}
	return consumer.Capabilities{MutatesData: false}
}
