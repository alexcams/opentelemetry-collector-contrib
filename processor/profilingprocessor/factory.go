// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profilingprocessor // import "go.opentelemetry.io/opentelemetry-collector-contrib/processor/profilingprocessor"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	typeStr   = "profiling"
	stability = component.StabilityLevelAlpha
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, stability),
		processor.WithTraces(createTracesProcessor, stability),
		processor.WithMetrics(createMetricsProcessor, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Label: "next_processor",
		Metrics: MetricConfig{
			PayloadSize:   true,
			ErrorCount:    false,
			SuccessRate:   false,
			Throughput:    false,
			EnableLogging: false,
		},
	}
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	procConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid config type for *profilingprocessor")
	}

	profProc, err := newProfilingProcessor(procConfig, set)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewLogs(
		ctx,
		set,
		cfg,
		newWrappedLogsConsumer(nextConsumer, profProc),
		profProc.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(profProc.start),
		processorhelper.WithShutdown(profProc.shutdown),
	)
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	procConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid config type for *profilingprocessor")
	}

	profProc, err := newProfilingProcessor(procConfig, set)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		newWrappedTracesConsumer(nextConsumer, profProc),
		profProc.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(profProc.start),
		processorhelper.WithShutdown(profProc.shutdown),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	procConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid config type for *profilingprocessor")
	}

	profProc, err := newProfilingProcessor(procConfig, set)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		newWrappedMetricsConsumer(nextConsumer, profProc),
		profProc.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(profProc.start),
		processorhelper.WithShutdown(profProc.shutdown),
	)
}
