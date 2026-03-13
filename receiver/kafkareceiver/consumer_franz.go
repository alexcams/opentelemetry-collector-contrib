// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

type topicPartition struct {
	topic     string
	partition int32
}

// franzConsumer implements a Kafka consumer using the franz-go client library.
type franzConsumer struct {
	config           *Config
	topics           []string
	excludeTopics    []string
	settings         receiver.Settings
	telemetryBuilder *metadata.TelemetryBuilder
	newConsumeFn     newConsumeMessageFunc
	consumeMessage   consumeMessageFunc

	mu             sync.RWMutex
	started        chan struct{}
	consumerClosed chan struct{}
	closing        chan struct{}

	client      *kgo.Client
	obsrecv     *receiverhelper.ObsReport
	assignments map[topicPartition]*pc
	partWG      sync.WaitGroup // tracks all partition consumer goroutines

	// ---- status reporting ----
	host         component.Host
	stoppingOnce sync.Once
	stoppedOnce  sync.Once
}

// partitionRecords holds a batch of records dispatched to a partition consumer.
type partitionRecords struct {
	records       []*kgo.Record
	highWatermark int64
	topic         string
	partition     int32
}

// pc represents the partition consumer shared information.
type pc struct {
	logger *zap.Logger
	attrs  attribute.Set

	ctx    context.Context
	cancel context.CancelCauseFunc
	// Not safe for concurrent use, this field is never accessed concurrently.
	backOff *backoff.ExponentialBackOff

	// records is a channel used to dispatch fetched records to the
	// partition's long-lived consumer goroutine. The channel is closed
	// when the partition is lost/revoked to signal the goroutine to exit.
	records chan partitionRecords

	// done is closed when the partition's consumer goroutine exits.
	done chan struct{}
}

// wait blocks until the partition's consumer goroutine has exited.
func (p *pc) wait() { <-p.done }

// newFranzKafkaConsumer creates a new franz-go based Kafka consumer
func newFranzKafkaConsumer(
	config *Config,
	set receiver.Settings,
	topics []string,
	excludeTopics []string,
	newConsumeFn newConsumeMessageFunc,
) (*franzConsumer, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &franzConsumer{
		config:           config,
		topics:           topics,
		excludeTopics:    excludeTopics,
		newConsumeFn:     newConsumeFn,
		settings:         set,
		telemetryBuilder: telemetryBuilder,
		started:          make(chan struct{}),
		consumerClosed:   make(chan struct{}),
		closing:          make(chan struct{}),
		assignments:      make(map[topicPartition]*pc),
	}, nil
}

// reportStatus emits a component status event if we have a host.
func (c *franzConsumer) reportStatus(s componentstatus.Status) {
	if c.host == nil {
		return
	}
	componentstatus.ReportStatus(c.host, componentstatus.NewEvent(s))
}

// reportRecoverable reports a recoverable error status event.
func (c *franzConsumer) reportRecoverable(err error) {
	if c.host == nil || err == nil {
		return
	}
	componentstatus.ReportStatus(c.host, componentstatus.NewRecoverableErrorEvent(err))
}

func (c *franzConsumer) Start(ctx context.Context, host component.Host) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closing:
		return errors.New("franz kafka consumer already shut down")
	case <-c.started:
		return errors.New("franz kafka consumer already started")
	default:
		close(c.started)
	}

	// Report "Starting" as soon as Start() is called.
	c.host = host
	c.reportStatus(componentstatus.StatusStarting)

	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	c.obsrecv = obsrecv

	var hooks kgo.Hook = c
	if c.config.Telemetry.Metrics.KafkaReceiverRecordsDelay.Enabled {
		hooks = franzConsumerWithOptionalHooks{c}
	}

	// Create franz-go consumer client
	opts := []kgo.Opt{
		kgo.OnPartitionsAssigned(c.assigned),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
			c.lost(ctx, c.client, m, false)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, _ *kgo.Client, m map[string][]int32) {
			c.lost(ctx, c.client, m, true)
		}),
		kgo.WithHooks(hooks),
	}

	if !c.config.UseLeaderEpoch {
		opts = append(opts, kgo.AdjustFetchOffsetsFn(makeClearLeaderEpochAdjuster()))
	}

	// Create franz-go consumer client
	client, err := kafka.NewFranzConsumerGroup(
		ctx,
		host,
		c.config.ClientConfig,
		c.config.ConsumerConfig,
		c.topics,
		c.excludeTopics,
		c.settings.Logger,
		opts...,
	)
	if err != nil {
		return err
	}
	c.client = client

	cm, err := c.newConsumeFn(host, c.obsrecv, c.telemetryBuilder)
	if err != nil {
		return err
	}
	c.consumeMessage = cm

	go c.consumeLoop(context.Background())
	return nil
}

func (c *franzConsumer) consumeLoop(ctx context.Context) {
	// When the loop exits, report Stopped.
	defer func() {
		c.stoppedOnce.Do(func() { c.reportStatus(componentstatus.StatusStopped) })
		close(c.consumerClosed)
	}()

	for {
		// Consume messages until the ctx is cancelled (the client is closed).
		// NOTE(marclop) we should make the fetch size configurable. It returns
		// all the internally buffered records. This isn't something that's
		// configurable in Sarama, and theoretically the max records to iterate
		// on is a factor of default / max (byte) fetch size.
		if !c.consume(ctx, -1) {
			return
		}
	}
}

// consume consumes a batch of messages from the Kafka topic. This is meant to
// be called in a loop until consume returns false.
func (c *franzConsumer) consume(ctx context.Context, size int) bool {
	fetch := c.client.PollRecords(ctx, size)

	if err := fetch.Err0(); fetch.IsClientClosed() {
		c.settings.Logger.Info("consumer stopped", zap.Error(err))
		return false // Shut down the consumer loop.
	}
	// There's a variety of errors that are returned by fetch.Errors(). We
	// handle the errors that require a client restart above. The rest can
	// simply be logged and keep fetching.
	var hasError bool
	fetch.EachError(func(topic string, partition int32, err error) {
		c.settings.Logger.Error("consumer fetch error", zap.Error(err),
			zap.String("topic", topic),
			zap.Int64("partition", int64(partition)),
		)
		// Report recoverable error while consuming.
		c.reportRecoverable(err)
		if !hasError {
			hasError = true
		}
	})
	if hasError || fetch.Empty() {
		return true // Return right away after errors or empty fetch.
	}

	// Acquire the read lock on each consume to ensure the client is not closed
	// and the assignments map is not modified while consuming. Copy the map
	// to avoid locking for the duration of the consume loop.
	c.mu.RLock()
	assignments := make(map[topicPartition]*pc, len(c.assignments))
	maps.Copy(assignments, c.assignments)
	c.mu.RUnlock()

	// Dispatch records to each partition's consumer goroutine via its channel.
	// This is non-blocking: each partition processes independently.
	fetch.EachPartition(func(p kgo.FetchTopicPartition) {
		count := len(p.Records)
		if count == 0 {
			return // Skip partitions without any records.
		}
		tp := topicPartition{topic: p.Topic, partition: p.Partition}
		assign, ok := assignments[tp]
		// NOTE(marclop): This could happen if the partition is lost between
		// the time the assignments map is copied and the partition is accessed.
		if !ok {
			c.settings.Logger.Warn(
				"attempted to process records for a partition not assigned to this consumer",
				zap.String("topic", tp.topic),
				zap.Int64("partition", int64(tp.partition)),
			)
			return
		}
		assign.logger.Debug("dispatching fetched records to partition consumer",
			zap.Int("count", count),
			zap.Int64("start_offset", p.Records[0].Offset),
			zap.Int64("end_offset", p.Records[count-1].Offset),
		)
		// Send records to the partition's channel. If the partition has
		// been lost (context cancelled / channel closed), this select
		// will fall through without blocking.
		select {
		case assign.records <- partitionRecords{
			records:       p.Records,
			highWatermark: p.HighWatermark,
			topic:         p.Topic,
			partition:     p.Partition,
		}:
		case <-assign.ctx.Done():
			assign.logger.Debug("partition context done, skipping dispatch")
		}
	})
	return true
}

func (c *franzConsumer) Shutdown(ctx context.Context) error {
	// Report Stopping at shutdown start.
	c.stoppingOnce.Do(func() { c.reportStatus(componentstatus.StatusStopping) })

	if !c.triggerShutdown() {
		// Idempotent: never fail if not started.
		// We still want to ensure Stopped is eventually emitted (consumeLoop defer handles it).
		// However, if the loop was never started, emit Stopped here too.
		c.stoppedOnce.Do(func() { c.reportStatus(componentstatus.StatusStopped) })
		return nil
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.consumerClosed:
	}

	// Wait for all partition consumer goroutines to exit. At this point,
	// the client has been closed and all partition contexts have been
	// cancelled via the lost/revoked callbacks, so goroutines will drain
	// and exit.
	c.partWG.Wait()

	return nil
}

// If it returns false, the caller should return immediately.
func (c *franzConsumer) triggerShutdown() bool {
	c.mu.Lock()
	select {
	case <-c.started:
	default: // Return immediately if the receiver hasn't started.
		c.mu.Unlock()
		return false
	}
	select {
	case <-c.closing:
		c.mu.Unlock()
		return true
	default:
		close(c.closing)
		client := c.client
		c.mu.Unlock()
		// Close the client without holding the write mutex, otherwise, the
		// Shutdown will deadlock when `franzConsumer` inevitably calls the
		// lost/assigned callback.
		if client != nil {
			client.Close()
		}
	}
	return true
}

// partitionConsumeLoop is a long-lived goroutine that processes records for a
// single partition. It reads batches from the partition's channel and processes
// each record sequentially within the partition. The goroutine exits when the
// context is cancelled (partition lost/revoked) or the channel is closed
// (during shutdown cleanup).
func (c *franzConsumer) partitionConsumeLoop(p *pc) {
	defer c.partWG.Done()
	defer close(p.done)
	for {
		select {
		case batch, ok := <-p.records:
			if !ok {
				return // Channel closed, exit the goroutine.
			}
			c.processPartitionBatch(p, batch)
		case <-p.ctx.Done():
			return
		}
	}
}

// processPartitionBatch processes a batch of records for a single partition.
func (c *franzConsumer) processPartitionBatch(p *pc, batch partitionRecords) {
	fatalOffset := int64(-1)
	var lastProcessed *kgo.Record
	for _, msg := range batch.records {
		if !c.config.MessageMarking.After {
			c.client.MarkCommitRecords(msg)
		}
		c.telemetryBuilder.KafkaReceiverCurrentOffset.Record(p.ctx, msg.Offset, metric.WithAttributeSet(p.attrs))
		if err := c.handleMessage(p, wrapFranzMsg(msg)); err != nil {
			p.logger.Error("unable to process message",
				zap.Error(err),
				zap.Int64("offset", msg.Offset),
			)
			// Pause consumption for partitions that have fatal errors,
			// which isn't ideal since there needs to be some sort of manual
			// intervention to unlock the partition.
			isPermanent := consumererror.IsPermanent(err)
			shouldMark := (!isPermanent && c.config.MessageMarking.OnError) || (isPermanent && c.config.MessageMarking.OnPermanentError)

			if !shouldMark {
				fatalOffset = msg.Offset
				break
			}
		}
		lastProcessed = msg
	}
	// Pause topic/partition processing locally, any rebalances that move
	// away the process the partition regularly, which will re-process
	// the message.
	if fatalOffset > -1 {
		c.client.PauseFetchPartitions(map[string][]int32{
			batch.topic: {batch.partition},
		})
		// We don't return false since we want to avoid shutting down
		// the consumer loop and consumption due to message poisoning.
		// If we did, we would cause an eventual systematic failure if
		// there are more topic / partitions in this consumer group when
		// the partition is rebalanced to another consumer in the group.
		//
		// Ideally, we would attempt to re-process permanent errors
		// for up to N times and then pause processing, or even better,
		// produce the message to a dead letter topic.
		p.logger.Error("unable to process message: pausing consumption of this topic / partition on this consumer instance due to message_marking configuration",
			zap.Int64("offset", fatalOffset),
		)
	}
	if lastProcessed == nil {
		return // No metrics nor marks to update.
	}
	c.telemetryBuilder.KafkaReceiverOffsetLag.Record(
		context.Background(),
		(batch.highWatermark-1)-(lastProcessed.Offset),
		metric.WithAttributeSet(p.attrs),
	)
	if c.config.MessageMarking.After {
		c.client.MarkCommitRecords(lastProcessed)
	}
	if !c.config.AutoCommit.Enable {
		if err := c.client.CommitMarkedOffsets(p.ctx); err != nil {
			c.settings.Logger.Error("failed to commit offsets", zap.Error(err))
			c.reportRecoverable(err)
		}
	}
}

// assigned must be set as kgo.OnPartitionsAssigned callback. Ensuring all
// assigned partitions to this consumer process received records.
func (c *franzConsumer) assigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	// Report OK on each successful assignment so we can recover status after transient errors.
	c.reportStatus(componentstatus.StatusOK)

	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.telemetryBuilder.KafkaReceiverPartitionStart.Add(context.Background(), 1)
			partitionConsumer := pc{
				backOff: newExponentialBackOff(c.config.ErrorBackOff),
				logger: c.settings.Logger.With(
					zap.String("topic", topic),
					zap.Int64("partition", int64(partition)),
				),
				attrs: attribute.NewSet(
					attribute.String("topic", topic),
					attribute.Int64("partition", int64(partition)),
				),
			}
			partitionConsumer.ctx, partitionConsumer.cancel = context.WithCancelCause(ctx)
			partitionConsumer.records = make(chan partitionRecords, 1)
			partitionConsumer.done = make(chan struct{})
			c.assignments[topicPartition{topic: topic, partition: partition}] = &partitionConsumer
			// Start a long-lived goroutine for this partition.
			c.partWG.Add(1)
			go c.partitionConsumeLoop(&partitionConsumer)
		}
	}
}

// lost must be set both on kgo.OnPartitionsLost and kgo.OnPartitionsReassigned
// callbacks. Ensures that partitions that are lost (see kgo.OnPartitionsLost
// for more details) or reassigned (see kgo.OnPartitionsReassigned for more
// details) have their partition consumer stopped.
// This callback must finish within the re-balance timeout.
func (c *franzConsumer) lost(ctx context.Context, _ *kgo.Client,
	lost map[string][]int32, fatal bool,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var wg sync.WaitGroup
	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			// In some cases, it is possible for the `lost` to be called with
			// no assignments. So, check if assignments exists first.
			//
			// - OnPartitionLost can be called without the group ever joining
			// and getting assigned.
			// - OnPartitionRevoked can be called multiple times for cooperative
			// balancer on topic lost/deleted.
			if pc, ok := c.assignments[tp]; ok {
				delete(c.assignments, tp)
				// Cancel the context to signal the partition's goroutine to stop.
				// We do NOT close the channel here because consume() may still
				// hold a reference to this pc and attempt to send on it.
				pc.cancel(errors.New(
					"stopping processing: partition reassigned or lost",
				))
				wg.Go(func() {
					pc.wait()
				})
				c.telemetryBuilder.KafkaReceiverPartitionClose.Add(context.Background(), 1)
			}
		}
	}
	if fatal {
		return
	}
	// Wait for all partition consumers to exit before committing marked offsets.
	wg.Wait()
	// NOTE(marclop) commit the marked offsets when the partition is rebalanced
	// away from this consumer.
	if err := c.client.CommitMarkedOffsets(ctx); err != nil {
		c.settings.Logger.Error("failed to commit marked offsets", zap.Error(err))
		// Report recoverable error on commit errors.
		c.reportRecoverable(err)
	}
}

// handleMessage is called on a per-partition basis.
func (c *franzConsumer) handleMessage(pc *pc, msg kafkaMessage) error {
	if pc.backOff != nil {
		defer pc.backOff.Reset()
	}

	for {
		err := c.consumeMessage(pc.ctx, msg, pc.attrs)
		if err == nil {
			return nil // Successfully processed.
		}
		// In the future, with Consumer Share Groups, messages not processed
		// within a configurable timeout, are re-delivered to the consumer in
		// the Share group, however, at the time of writing this feature isn't
		// yet GA nor widely deployed.
		// Share groups are generally more in line with observability and OTel
		// collector use cases than traditional consumer groups.
		// https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka.
		// One possible exception is if the OTel collector is used for analytics
		// pipelines, where it may make sense to make share groups opt-in.
		if pc.backOff != nil && !consumererror.IsPermanent(err) {
			backOffDelay := pc.backOff.NextBackOff()
			if backOffDelay != backoff.Stop {
				pc.logger.Info("Backing off due to error from the next consumer.",
					zap.Error(err),
					zap.Duration("delay", backOffDelay),
				)
				select {
				case <-pc.ctx.Done():
					return context.Cause(pc.ctx)
				case <-time.After(backOffDelay):
					continue
				}
			}
			pc.logger.Warn("Stop error backoff because the configured max_elapsed_time is reached",
				zap.Duration("max_elapsed_time", pc.backOff.MaxElapsedTime),
			)
		}

		isPermanent := consumererror.IsPermanent(err)
		shouldMark := (!isPermanent && c.config.MessageMarking.OnError) || (isPermanent && c.config.MessageMarking.OnPermanentError)

		if c.config.MessageMarking.After && !shouldMark {
			// Only return an error if messages are marked after successful processing.
			return err
		}
		pc.logger.Error("failed to consume message, skipping due to message_marking config",
			zap.Error(err),
			zap.Int64("offset", msg.offset()),
		)
		return nil
	}
}

// The methods below implement the relevant franz-go hook interfaces
// record the metrics defined in the metadata telemetry.

func (c *franzConsumer) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node_id", kgo.NodeName(meta.NodeID)),
		attribute.String("outcome", outcome),
	))
	c.telemetryBuilder.KafkaBrokerConnects.Add(
		context.Background(),
		1,
		opt,
	)
}

func (c *franzConsumer) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node_id", kgo.NodeName(meta.NodeID)),
	))
	c.telemetryBuilder.KafkaBrokerClosed.Add(
		context.Background(),
		1,
		opt,
	)
}

func (c *franzConsumer) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node_id", kgo.NodeName(meta.NodeID)),
	))
	// KafkaBrokerThrottlingDuration is deprecated in favor of KafkaBrokerThrottlingLatency.
	c.telemetryBuilder.KafkaBrokerThrottlingDuration.Record(
		context.Background(),
		throttleInterval.Milliseconds(),
		opt,
	)
	c.telemetryBuilder.KafkaBrokerThrottlingLatency.Record(
		context.Background(),
		throttleInterval.Seconds(),
		opt,
	)
}

func (c *franzConsumer) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, _ int, readWait, timeToRead time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node_id", kgo.NodeName(meta.NodeID)),
		attribute.String("outcome", outcome),
	))
	// KafkaReceiverLatency is deprecated in favor of KafkaReceiverReadLatency.
	c.telemetryBuilder.KafkaReceiverLatency.Record(
		context.Background(),
		readWait.Milliseconds()+timeToRead.Milliseconds(),
		opt,
	)
	c.telemetryBuilder.KafkaReceiverReadLatency.Record(
		context.Background(),
		readWait.Seconds()+timeToRead.Seconds(),
		opt,
	)
}

// OnFetchBatchRead is called once per batch read from Kafka.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookFetchBatchRead
func (c *franzConsumer) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, partition int32, m kgo.FetchBatchMetrics) {
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node_id", kgo.NodeName(meta.NodeID)),
		attribute.String("topic", topic),
		attribute.Int64("partition", int64(partition)),
		attribute.String("compression_codec", compressionFromCodec(m.CompressionType)),
		attribute.String("outcome", "success"),
	))
	// KafkaReceiverMessages is deprecated in favor of KafkaReceiverRecords.
	c.telemetryBuilder.KafkaReceiverMessages.Add(
		context.Background(),
		int64(m.NumRecords),
		opt,
	)
	c.telemetryBuilder.KafkaReceiverRecords.Add(
		context.Background(),
		int64(m.NumRecords),
		opt,
	)
	c.telemetryBuilder.KafkaReceiverBytes.Add(
		context.Background(),
		int64(m.CompressedBytes),
		opt,
	)
	c.telemetryBuilder.KafkaReceiverBytesUncompressed.Add(
		context.Background(),
		int64(m.UncompressedBytes),
		opt,
	)
}

// franzConsumerWithOptionalHooks wraps franzConsumer
// so the optional OnFetchRecordUnbuffered can be enabled.
type franzConsumerWithOptionalHooks struct {
	*franzConsumer
}

// OnFetchRecordUnbuffered is called when a fetched record is unbuffered and ready to be processed.
// Note that this hook may slow down high-volume consuming a bit.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookFetchRecordUnbuffered
func (c franzConsumerWithOptionalHooks) OnFetchRecordUnbuffered(r *kgo.Record, polled bool) {
	if !polled {
		return // Record metrics when polled by `client.PollRecords()`.
	}
	opt := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("topic", r.Topic),
		attribute.Int64("partition", int64(r.Partition)),
	))
	c.telemetryBuilder.KafkaReceiverRecordsDelay.Record(
		context.Background(),
		time.Since(r.Timestamp).Seconds(),
		opt,
	)
}

func compressionFromCodec(c uint8) string {
	// CompressionType signifies which algorithm the batch was compressed
	// with.
	//
	// 0 is no compression, 1 is gzip, 2 is snappy, 3 is lz4, and 4 is
	// zstd.
	switch c {
	case 0:
		return "none"
	case 1:
		return "gzip"
	case 2:
		return "snappy"
	case 3:
		return "lz4"
	case 4:
		return "zstd"
	default:
		return "unknown"
	}
}

func makeClearLeaderEpochAdjuster() func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	return func(_ context.Context, topics map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		for _, parts := range topics {
			for p, off := range parts {
				parts[p] = off.WithEpoch(-1)
			}
		}
		return topics, nil
	}
}
