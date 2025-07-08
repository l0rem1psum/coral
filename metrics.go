package processor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	metricsInputProcessed  = "coral_input_processed_total"
	metricsItemReleased    = "coral_item_released_total"
	metricsProcessDuration = "coral_process_duration_microseconds"
	metricsOutputDuration  = "coral_output_duration_microseconds"
)

type metricsRecorder struct {
	attributes []attribute.KeyValue

	inputProcessed  metric.Int64Counter
	itemReleased    metric.Int64Counter
	processDuration metric.Int64Histogram
	outputDuration  metric.Int64Histogram
}

func newMetricsRecorder(meter metric.Meter, label string) (*metricsRecorder, error) {
	inputProcessed, err := meter.Int64Counter(
		metricsInputProcessed,
		metric.WithDescription("Total number of input processed by the processor"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	itemReleased, err := meter.Int64Counter(
		metricsItemReleased,
		metric.WithDescription("Total number of items released at input/output"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	processDuration, err := meter.Int64Histogram(
		metricsProcessDuration,
		metric.WithDescription("Time taken to process an input"),
		metric.WithUnit("μs"),
	)
	if err != nil {
		return nil, err
	}

	outputDuration, err := meter.Int64Histogram(
		metricsOutputDuration,
		metric.WithDescription("Time taken to send an output"),
		metric.WithUnit("μs"),
	)
	if err != nil {
		return nil, err
	}

	return &metricsRecorder{
		attributes:      []attribute.KeyValue{{Key: "label", Value: attribute.StringValue(label)}},
		inputProcessed:  inputProcessed,
		itemReleased:    itemReleased,
		processDuration: processDuration,
		outputDuration:  outputDuration,
	}, nil
}

func (m *metricsRecorder) recordInputProcessedSuccess(ctx context.Context, processIdx int) {
	if m == nil {
		return
	}

	m.inputProcessed.Add(
		ctx,
		1,
		metric.WithAttributes(
			append(
				[]attribute.KeyValue{
					attribute.String("result", "success"),
					attribute.String("process_idx", fmt.Sprintf("%d", processIdx)),
				},
				m.attributes...,
			)...,
		),
	)
}

func (m *metricsRecorder) recordInputProcessedFailure(ctx context.Context, processIdx int) {
	if m == nil {
		return
	}

	m.inputProcessed.Add(
		ctx,
		1,
		metric.WithAttributes(
			append(
				[]attribute.KeyValue{
					attribute.String("result", "failure"),
					attribute.String("process_idx", fmt.Sprintf("%d", processIdx)),
				},
				m.attributes...,
			)...,
		),
	)
}

func (m *metricsRecorder) recordInputReleased(ctx context.Context, inputIdx int) {
	if m == nil {
		return
	}

	m.itemReleased.Add(
		ctx,
		1,
		metric.WithAttributes(
			append(
				[]attribute.KeyValue{
					attribute.String("released_at", "input"),
					attribute.String("input_idx", fmt.Sprintf("%d", inputIdx)),
				},
				m.attributes...,
			)...,
		),
	)
}

func (m *metricsRecorder) recordOutputReleased(ctx context.Context, outputIdx int) {
	if m == nil {
		return
	}

	m.itemReleased.Add(
		ctx,
		1,
		metric.WithAttributes(
			append(
				[]attribute.KeyValue{
					attribute.String("released_at", "output"),
					attribute.String("output_idx", fmt.Sprintf("%d", outputIdx)),
				},
				m.attributes...,
			)...,
		),
	)
}

func (m *metricsRecorder) recordProcessDuration(ctx context.Context, duration time.Duration) {
	if m == nil {
		return
	}

	m.processDuration.Record(
		ctx,
		duration.Microseconds(),
		metric.WithAttributes(m.attributes...),
	)
}

func (m *metricsRecorder) recordOutputDuration(ctx context.Context, outputIdx int, duration time.Duration) {
	if m == nil {
		return
	}

	m.outputDuration.Record(
		ctx,
		duration.Microseconds(),
		metric.WithAttributes(
			append(
				[]attribute.KeyValue{attribute.String("output_idx", fmt.Sprintf("%d", outputIdx))},
				m.attributes...,
			)...,
		),
	)
}
