/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apis

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterName = "kubeops.dev/pgoutbox"

	attrKeyTable   = "table"
	attrKeySubject = "subject"
	attrKeyKind    = "kind"
)

type Metrics struct {
	// Counters
	eventsPublished metric.Int64Counter
	eventsFiltered  metric.Int64Counter
	eventsFailed    metric.Int64Counter

	// Histograms
	processingDuration metric.Float64Histogram
	publishDuration    metric.Float64Histogram

	// Gauges (observable)
	replicationLagBytes metric.Int64ObservableGauge
	currentLSN          metric.Int64ObservableGauge
}

func NewMetrics() (*Metrics, error) {
	meter := otel.Meter(meterName)

	eventsPublished, err := meter.Int64Counter(
		"pgoutbox.events.published",
		metric.WithDescription("Total number of successfully published events"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	eventsFiltered, err := meter.Int64Counter(
		"pgoutbox.events.filtered",
		metric.WithDescription("Total number of events skipped by filter"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	eventsFailed, err := meter.Int64Counter(
		"pgoutbox.events.failed",
		metric.WithDescription("Total number of events that failed processing"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	processingDuration, err := meter.Float64Histogram(
		"pgoutbox.processing.duration",
		metric.WithDescription("Time to process a WAL message end-to-end"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	publishDuration, err := meter.Float64Histogram(
		"pgoutbox.publish.duration",
		metric.WithDescription("Time to publish an event to the message broker"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	)
	if err != nil {
		return nil, err
	}

	replicationLagBytes, err := meter.Int64ObservableGauge(
		"pgoutbox.replication.lag.bytes",
		metric.WithDescription("Replication lag in bytes (difference between server WAL end and current LSN)"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	currentLSN, err := meter.Int64ObservableGauge(
		"pgoutbox.replication.lsn",
		metric.WithDescription("Current LSN position in the replication stream"),
		metric.WithUnit("{position}"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		eventsPublished:     eventsPublished,
		eventsFiltered:      eventsFiltered,
		eventsFailed:        eventsFailed,
		processingDuration:  processingDuration,
		publishDuration:     publishDuration,
		replicationLagBytes: replicationLagBytes,
		currentLSN:          currentLSN,
	}, nil
}

func (m *Metrics) IncPublishedEvents(subject, table string) {
	if m == nil || m.eventsPublished == nil {
		return
	}
	m.eventsPublished.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String(attrKeySubject, subject),
			attribute.String(attrKeyTable, table),
		),
	)
}

func (m *Metrics) IncFilterSkippedEvents(table string) {
	if m == nil || m.eventsFiltered == nil {
		return
	}
	m.eventsFiltered.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String(attrKeyTable, table)),
	)
}

func (m *Metrics) IncProblematicEvents(kind string) {
	if m == nil || m.eventsFailed == nil {
		return
	}
	m.eventsFailed.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String(attrKeyKind, kind)),
	)
}

func (m *Metrics) RecordProcessingDuration(seconds float64) {
	if m == nil || m.processingDuration == nil {
		return
	}
	m.processingDuration.Record(context.Background(), seconds)
}

func (m *Metrics) RecordPublishDuration(seconds float64, subject string) {
	if m == nil || m.publishDuration == nil {
		return
	}
	m.publishDuration.Record(context.Background(), seconds,
		metric.WithAttributes(attribute.String(attrKeySubject, subject)),
	)
}

type GaugeCallbacks struct {
	GetCurrentLSN func() uint64
	GetServerLSN  func() uint64
}

func (m *Metrics) RegisterCallbacks(cb GaugeCallbacks) error {
	if m == nil {
		return nil
	}

	meter := otel.Meter(meterName)

	_, err := meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			if cb.GetCurrentLSN != nil {
				currentLSN := cb.GetCurrentLSN()
				o.ObserveInt64(m.currentLSN, int64(currentLSN))
				if cb.GetServerLSN != nil {
					serverLSN := cb.GetServerLSN()
					if serverLSN > currentLSN {
						o.ObserveInt64(m.replicationLagBytes, int64(serverLSN-currentLSN))
					} else {
						o.ObserveInt64(m.replicationLagBytes, 0)
					}
				}
			}

			return nil
		},
		m.currentLSN,
		m.replicationLagBytes,
	)

	return err
}
