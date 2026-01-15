package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var meterProvider *metric.MeterProvider

// Init initializes the OpenTelemetry meter provider with Prometheus exporter.
// It should be called once at application startup.
func InitMetrics(ctx context.Context, version string) error {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	promExporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	meterProvider = metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(promExporter),
	)

	otel.SetMeterProvider(meterProvider)

	return nil
}

// Shutdown gracefully shuts down the telemetry provider.
func Shutdown(ctx context.Context) error {
	if meterProvider == nil {
		return nil
	}
	return meterProvider.Shutdown(ctx)
}
