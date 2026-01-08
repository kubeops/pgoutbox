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

package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var meterProvider *metric.MeterProvider

// Config holds the telemetry configuration.
type Config struct {
	// PrometheusEnabled enables the Prometheus metrics endpoint.
	PrometheusEnabled bool `json:"prometheusEnabled" mapstructure:"prometheusEnabled"`
}

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
