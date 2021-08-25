/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

/**
 * Tracing constants.
 */
public interface TracingConstants {
    String JAEGER = "jaeger";

    String JAEGER_OPENTRACING = JAEGER;
    String JAEGER_OPENTELEMETRY = JAEGER + "-otel";

    String OPENTELEMETRY_SERVICE_NAME_ENV_KEY = "OTEL_SERVICE_NAME";
    String OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY = "otel.service.name";
    String OPENTELEMETRY_TRACES_EXPORTER_KEY = "otel.traces.exporter";
}
