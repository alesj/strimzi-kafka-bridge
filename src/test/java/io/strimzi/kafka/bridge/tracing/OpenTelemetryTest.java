/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.core.tracing.TracingOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;

/**
 * OpenTelemetry tests
 */
public class OpenTelemetryTest extends TracingTestBase implements TracingConstants {
    @Override
    protected TracingOptions tracingOptions() {
        System.setProperty(OPENTELEMETRY_TRACES_EXPORTER_KEY, JAEGER);
        System.setProperty(OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY, "strimzi-kafka-bridge-test");
        return new OpenTelemetryOptions();
    }
}
