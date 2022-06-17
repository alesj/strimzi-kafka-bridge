/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.Properties;

/**
 * Simple interface to abstract tracing between legacy OpenTracing and new OpenTelemetry.
 */
public interface TracingHandle {
    String envName();
    String serviceName(BridgeConfig config);
    void initialize();

    <K, V> SpanBuilderHandle<K, V> builder(RoutingContext routingContext, String operationName);
    <K, V> SpanHandle<K, V> span(RoutingContext routingContext, String operationName);

    <K, V>  void handleRecordSpan(SpanHandle<K, V> parentSpanHandle, KafkaConsumerRecord<K, V> record);

    void kafkaConsumerConfig(Properties props);
    void kafkaProducerConfig(Properties props);
}
