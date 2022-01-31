/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.kafkaclients.KafkaTracing;
import io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor;
import io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor;
import io.vertx.core.tracing.TracingOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OpenTelemetry tests
 */
public class OpenTelemetryTest extends TracingTestBase implements TracingConstants {
    @Override
    protected TracingOptions tracingOptions() {
        System.setProperty(OPENTELEMETRY_TRACES_EXPORTER_KEY, JAEGER);
        System.setProperty(OPENTELEMETRY_SERVICE_NAME_PROPERTY_KEY, "strimzi-kafka-bridge-test");
        return null;
    }

    private static class TestProducerAttribExtractor implements AttributesExtractor<ProducerRecord<?, ?>, Void> {
        @Override
        public void onStart(AttributesBuilder attributes, ProducerRecord<?, ?> producerRecord) {
            set(attributes, AttributeKey.stringKey("pfoo_start"), "pbar1");
        }

        @Override
        public void onEnd(AttributesBuilder attributes, ProducerRecord<?, ?> producerRecord, @Nullable Void unused, @Nullable Throwable error) {
            set(attributes, AttributeKey.stringKey("pfoo_end"), "pbar2");
        }
    }

    private static class TestConsumerAttribExtractor implements AttributesExtractor<ConsumerRecord<?, ?>, Void> {
        @Override
        public void onStart(AttributesBuilder attributes, ConsumerRecord<?, ?> producerRecord) {
            set(attributes, AttributeKey.stringKey("cfoo_start"), "cbar1");
        }

        @Override
        public void onEnd(AttributesBuilder attributes, ConsumerRecord<?, ?> producerRecord, @Nullable Void unused, @Nullable Throwable error) {
            set(attributes, AttributeKey.stringKey("cfoo_end"), "cbar2");
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> configs = new HashMap<>(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

        System.setProperty("otel.traces.exporter", "jaeger");
        System.setProperty("otel.service.name", "myapp1");
        //System.setProperty("otel.exporter.jaeger.endpoint", "http://localhost:14250"); // by default
        KafkaTracing tracing = KafkaTracing.newBuilder(GlobalOpenTelemetry.get())
            .addProducerAttributesExtractors(new TestProducerAttribExtractor())
            .addConsumerAttributesExtractors(new TestConsumerAttribExtractor())
            .build();

        Producer<String, String> op = new KafkaProducer<>(
            configs,
            new StringSerializer(),
            new StringSerializer()
        );
        Producer<String, String> producer = tracing.wrap(op);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        Consumer<String, String> oc = new KafkaConsumer<>(
            configs,
            new StringDeserializer(),
            new StringDeserializer()
        );
        Consumer<String, String> consumer = tracing.wrap(oc);
        consumer.subscribe(Collections.singleton("mytopic"));

        AtomicBoolean run = new AtomicBoolean(true);
        new Thread(() -> {
            while (run.get()) {
                consumer.poll(Duration.ofMillis(1L)).forEach(c -> {
                    System.out.println("ck = " + c.key());
                    System.out.println("cv = " + c.value());
                });
            }
        }).start();

        Thread.sleep(10_000L); // 10sec

        producer.send(new ProducerRecord<>("mytopic", "foo", "bar")).get();

        Thread.sleep(10_000L); // 10sec
        run.set(false);

        producer.close();
        consumer.close();
    }

    public static void mainy(String[] args) throws Exception {
        Map<String, Object> configs = new HashMap<>(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

        System.setProperty("otel.traces.exporter", "jaeger");
        System.setProperty("otel.service.name", "myapp2");
        //System.setProperty("otel.exporter.jaeger.endpoint", "http://localhost:14250"); // by default

        Map<String, Object> producerConfig = new HashMap<>(configs);
        producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(
            producerConfig,
            new StringSerializer(),
            new StringSerializer()
        );

        Map<String, Object> consumerConfig = new HashMap<>(configs);
        consumerConfig.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingConsumerInterceptor.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        Consumer<String, String> consumer = new KafkaConsumer<>(
            consumerConfig,
            new StringDeserializer(),
            new StringDeserializer()
        );
        consumer.subscribe(Collections.singleton("mytopic"));

        AtomicBoolean run = new AtomicBoolean(true);
        new Thread(() -> {
            while (run.get()) {
                consumer.poll(Duration.ofMillis(1L)).forEach(c -> {
                    System.out.println("ck = " + c.key());
                    System.out.println("cv = " + c.value());
                });
            }
        }).start();

        Thread.sleep(10_000L); // 10sec

        producer.send(new ProducerRecord<>("mytopic", "foo", "bar")).get();

        Thread.sleep(10_000L); // 10sec
        run.set(false);

        producer.close();
        consumer.close();
    }

    public static void streams() {
        Topology topology = null;
        Properties properties = null;
        KafkaClientSupplier supplier = new TracingKafkaClientSupplier();
        KafkaStreams streams = new KafkaStreams(
            topology, properties, supplier
        );
    }

    private static class TracingKafkaClientSupplier implements KafkaClientSupplier {
        @Override
        public Admin getAdmin(Map<String, Object> config) {
            return Admin.create(config);
        }

        @Override
        public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
            KafkaTracing tracing = KafkaTracing.create(GlobalOpenTelemetry.get());
            return tracing.wrap(new KafkaProducer<>(config));
        }

        @Override
        public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
            KafkaTracing tracing = KafkaTracing.create(GlobalOpenTelemetry.get());
            return tracing.wrap(new KafkaConsumer<>(config));
        }

        @Override
        public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
            return getConsumer(config);
        }

        @Override
        public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
            return getConsumer(config);
        }
    }

}
