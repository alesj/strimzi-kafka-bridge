/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.core.Context;
import io.vertx.core.spi.VertxTracerFactory;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingOptions;
import io.vertx.core.tracing.TracingPolicy;

import java.util.function.BiConsumer;

/**
 * Custom Bridge TracingOptions.
 * It lazily delegates usage to TracingHandle.
 */
public class BridgeTracingOptions extends TracingOptions {
    @Override
    public VertxTracerFactory getFactory() {
        return new BridgeVertxTracerFactory();
    }

    private static class BridgeVertxTracerFactory implements VertxTracerFactory {
        @Override
        public VertxTracer tracer(TracingOptions options) {
            return new BridgeVertxTracer();
        }
    }

    private static class BridgeVertxTracer implements VertxTracer {
        private VertxTracer getTracer() {
            return TracingUtil.getTracing().tracer();
        }
        @Override
        public Object receiveRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, Iterable headers, TagExtractor tagExtractor) {
            return getTracer().receiveRequest(context, kind, policy, request, operation, headers, tagExtractor);
        }

        @Override
        public void sendResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
            getTracer().sendResponse(context, response, payload, failure, tagExtractor);
        }

        @Override
        public Object sendRequest(Context context, SpanKind kind, TracingPolicy policy, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
            return getTracer().sendRequest(context, kind, policy, request, operation, headers, tagExtractor);
        }

        @Override
        public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
            getTracer().receiveResponse(context, response, payload, failure, tagExtractor);
        }

        @Override
        public void close() {
            getTracer().close();
        }
    }
}
