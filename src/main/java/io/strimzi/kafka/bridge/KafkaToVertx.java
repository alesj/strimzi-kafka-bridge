/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import java.util.concurrent.Callable;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.common.KafkaFuture;

public class KafkaToVertx {
    public static <T> void handle(KafkaFuture<T> kafkaFuture, Handler<AsyncResult<T>> handler) {
        kafkaFuture.whenComplete((r, t) -> {
            Future<T> future = t != null ? Future.failedFuture(t) : Future.succeededFuture(r);
            handler.handle(future);
        });
    }

    public static <T> void handle(Runnable runnable, Handler<AsyncResult<T>> handler) {
        handle(() -> {
            runnable.run();
            return null;
        }, handler);
    }

    public static <T> void handle(Callable<T> callable, Handler<AsyncResult<T>> handler) {
        if (handler != null) {
            handler.handle(toAsyncResult(callable));
        } else {
            try {
                callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> AsyncResult<T> toAsyncResult(Runnable runnable) {
        return toAsyncResult(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> AsyncResult<T> toAsyncResult(Callable<T> callable) {
        try {
            return Future.succeededFuture(callable.call());
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }
}
