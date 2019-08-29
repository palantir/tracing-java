/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("FutureReturnValueIgnored")
class TracingDemos {

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    void handles_async_spans() throws Exception {
        int numThreads = 2;
        int numTasks = 4;
        int taskDurationMillis = 1000;
        int expectedDurationMillis = numTasks * taskDurationMillis / numThreads;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);

        IntStream.range(0, numTasks).forEach(i -> {
            Tracer.clearCurrentTrace(); // just pretending all these tasks are on a fresh request

            CrossThreadSpan crossThread = CrossThreadSpan.startSpan("task-queue-time" + i);
            executorService.submit(() -> {
                try (CloseableTracer t = crossThread.completeAndStartSpan("task" + i)) {
                    emit_nested_spans();
                    countDownLatch.countDown();
                }
            });
        });

        assertThat(countDownLatch.await(expectedDurationMillis + 1000, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    void async_future() throws InterruptedException {
        int numThreads = 2;
        int numCallbacks = 10;
        ExecutorService executorService = Tracers.wrap(Executors.newFixedThreadPool(numThreads));
        final SettableFuture<Object> future = SettableFuture.create();
        CountDownLatch latch = new CountDownLatch(numCallbacks);

        assertThat(Tracer.hasTraceId()).isFalse();

        IntStream.range(0, numCallbacks).forEach(i -> {

            Tracer.clearCurrentTrace();

            CrossThreadSpan span = CrossThreadSpan.startSpan("callback-pending" + i + " (cross thread span)");
            Futures.addCallback(future, new FutureCallback<Object>() {
                @Override
                public void onSuccess(@Nullable Object result) {
                    try (CloseableTracer t = span.completeAndStartSpan("success" + i)) {
                        Thread.sleep(10);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    Assertions.fail();
                }
            }, executorService);
        });

        try (CloseableTracer root = CloseableTracer.startSpan("bbb")) {
            executorService.submit(() -> {
                future.set(null);
            });
        }


        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    void multi_producer_single_consumer() throws InterruptedException {
        int numProducers = 2;
        int numElem = 20;
        PriorityBlockingQueue<String> work = new PriorityBlockingQueue<>();

        CountDownLatch submitLatch = new CountDownLatch(numElem);
        CountDownLatch consumeLatch = new CountDownLatch(numElem);
        ExecutorService producerExecutorService = Tracers.wrap(Executors.newFixedThreadPool(numProducers));
        ExecutorService consumerExecutorService = Tracers.wrap(Executors.newFixedThreadPool(1));

        try (CloseableTracer submit = CloseableTracer.startSpan("submit")) {
            IntStream.range(0, numElem).forEach(i -> {
                producerExecutorService.submit(() -> {
                    try (CloseableTracer closeableTracer = CloseableTracer.startSpan("submit-work" + i)) {
                        work.add("work" + i);
                        submitLatch.countDown();
                    }
                });
            });
            assertThat(submitLatch.await(10, TimeUnit.SECONDS)).isTrue();

            consumerExecutorService.submit(() -> {
                for (int i = 0; i < numElem; i++) {
                    String poll = work.take();
                    sleep(10, "processing" + poll);
                    consumeLatch.countDown();
                }
                return null;
            });
            assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    void backoffs_on_a_scheduled_executor() throws InterruptedException {
        ScheduledExecutorService executor = Tracers.wrap(Executors.newScheduledThreadPool(2));
        CountDownLatch latch = new CountDownLatch(1);

        try (CloseableTracer t = CloseableTracer.startSpan("some-request")) {
            executor.execute(() -> {
                // first attempt at a network call
                sleep(100, "first attempt");

                executor.schedule(() -> {
                    // attempt number 2
                    sleep(100, "second attempt");

                    executor.schedule(() -> {
                        // attempt number 3
                        sleep(100, "final attempt");

                        latch.countDown();
                    }, 100, TimeUnit.MILLISECONDS);

                    sleep(200, "second tidying");
                }, 100, TimeUnit.MILLISECONDS);

                sleep(200, "first tidying");
            });

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        }

        MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    @SuppressWarnings("CheckReturnValue")
    void transformed_future() throws InterruptedException {
        SettableFuture<Object> future = SettableFuture.create();
        ScheduledExecutorService executor = Tracers.wrap(Executors.newScheduledThreadPool(2));
        CountDownLatch latch = new CountDownLatch(1);

        FluentFuture.from(future)
                .transform(result -> {
                    sleep(100, "first");
                    return result;
                }, executor)
                .transform(result -> {
                    sleep(100, "second");
                    latch.countDown();
                    return result;
                }, executor);

        executor.submit(() -> {
            try (CloseableTracer root = CloseableTracer.startSpan("root")) {
                future.set(null);
            }
        });

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void sleep(int millis, String operation) {
        try (CloseableTracer t = CloseableTracer.startSpan(operation)) {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException("dont care", e);
        }
    }

    private static void sleep(int millis) {
        sleep(millis, "sleep " + millis);
    }

    @SuppressWarnings("NestedTryDepth")
    private static void emit_nested_spans() {
        try (CloseableTracer root = CloseableTracer.startSpan("emit_nested_spans")) {
            try (CloseableTracer first = CloseableTracer.startSpan("first")) {
                sleep(100);
                try (CloseableTracer nested = CloseableTracer.startSpan("nested")) {
                    sleep(90);
                }
                sleep(10);
            }
            try (CloseableTracer second = CloseableTracer.startSpan("second")) {
                sleep(100);
            }
        }
    }
}
