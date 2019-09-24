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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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

            DetachedSpan crossThread = DetachedSpan.start("task-queue-time" + i);
            executorService.submit(() -> {
                try (CloseableSpan t = crossThread.completeAndStartChild("task" + i)) {
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
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        final SettableFuture<Object> future = SettableFuture.create();
        CountDownLatch latch = new CountDownLatch(numCallbacks);

        try (CloseableTracer tracer = CloseableTracer.startSpan("I am a root span")) {
            String traceId = Tracer.getTraceId();

            IntStream.range(0, numCallbacks).forEach(i -> {

                DetachedSpan span = DetachedSpan.start("callback-pending" + i + " (cross thread span)");

                Futures.addCallback(future, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object _value) {
                        assertThat(Tracer.hasTraceId()).isFalse();
                        try (CloseableSpan tracer = span.completeAndStartChild("success" + i)) {
                            assertThat(Tracer.getTraceId()).isEqualTo(traceId);
                            sleep(10);
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Throwable _throwable) {
                        Assertions.fail();
                    }
                }, executorService);
            });

            try (CloseableTracer root = CloseableTracer.startSpan("bbb")) {
                executorService.submit(() -> {
                    future.set(null);
                });
            }

        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    void multi_producer_single_consumer() throws InterruptedException {
        int numProducers = 2;
        int numElem = 20;
        ArrayBlockingQueue<QueuedWork> work = new ArrayBlockingQueue<QueuedWork>(numElem);

        CountDownLatch submitLatch = new CountDownLatch(numElem);
        CountDownLatch consumeLatch = new CountDownLatch(numElem);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(numProducers);
        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(1);

        try (CloseableTracer submit = CloseableTracer.startSpan("submit")) {
            IntStream.range(0, numElem).forEach(i -> {

                Tracer.clearCurrentTrace(); // just pretending all these tasks are on a fresh request

                DetachedSpan span = DetachedSpan.start("callback-pending" + i + " (cross thread span)");
                producerExecutorService.submit(() -> {
                    work.add(new QueuedWork() {
                        @Override
                        public String name() {
                            return "work" + i;
                        }

                        @Override
                        public DetachedSpan span() {
                            return span;
                        }
                    });
                    submitLatch.countDown();
                });
            });
            assertThat(submitLatch.await(10, TimeUnit.SECONDS)).isTrue();

            consumerExecutorService.submit(() -> {
                for (int i = 0; i < numElem; i++) {
                    QueuedWork queuedWork = work.take();
                    try (CloseableSpan span = queuedWork.span().completeAndStartChild("consume" + queuedWork.name())) {
                        Thread.sleep(10);
                    }
                    consumeLatch.countDown();
                }
                return null;
            });
            assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.CHRONOLOGICAL)
    void backoffs_on_a_scheduled_executor() throws InterruptedException {
        ScheduledExecutorService executor = Tracers.wrap(Executors.newScheduledThreadPool(2));
        CountDownLatch latch = new CountDownLatch(1);

        DetachedSpan overall = DetachedSpan.start("overall request");
        executor.execute(() -> {

            try (CloseableTracer t = CloseableTracer.startSpan("first network call (pretending this fails)")) {
                sleep(100);
            }

            DetachedSpan backoff = overall.childDetachedSpan("backoff");
            executor.schedule(() -> {
                try (CloseableSpan attempt2 = backoff.completeAndStartChild("secondAttempt")) {
                    sleep(100);
                    overall.complete();
                    latch.countDown();

                }
            }, 20, TimeUnit.MILLISECONDS);
        });

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    @SuppressWarnings("CheckReturnValue")
    void transformed_future() throws InterruptedException {
        SettableFuture<Object> future = SettableFuture.create();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        CountDownLatch latch = new CountDownLatch(1);

        DetachedSpan foo = DetachedSpan.start("foo");
        FluentFuture.from(future)
                .transform(result -> {
                    try (CloseableSpan t = foo.childSpan("first transform")) {
                        sleep(1000);
                        return result;
                    }
                }, executor)
                .transform(result -> {
                    try (CloseableSpan t = foo.childSpan("second transform")) {
                        sleep(1000);
                        latch.countDown();
                        return result;
                    }
                }, executor)
                .addCallback(new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object _value) {
                        foo.complete();
                    }

                    @Override
                    public void onFailure(Throwable _throwable) {
                        foo.complete();
                    }
                }, executor);

        executor.submit(() -> {
            future.set(null);
        });

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void sleepSpan(int millis) {
        try (CloseableTracer t = CloseableTracer.startSpan("sleep " + millis)) {
            sleep(millis);
        }
    }

    @SuppressWarnings("NestedTryDepth")
    private static void emit_nested_spans() {
        try (CloseableTracer root = CloseableTracer.startSpan("emit_nested_spans")) {
            try (CloseableTracer first = CloseableTracer.startSpan("first")) {
                sleepSpan(100);
                try (CloseableTracer nested = CloseableTracer.startSpan("nested")) {
                    sleepSpan(90);
                }
                sleepSpan(10);
            }
            try (CloseableTracer second = CloseableTracer.startSpan("second")) {
                sleepSpan(100);
            }
        }
    }

    interface QueuedWork {
        String name();
        DetachedSpan span();
    }
}
