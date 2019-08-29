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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class TracingDemos {

    @Test
    @TestTracing(snapshot = true, layout = LayoutStrategy.SPLIT_BY_TRACE)
    @SuppressWarnings("FutureReturnValueIgnored")
    void handles_async_spans() throws Exception {
        int numThreads = 2;
        int numTasks = 4;
        int taskDurationMillis = 1000;
        int expectedDurationMillis = numTasks * taskDurationMillis / numThreads;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);

        try (CloseableTracer root = CloseableTracer.startSpan("root")) {
            IntStream.range(0, numTasks).forEach(i -> {
                // DetachedSpan detachedSpan = DetachedSpan.start("task-queue-time" + i);

                executorService.submit(() -> {
                    // detachedSpan.close();
                    emit_nested_spans();
                    countDownLatch.countDown();
                });
            });
        }

        assertThat(countDownLatch.await(expectedDurationMillis + 1000, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    void async_future() {
        int numThreads = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        final SettableFuture<Object> objectSettableFuture = SettableFuture.create();

        try (CloseableTracer root = CloseableTracer.startSpan("root")) {
            // DetachedSpan listener = DetachedSpan.start("listener");
            // objectSettableFuture.addListener(() -> {
            //
            // }), executorService;
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

    private static void sleep(int i, String operation) {
        try (CloseableTracer t = CloseableTracer.startSpan(operation)) {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            throw new RuntimeException("dont care", e);
        }
    }

    private static void sleep(int i) {
        sleep(i, "sleep " + i);
    }

    private static void emit_nested_spans() {
        try (CloseableTracer root = CloseableTracer.startSpan("root")) {
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
