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

import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
                    try {
                        emit_nested_spans();
                        countDownLatch.countDown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
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
            objectSettableFuture.addListener(() -> {

            }), executorService;
        }
    }

    private static void emit_nested_spans() throws InterruptedException {
        try (CloseableTracer root = CloseableTracer.startSpan("root")) {
            try (CloseableTracer first = CloseableTracer.startSpan("first")) {
                Thread.sleep(100);
                try (CloseableTracer nested = CloseableTracer.startSpan("nested")) {
                    Thread.sleep(90);
                }
                Thread.sleep(10);
            }
            try (CloseableTracer second = CloseableTracer.startSpan("second")) {
                Thread.sleep(100);
            }
        }
    }
}
