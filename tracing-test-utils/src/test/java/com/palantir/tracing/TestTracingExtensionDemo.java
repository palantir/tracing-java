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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class TestTracingExtensionDemo {

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        // Ensure traces created in test setup are not captured by the test annotation
        try (CloseableTracer ignored = CloseableTracer.startSpan("ignored")) {
            Thread.sleep(10);
        }
    }

    @SuppressWarnings("NestedTryDepth")
    void prod_code() throws InterruptedException {
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
            try (CloseableTracer third = CloseableTracer.startSpan("third")) {
                Thread.sleep(100);
            }
        }
    }

    @Test
    @TestTracing(snapshot = true)
    void handles_trace_with_multiple_root_spans() throws InterruptedException {
        prod_code();
        prod_code();
    }

    @ParameterizedTest(name = "foo {index} bar {arguments}")
    @ValueSource(ints = {1, 2, 3})
    @TestTracing(snapshot = true)
    void handles_trace_with_single_root_span(int _value) throws InterruptedException {
        prod_code();
    }

    @Test
    void vacuous() {
        // no spans here - nothing should break
    }

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

        IntStream.range(0, numTasks).forEach(i -> {
            DetachedSpan detachedSpan = DetachedSpan.start("task-queue-time" + i);

            executorService.execute(() -> {
                try (CloseableSpan closeableSpan = detachedSpan.completeAndStartChild("do-work-" + i)) {
                    prod_code();
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        assertThat(countDownLatch.await(expectedDurationMillis + 1000, TimeUnit.MILLISECONDS))
                .isTrue();
    }
}
