/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;

final class SampledSpanTest {
    private final List<Span> spans = new CopyOnWriteArrayList<>();

    @BeforeEach
    void before() {
        Tracer.clearCurrentTrace();
        Tracer.setSampler(NeverSampler.INSTANCE);
        Tracer.subscribe("sampled-span-test", spans::add);
    }

    @AfterEach
    void after() {
        Tracer.clearCurrentTrace();
        Tracer.unsubscribe("sampled-span-test");
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @Test
    void preservesContextAndRestoresTheUnsampledStack() {
        Tracer.initTraceWithSpan(
                Observability.DO_NOT_SAMPLE,
                "trace-id",
                Optional.of("user-agent"),
                "request",
                SpanType.SERVER_INCOMING);
        Tracer.fastStartSpan("existing-child");
        Trace original = Tracer.getTrace();
        String requestId = MDC.get(Tracers.REQUEST_ID_KEY);
        TraceLocal<Object> local = TraceLocal.of();
        Object value = new Object();
        local.set(value);

        try (CloseableSpan ignored = Tracer.startSampledSpan("sampled-operation")) {
            assertThat(Tracer.isTraceObservable()).isTrue();
            assertThat(Tracer.hasUnobservableTrace()).isFalse();
            assertThat(Tracer.getTraceId()).isEqualTo("trace-id");
            assertThat(Tracer.getForUserAgent()).contains("user-agent");
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo("trace-id");
            assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isEqualTo(requestId);
            assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isEqualTo("1");
            assertThat(local.get()).isSameAs(value);
            Tracer.fastStartSpan("sampled-child");
            Tracer.fastCompleteSpan();
        }

        assertThat(Tracer.getTrace()).isSameAs(original);
        assertThat(Tracer.hasUnobservableTrace()).isTrue();
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
        assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isEqualTo(requestId);
        assertThat(local.get()).isSameAs(value);
        assertThat(spans).extracting(Span::getOperation).containsExactly("sampled-child", "sampled-operation");
        assertThat(spans.get(0).getParentSpanId()).contains(spans.get(1).getSpanId());
        assertThat(spans.get(1).getParentSpanId()).isEmpty();
        assertThat(spans).extracting(Span::type).containsOnly(SpanType.LOCAL);

        Tracer.fastCompleteSpan();
        assertThat(Tracer.getTrace()).isSameAs(original);
        Tracer.fastCompleteSpan();
        assertNoTrace();
        assertThat(local.get()).isNull();
        assertThat(spans).hasSize(2);
    }

    @Test
    void addsAnOrdinaryChildWhenAlreadySampled() {
        Tracer.initTraceWithSpan(Observability.SAMPLE, "trace-id", "root", SpanType.SERVER_INCOMING);
        Trace original = Tracer.getTrace();
        String rootSpanId = Tracer.maybeGetTraceMetadata().orElseThrow().getSpanId();
        TraceLocal<String> local = TraceLocal.of();
        local.set("value");

        try (CloseableSpan ignored = Tracer.startSampledSpan("child", SpanType.CLIENT_OUTGOING)) {
            assertThat(Tracer.getTrace()).isSameAs(original);
            assertThat(local.get()).isEqualTo("value");
        }

        assertThat(Tracer.getTrace()).isSameAs(original);
        assertThat(Tracer.maybeGetTraceMetadata().orElseThrow().getSpanId()).isEqualTo(rootSpanId);
        assertThat(spans).hasSize(1);
        assertThat(spans.get(0).getParentSpanId()).contains(rootSpanId);
        assertThat(spans.get(0).type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        Tracer.fastCompleteSpan();
        assertThat(spans).extracting(Span::getOperation).containsExactly("child", "root");
        assertNoTrace();
    }

    @ParameterizedTest
    @EnumSource(SpanType.class)
    void createsAndClearsANewTraceWithoutConsultingTheSampler(SpanType type) {
        Tracer.setSampler(() -> {
            throw new AssertionError("Explicit sampling must not consult the sampler");
        });
        TraceLocal<Object> local = TraceLocal.of();
        String traceId;
        try (CloseableSpan ignored = Tracer.startSampledSpan("new-trace", type)) {
            assertThat(Tracer.isTraceObservable()).isTrue();
            traceId = Tracer.getTraceId();
            assertThat(traceId).isNotEmpty();
            assertThat(Tracer.getForUserAgent()).isEmpty();
            assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isEqualTo("1");
            if (type == SpanType.SERVER_INCOMING) {
                assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isNotEmpty();
            } else {
                assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isNull();
            }
            local.set(new Object());
        }
        assertNoTrace();
        assertThat(local.get()).isNull();
        assertThat(spans).hasSize(1);
        assertThat(spans.get(0).getTraceId()).isEqualTo(traceId);
        assertThat(spans.get(0).type()).isEqualTo(type);
    }

    @Test
    void nestedScopesUnwindOnFailure() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "root", SpanType.SERVER_INCOMING);
        Trace original = Tracer.getTrace();
        assertThatThrownBy(() -> {
                    try (CloseableSpan outer = Tracer.startSampledSpan("outer")) {
                        try (CloseableSpan inner = Tracer.startSampledSpan("inner")) {
                            throw new IllegalStateException("delegate failure");
                        }
                    }
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("delegate failure");
        assertThat(Tracer.getTrace()).isSameAs(original);
        assertThat(Tracer.isTraceObservable()).isFalse();
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
        assertThat(spans).extracting(Span::getOperation).containsExactly("inner", "outer");
        assertThat(spans.get(0).getParentSpanId()).contains(spans.get(1).getSpanId());
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void restoresTheOriginalTraceWhenAnObserverThrowsAnError() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        Tracer.fastStartSpan("existing-child");
        Trace original = Tracer.getTrace();
        String requestId = MDC.get(Tracers.REQUEST_ID_KEY);
        AssertionError failure = new AssertionError("observer failure");
        Tracer.subscribe("sampled-span-test", _span -> {
            throw failure;
        });

        assertThatThrownBy(() -> {
                    try (CloseableSpan ignored = Tracer.startSampledSpan("sampled")) {
                        assertThat(Tracer.isTraceObservable()).isTrue();
                    }
                })
                .isSameAs(failure);
        assertThat(Tracer.getTrace()).isSameAs(original);
        assertThat(Tracer.isTraceObservable()).isFalse();
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo("trace-id");
        assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isEqualTo(requestId);
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
        Tracer.fastCompleteSpan();
        assertThat(Tracer.getTrace()).isSameAs(original);
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void sharesSetAndRemoveWithContextsDetachedBeforeAndAfterSampling() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        Detached before = DetachedSpan.detach();
        TraceLocal<String> local = TraceLocal.of();
        local.set("original");
        DetachedSpan after;

        try (CloseableSpan ignored = Tracer.startSampledSpan("sampled")) {
            after = DetachedSpan.start("detached-after");
            local.set("updated");
            try (CloseableSpan attached = before.attach()) {
                assertThat(Tracer.isTraceObservable()).isFalse();
                assertThat(local.get()).isEqualTo("updated");
                local.remove();
            }
            assertThat(Tracer.isTraceObservable()).isTrue();
            assertThat(local.get()).isNull();
            local.set("final");
        }

        assertThat(local.get()).isEqualTo("final");
        try (CloseableSpan attached = after.attach()) {
            assertThat(Tracer.isTraceObservable()).isTrue();
            assertThat(local.get()).isEqualTo("final");
            local.remove();
        }
        assertThat(Tracer.isTraceObservable()).isFalse();
        assertThat(local.get()).isNull();
        after.complete();
        assertThat(spans).extracting(Span::getOperation).containsExactly("sampled", "detached-after");
        assertThat(spans.get(1).getParentSpanId()).contains(spans.get(0).getSpanId());
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void sharesFirstLocalInitializationWithAnOlderDetachedContextOnAnotherThread() throws Exception {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        Detached before = DetachedSpan.detach();
        AtomicInteger initializations = new AtomicInteger();
        TraceLocal<Object> local = TraceLocal.withInitialValue(() -> {
            initializations.incrementAndGet();
            return new Object();
        });
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Object> originalContextValue = executor.submit(() -> {
                try (CloseableSpan ignored = before.attach()) {
                    barrier.await(10, TimeUnit.SECONDS);
                    assertThat(Tracer.isTraceObservable()).isFalse();
                    return local.get();
                } finally {
                    assertNoTrace();
                }
            });
            try (CloseableSpan ignored = Tracer.startSampledSpan("sampled")) {
                barrier.await(10, TimeUnit.SECONDS);
                assertThat(local.get()).isSameAs(originalContextValue.get(10, TimeUnit.SECONDS));
                assertThat(initializations.get()).isEqualTo(1);
                local.remove();
            }
            assertThat(local.get()).isNotNull();
            assertThat(initializations.get()).isEqualTo(2);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void concurrentUpgradesOfTheSameDetachedContextShareFirstLocalInitialization() throws Exception {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        Detached detached = DetachedSpan.detach();
        AtomicInteger initializations = new AtomicInteger();
        TraceLocal<Object> local = TraceLocal.withInitialValue(() -> {
            initializations.incrementAndGet();
            return new Object();
        });
        CyclicBarrier barrier = new CyclicBarrier(2);
        Callable<Object> upgrade = () -> {
            try (CloseableSpan attached = detached.attach()) {
                Trace original = Tracer.getTrace();
                barrier.await(10, TimeUnit.SECONDS);
                Object value;
                try (CloseableSpan sampled = Tracer.startSampledSpan("sampled")) {
                    assertThat(Tracer.isTraceObservable()).isTrue();
                    value = local.get();
                }
                assertThat(Tracer.getTrace()).isSameAs(original);
                assertThat(Tracer.isTraceObservable()).isFalse();
                return value;
            } finally {
                assertNoTrace();
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Object> first = executor.submit(upgrade);
            Future<Object> second = executor.submit(upgrade);
            assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(second.get(10, TimeUnit.SECONDS));
            assertThat(local.get()).isSameAs(first.get(10, TimeUnit.SECONDS));
            assertThat(initializations.get()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        Tracer.fastCompleteSpan();
        assertNoTrace();
        assertThat(spans).hasSize(2);
    }

    @Test
    void repeatedUpgradesShareLocalsWithoutSharingSamplingDecisions() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        TraceLocal<String> local = TraceLocal.of();
        for (int attempt = 0; attempt < 2; attempt++) {
            try (CloseableSpan ignored = Tracer.startSampledSpan("sampled")) {
                local.set("value-" + attempt);
                assertThat(Tracer.isTraceObservable()).isTrue();
            }
            assertThat(Tracer.isTraceObservable()).isFalse();
            assertThat(local.get()).isEqualTo("value-" + attempt);
        }
        Tracer.fastCompleteSpan();
        assertNoTrace();
        assertThat(spans).hasSize(2);
    }

    @Test
    void doesNotCreateARequestIdWhenUpgradingAnExistingLocalTrace() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "local", SpanType.LOCAL);
        try (CloseableSpan ignored = Tracer.startSampledSpan("sampled", SpanType.SERVER_INCOMING)) {
            assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isNull();
            assertThat(Tracer.maybeGetTraceMetadata().orElseThrow().getRequestId())
                    .isEmpty();
        }
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void invalidSpanDoesNotReplaceTheCurrentTrace() {
        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        Trace original = Tracer.getTrace();
        assertThatThrownBy(() -> {
                    try (CloseableSpan ignored = Tracer.startSampledSpan(null)) {
                        throw new AssertionError("Expected validation to reject a null operation");
                    }
                })
                .isInstanceOf(NullPointerException.class);
        assertThat(Tracer.getTrace()).isSameAs(original);
        assertThat(Tracer.isTraceObservable()).isFalse();
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    @Test
    void initTraceStillIsolatesContextsEvenWhenTheTraceIdIsReused() {
        TraceLocal<String> local = TraceLocal.of();
        try (CloseableSpan ignored = Tracer.startSampledSpan("request")) {
            local.set("first-request");
        }
        try (CloseableSpan ignored = Tracer.startSampledSpan("next-request")) {
            assertThat(local.get()).isNull();
        }

        Tracer.initTraceWithSpan(Observability.DO_NOT_SAMPLE, "trace-id", "request", SpanType.SERVER_INCOMING);
        local.set("original");
        Detached original = DetachedSpan.detach();
        Tracer.initTraceWithSpan(Observability.SAMPLE, "trace-id", "replacement", SpanType.SERVER_INCOMING);
        assertThat(local.get()).isNull();
        try (CloseableSpan ignored = original.attach()) {
            assertThat(local.get()).isEqualTo("original");
        }
        Tracer.fastCompleteSpan();
        assertNoTrace();
    }

    private static void assertNoTrace() {
        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
        assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isNull();
    }
}
