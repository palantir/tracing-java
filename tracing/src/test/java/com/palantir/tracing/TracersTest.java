/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.logsafe.exceptions.SafeNullPointerException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

@SuppressWarnings("deprecation")
public final class TracersTest {

    @Before
    public void before() {
        MDC.clear();

        Tracer.setSampler(AlwaysSampler.INSTANCE);
        // Initialize a new trace for each test
        Tracer.initTraceWithSpan(
                Observability.SAMPLE, "defaultTraceId", Optional.of("forUserAgent"), "rootOperation", SpanType.LOCAL);
    }

    @After
    public void after() {
        // Clear out the old trace from each test
        Tracer.clearCurrentTrace();
    }

    @Test
    public void testWrapExecutorService() {
        withExecutor(() -> Tracers.wrap(Executors.newSingleThreadExecutor()), wrappedService -> {
            String subscription = UUID.randomUUID().toString();
            List<String> operations = new CopyOnWriteArrayList<>();
            Tracer.subscribe(subscription, span -> operations.add(span.getOperation()));
            try {
                String traceId = Tracer.getTraceId();
                wrappedService
                        .submit(() -> assertThat(Tracer.getTraceId()).isEqualTo(traceId))
                        .get();
                wrappedService
                        .submit(() -> assertThat(Tracer.getTraceId()).isEqualTo(traceId))
                        .get();

                Tracer.fastStartSpan("foo");
                Tracer.fastStartSpan("bar");
                Tracer.fastStartSpan("baz");
                wrappedService
                        .submit(() -> assertThat(Tracer.getTraceId()).isEqualTo(traceId))
                        .get();
                wrappedService
                        .submit(() -> assertThat(Tracer.getTraceId()).isEqualTo(traceId))
                        .get();
                Tracer.fastCompleteSpan();
                Tracer.fastCompleteSpan();
                Tracer.fastCompleteSpan();
                assertThat(operations).containsExactly("baz", "bar", "foo");
            } finally {
                Tracer.unsubscribe(subscription);
            }
        });
    }

    @Test
    public void testWrapExecutorService_withSpan() throws Exception {
        ExecutorService wrappedService = Tracers.wrap("operation", Executors.newSingleThreadExecutor());

        // Empty trace
        wrappedService.submit(traceExpectingCallableWithSingleSpan("operation")).get();
        wrappedService.submit(traceExpectingRunnableWithSingleSpan("operation")).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.submit(traceExpectingCallableWithSingleSpan("operation")).get();
        wrappedService.submit(traceExpectingRunnableWithSingleSpan("operation")).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();

        wrappedService.shutdownNow();
    }

    @Test
    public void testWrapExecutorService_withRequestId() throws Exception {
        ExecutorService wrappedService = Tracers.wrap("operation", Executors.newSingleThreadExecutor());

        // Non-empty trace
        Tracer.initTraceWithSpan(Observability.UNDECIDED, "traceId", "root", SpanType.SERVER_INCOMING);
        String requestId = Tracer.maybeGetTraceMetadata()
                .flatMap(TraceMetadata::getRequestId)
                .get();
        wrappedService
                .submit(traceExpectingCallableWithSingleSpan("operation", Optional.of(requestId)))
                .get();
        wrappedService
                .submit(traceExpectingRunnableWithSingleSpan("operation", Optional.of(requestId)))
                .get();
        Tracer.fastCompleteSpan();
        wrappedService.shutdownNow();
    }

    @Test
    public void testWrapScheduledExecutorService() {
        withExecutor(() -> Tracers.wrap("scheduler", Executors.newSingleThreadScheduledExecutor()), wrappedService -> {
            // Empty trace
            wrappedService
                    .schedule(traceExpectingCallableWithSingleSpan("scheduler"), 0, TimeUnit.SECONDS)
                    .get();
            wrappedService
                    .schedule(traceExpectingRunnableWithSingleSpan("scheduler"), 0, TimeUnit.SECONDS)
                    .get();

            // Non-empty trace
            Tracer.fastStartSpan("foo");
            Tracer.fastStartSpan("bar");
            Tracer.fastStartSpan("baz");
            wrappedService
                    .schedule(traceExpectingCallableWithSingleSpan("scheduler"), 0, TimeUnit.SECONDS)
                    .get();
            wrappedService
                    .schedule(traceExpectingRunnableWithSingleSpan("scheduler"), 0, TimeUnit.SECONDS)
                    .get();
            Tracer.fastCompleteSpan();
            Tracer.fastCompleteSpan();
            Tracer.fastCompleteSpan();
            wrappedService.shutdownNow();
        });
    }

    @Test
    public void testWrapScheduledExecutorService_scheduleAtFixedRate() {
        withExecutor(() -> Tracers.wrap(Executors.newSingleThreadScheduledExecutor()), wrappedService -> {
            SettableFuture<String> first = SettableFuture.create();
            SettableFuture<String> second = SettableFuture.create();
            Tracer.fastStartSpan("start");
            ScheduledFuture<?> scheduledFuture = wrappedService.scheduleAtFixedRate(
                    () -> {
                        String traceId = Tracer.getTraceId();
                        if (!first.set(traceId)) {
                            second.set(traceId);
                        }
                    },
                    0,
                    1,
                    TimeUnit.MILLISECONDS);
            String secondValue = second.get();
            scheduledFuture.cancel(true);
            assertThat(secondValue)
                    .describedAs("Scheduled task traceIds should be unique for each repetition")
                    .isNotEqualTo(Futures.getDone(first))
                    .describedAs("Scheduled task traceIds should be unique from the submitting thread")
                    .isNotEqualTo(Tracer.getTraceId());
            assertThat(Futures.getDone(first))
                    .describedAs("Scheduled task traceIds should be unique from the submitting thread")
                    .isNotEqualTo(Tracer.getTraceId());
            Tracer.fastCompleteSpan();
        });
    }

    @Test
    public void testWrapScheduledExecutorService_scheduleWithFixedDelay() {
        withExecutor(() -> Tracers.wrap(Executors.newSingleThreadScheduledExecutor()), wrappedService -> {
            SettableFuture<String> first = SettableFuture.create();
            SettableFuture<String> second = SettableFuture.create();
            Tracer.fastStartSpan("start");
            ScheduledFuture<?> scheduledFuture = wrappedService.scheduleWithFixedDelay(
                    () -> {
                        String traceId = Tracer.getTraceId();
                        if (!first.set(traceId)) {
                            second.set(traceId);
                        }
                    },
                    0,
                    1,
                    TimeUnit.MILLISECONDS);
            String secondValue = second.get();
            scheduledFuture.cancel(true);
            assertThat(secondValue)
                    .describedAs("Scheduled task traceIds should be unique for each repetition")
                    .isNotEqualTo(Futures.getDone(first))
                    .describedAs("Scheduled task traceIds should be unique from the submitting thread")
                    .isNotEqualTo(Tracer.getTraceId());
            assertThat(Futures.getDone(first))
                    .describedAs("Scheduled task traceIds should be unique from the submitting thread")
                    .isNotEqualTo(Tracer.getTraceId());
            Tracer.fastCompleteSpan();
        });
    }

    @Test
    public void testWrapScheduledExecutorService_withSpan() throws Exception {
        ScheduledExecutorService wrappedService =
                Tracers.wrap("operation", Executors.newSingleThreadScheduledExecutor());

        // Empty trace
        wrappedService
                .schedule(traceExpectingCallableWithSingleSpan("operation"), 0, TimeUnit.SECONDS)
                .get();
        wrappedService
                .schedule(traceExpectingRunnableWithSingleSpan("operation"), 0, TimeUnit.SECONDS)
                .get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService
                .schedule(traceExpectingCallableWithSingleSpan("operation"), 0, TimeUnit.SECONDS)
                .get();
        wrappedService
                .schedule(traceExpectingRunnableWithSingleSpan("operation"), 0, TimeUnit.SECONDS)
                .get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapExecutorServiceWithNewTrace() throws Exception {
        ExecutorService wrappedService = Tracers.wrapWithNewTrace("operation", Executors.newSingleThreadExecutor());

        Callable<Void> callable = newTraceExpectingCallable("operation");
        Runnable runnable = newTraceExpectingRunnable("operation");

        // Empty trace
        wrappedService.submit(callable).get();
        wrappedService.submit(runnable).get();

        wrappedService.submit(callable).get();
        wrappedService.submit(runnable).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.submit(callable).get();
        wrappedService.submit(runnable).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapScheduledExecutorServiceWithNewTrace() throws Exception {
        ScheduledExecutorService wrappedService =
                Tracers.wrapWithNewTrace("operation", Executors.newSingleThreadScheduledExecutor());

        Callable<Void> callable = newTraceExpectingCallable("operation");
        Runnable runnable = newTraceExpectingRunnable("operation");

        // Empty trace
        wrappedService.schedule(callable, 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(runnable, 0, TimeUnit.SECONDS).get();

        wrappedService.schedule(callable, 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(runnable, 0, TimeUnit.SECONDS).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.schedule(callable, 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(runnable, 0, TimeUnit.SECONDS).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapCallable_callableTraceIsIsolated() throws Exception {
        Tracer.fastStartSpan("outside");
        Callable<Void> callable = Tracers.wrap(() -> {
            Tracer.fastStartSpan("inside"); // never completed
            return null;
        });
        callable.call();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapCallable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.fastStartSpan("before-construction");
        Callable<Void> callable = Tracers.wrap("callable", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("callable");
            return null;
        });
        Tracer.fastStartSpan("after-construction");
        callable.call();
    }

    @Test
    public void testWrapCallable_withSpan() throws Exception {
        Tracer.fastStartSpan("outside");
        Runnable runnable = Tracers.wrap("operation", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("operation");
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapRunnable_runnableTraceIsIsolated() throws Exception {
        Tracer.fastStartSpan("outside");
        Runnable runnable = Tracers.wrap(() -> {
            Tracer.fastStartSpan("inside"); // never completed
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapRunnable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.fastStartSpan("before-construction");
        Runnable runnable = Tracers.wrap("runnable", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("runnable");
        });
        Tracer.fastStartSpan("after-construction");
        runnable.run();
    }

    @Test
    public void testWrapRunnable_startsNewSpan() throws Exception {
        Tracer.fastStartSpan("outside");
        Runnable runnable = Tracers.wrap("operation", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("operation");
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapFuture_immediate() {
        List<Span> observed = new ArrayList<>();
        Tracer.subscribe("futureTest", observed::add);
        try {
            String operationName = "testOperation";
            ListenableFuture<String> traced =
                    Tracers.wrapListenableFuture(operationName, () -> Futures.immediateFuture("result"));
            assertThat(traced).isDone();
            assertThat(observed).hasSize(1);
            assertThat(observed.get(0)).extracting(Span::getOperation).isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span ->
                            assertThat(span).extracting(Span::getTraceId).isEqualTo("defaultTraceId"));
        } finally {
            Tracer.unsubscribe("futureTest");
        }
    }

    @Test
    public void testWrapFuture_failure() {
        List<Span> observed = new ArrayList<>();
        Tracer.subscribe("futureTest", observed::add);
        try {
            String operationName = "testOperation";
            ListenableFuture<String> traced = Tracers.wrapListenableFuture(
                    operationName, () -> Futures.immediateFailedFuture(new SafeRuntimeException("result")));
            assertThat(traced).isDone();
            assertThat(observed).hasSize(1);
            assertThat(observed.get(0)).extracting(Span::getOperation).isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span ->
                            assertThat(span).extracting(Span::getTraceId).isEqualTo("defaultTraceId"));
        } finally {
            Tracer.unsubscribe("futureTest");
        }
    }

    @Test
    public void testWrapFuture_delayed() {
        List<Span> observed = new ArrayList<>();
        Tracer.subscribe("futureTest", observed::add);
        String operationName = "testOperation";
        SettableFuture<String> rawFuture = SettableFuture.create();
        SettableFuture<String> traced = Tracers.wrapListenableFuture(operationName, () -> rawFuture);
        assertThat(traced).isNotDone();
        // attached component has completed but does not emit any spans
        assertThat(observed).isEmpty();
        // Complete the future
        rawFuture.set("complete");
        assertThat(traced).isDone();
        assertThat(observed).hasSize(1);
        assertThat(observed.get(0)).extracting(Span::getOperation).isEqualTo(operationName);
        assertThat(observed)
                .allSatisfy(
                        span -> assertThat(span).extracting(Span::getTraceId).isEqualTo("defaultTraceId"));
    }

    @Test
    public void testWrapFuture_throws() {
        List<Span> observed = new ArrayList<>();
        Tracer.subscribe("futureTest", observed::add);
        try {
            String operationName = "testOperation";
            assertThatThrownBy(() -> Tracers.wrapListenableFuture(operationName, () -> {
                        // It's best if these cases result in a failed future, but we've found these in the
                        // wild so we must handle them properly.
                        throw new SafeRuntimeException("initial operation failure");
                    }))
                    .isInstanceOf(SafeRuntimeException.class)
                    .hasMessage("initial operation failure");
            assertThat(observed).hasSize(1);
            assertThat(observed.get(0)).extracting(Span::getOperation).isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span ->
                            assertThat(span).extracting(Span::getTraceId).isEqualTo("defaultTraceId"));
        } finally {
            Tracer.unsubscribe("futureTest");
        }
    }

    @Test
    public void testWrapFuture_returnNull() {
        assertThatThrownBy(() -> Tracers.wrapListenableFuture("operation", () -> null))
                .isInstanceOf(SafeNullPointerException.class)
                .hasMessage("Expected a ListenableFuture");
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableIsIsolated() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        Callable<String> wrappedCallable = Tracers.wrapWithNewTrace(Tracer::getTraceId);

        String traceIdFirstCall = wrappedCallable.call();
        String traceIdSecondCall = wrappedCallable.call();

        String traceIdAfterCalls = Tracer.getTraceId();

        assertThat(traceIdFirstCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCalls)
                .isNotEqualTo(traceIdSecondCall);

        assertThat(traceIdSecondCall).isNotEqualTo(traceIdBeforeConstruction).isNotEqualTo(traceIdAfterCalls);

        assertThat(traceIdBeforeConstruction).isEqualTo(traceIdAfterCalls);
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableHasSpan() throws Exception {
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithNewTrace(TracersTest::getCurrentTrace);

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("root");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableHasGivenSpan() throws Exception {
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithNewTrace("operation", TracersTest::getCurrentTrace);

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("operation");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateRestoredWhenThrows() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        Callable<String> wrappedCallable = Tracers.wrapWithNewTrace(() -> {
            throw new IllegalStateException();
        });

        assertThatThrownBy(wrappedCallable::call).isInstanceOf(IllegalStateException.class);

        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateRestoredToCleared() throws Exception {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithNewTrace(() -> null).call();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrapCallableWithNewTrace_canSpecifyObservability() throws Exception {
        Callable<Boolean> rawCallable = () -> {
            return Tracer.copyTrace().get().isObservable();
        };

        Callable<Boolean> sampledCallable = Tracers.wrapWithNewTrace("someTraceId", Observability.SAMPLE, rawCallable);
        Callable<Boolean> unSampledCallable =
                Tracers.wrapWithNewTrace("someTraceId", Observability.DO_NOT_SAMPLE, rawCallable);

        assertThat(sampledCallable.call()).isTrue();
        assertThat(unSampledCallable.call()).isFalse();
    }

    @Test
    public void testWrapCallableWithAlternateTraceId_traceStateInsideCallableUsesGivenTraceId() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();
        String traceIdToUse = "someTraceId";
        Callable<String> wrappedCallable = Tracers.wrapWithAlternateTraceId(
                traceIdToUse, "operation", Observability.UNDECIDED, Tracer::getTraceId);

        String traceIdInsideCallable = wrappedCallable.call();

        String traceIdAfterCall = Tracer.getTraceId();

        assertThat(traceIdInsideCallable)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCall)
                .isEqualTo(traceIdToUse);

        assertThat(traceIdBeforeConstruction).isEqualTo(traceIdAfterCall);
    }

    @Test
    public void testWrapCallableWithAlternateTraceId_traceStateInsideCallableHasSpan() throws Exception {
        String traceIdToUse = "someTraceId";
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithAlternateTraceId(
                traceIdToUse, "operation", Observability.UNDECIDED, TracersTest::getCurrentTrace);

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("operation");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapCallableWithAlternateTraceId_traceStateRestoredWhenThrows() {
        String traceIdBeforeConstruction = Tracer.getTraceId();
        Callable<Void> rawCallable = () -> {
            throw new IllegalStateException();
        };
        Callable<Void> wrappedCallable =
                Tracers.wrapWithAlternateTraceId("someTraceId", "operation", Observability.UNDECIDED, rawCallable);

        assertThatThrownBy(wrappedCallable::call).isInstanceOf(IllegalStateException.class);
        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapCallableWithAlternateTraceId_traceStateRestoredToCleared() throws Exception {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithAlternateTraceId("someTraceId", "operation", Observability.UNDECIDED, () -> {
                    // no-op
                    return null;
                })
                .call();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrapCallableWithAlternateTraceId_canSpecifyObservability() throws Exception {
        Callable<?> sampledCallable =
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isTrue();
        Callable<?> wrappedSampledCallable =
                Tracers.wrapWithAlternateTraceId("someTraceId", "operation", Observability.SAMPLE, sampledCallable);

        wrappedSampledCallable.call();

        Callable<?> unSampledCallable =
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isFalse();
        Callable<?> wrappedUnSampledCallable = Tracers.wrapWithAlternateTraceId(
                "someTraceId", "operation", Observability.DO_NOT_SAMPLE, unSampledCallable);

        wrappedUnSampledCallable.call();
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateInsideRunnableIsIsolated() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        List<String> traceIds = new ArrayList<>();

        Runnable wrappedRunnable = Tracers.wrapWithNewTrace(() -> {
            traceIds.add(Tracer.getTraceId());
        });

        wrappedRunnable.run();
        wrappedRunnable.run();

        String traceIdFirstCall = traceIds.get(0);
        String traceIdSecondCall = traceIds.get(1);

        String traceIdAfterCalls = Tracer.getTraceId();

        assertThat(traceIdFirstCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCalls)
                .isNotEqualTo(traceIdSecondCall);

        assertThat(traceIdSecondCall).isNotEqualTo(traceIdBeforeConstruction).isNotEqualTo(traceIdAfterCalls);

        assertThat(traceIdBeforeConstruction).isEqualTo(traceIdAfterCalls);
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateInsideRunnableHasSpan() throws Exception {
        List<List<OpenSpan>> spans = new ArrayList<>();

        Runnable wrappedRunnable = Tracers.wrapWithNewTrace(() -> {
            spans.add(getCurrentTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("root");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateInsideRunnableHasGivenSpan() throws Exception {
        List<List<OpenSpan>> spans = new ArrayList<>();

        Runnable wrappedRunnable = Tracers.wrapWithNewTrace("operation", () -> {
            spans.add(getCurrentTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("operation");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateRestoredWhenThrows() {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        Runnable rawRunnable = () -> {
            throw new IllegalStateException();
        };
        Runnable wrappedRunnable = Tracers.wrapWithNewTrace(rawRunnable);

        assertThatThrownBy(wrappedRunnable::run).isInstanceOf(IllegalStateException.class);

        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateRestoredToCleared() {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithNewTrace(() -> {
                    // no-op
                })
                .run();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrapRunnableWithNewTrace_canSpecifyObservability() {
        Runnable rawSampledRunnable = () -> {
            assertThat(Tracer.copyTrace().get().isObservable()).isTrue();
        };

        Runnable sampledRunnable = Tracers.wrapWithNewTrace("someTraceId", Observability.SAMPLE, rawSampledRunnable);

        sampledRunnable.run();

        Runnable rawUnSampledRunnable = () -> {
            assertThat(Tracer.copyTrace().get().isObservable()).isFalse();
        };

        Runnable unSampledRunnable =
                Tracers.wrapWithNewTrace("someTraceId", Observability.DO_NOT_SAMPLE, rawUnSampledRunnable);

        unSampledRunnable.run();
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateInsideRunnableUsesGivenTraceId() {
        String traceIdBeforeConstruction = Tracer.getTraceId();
        AtomicReference<String> traceId = new AtomicReference<>();
        String traceIdToUse = "someTraceId";
        Runnable wrappedRunnable = Tracers.wrapWithAlternateTraceId(traceIdToUse, () -> {
            traceId.set(Tracer.getTraceId());
        });

        wrappedRunnable.run();

        String traceIdAfterCall = Tracer.getTraceId();

        assertThat(traceId.get())
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCall)
                .isEqualTo(traceIdToUse);

        assertThat(traceIdBeforeConstruction).isEqualTo(traceIdAfterCall);
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateInsideRunnableHasSpan() {
        List<List<OpenSpan>> spans = new ArrayList<>();

        String traceIdToUse = "someTraceId";
        Runnable wrappedRunnable = Tracers.wrapWithAlternateTraceId(traceIdToUse, () -> {
            spans.add(getCurrentTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("root");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateInsideRunnableHasGivenSpan() {
        List<List<OpenSpan>> spans = new ArrayList<>();

        String traceIdToUse = "someTraceId";
        Runnable wrappedRunnable = Tracers.wrapWithAlternateTraceId(traceIdToUse, "operation", () -> {
            spans.add(getCurrentTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("operation");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateRestoredWhenThrows() {
        String traceIdBeforeConstruction = Tracer.getTraceId();
        Runnable rawRunnable = () -> {
            throw new IllegalStateException();
        };
        Runnable wrappedRunnable = Tracers.wrapWithAlternateTraceId("someTraceId", rawRunnable);

        assertThatThrownBy(wrappedRunnable::run).isInstanceOf(IllegalStateException.class);
        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateRestoredToCleared() {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithAlternateTraceId("someTraceId", () -> {
                    // no-op
                })
                .run();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_canSpecifyObservability() {
        Runnable sampledRunnable =
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isTrue();
        Runnable wrappedSampledRunnable =
                Tracers.wrapWithAlternateTraceId("someTraceId", "operation", Observability.SAMPLE, sampledRunnable);

        wrappedSampledRunnable.run();

        Runnable unSampledRunnable =
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isFalse();
        Runnable wrappedUnSampledRunnable = Tracers.wrapWithAlternateTraceId(
                "someTraceId", "operation", Observability.DO_NOT_SAMPLE, unSampledRunnable);

        wrappedUnSampledRunnable.run();
    }

    @Test
    public void testTraceIdGeneration() throws Exception {
        assertThat(Tracers.randomId()).hasSize(16); // fails with p=1/16 if generated string is not padded
        assertThat(Tracers.longToPaddedHex(0)).isEqualTo("0000000000000000");
        assertThat(Tracers.longToPaddedHex(42)).isEqualTo("000000000000002a");
        assertThat(Tracers.longToPaddedHex(-42)).isEqualTo("ffffffffffffffd6");
        assertThat(Tracers.longToPaddedHex(123456789L)).isEqualTo("00000000075bcd15");
    }

    /**
     * n.b. When this test fails it may be fair to update the expected values if the new frames make sense, this should
     * not necessarily be a blocker, but a function to ensure we consider the consequences of future changes. Keep in
     * mind that Tracing is used heavily and will appear in stack traces for a lot of exceptions, where we want to
     * maintain high signal for quick debugging.
     */
    @Test
    public void testExecutorStackDepth_callable() throws InterruptedException, ExecutionException {
        Callable<StackTraceElement[]> stackTraceCallable = () -> new Exception().getStackTrace();
        ExecutorService executor = Executors.newCachedThreadPool();
        ExecutorService tracedExecutor = Tracers.wrap(executor);
        try {
            Future<StackTraceElement[]> raw = executor.submit(stackTraceCallable);
            Future<StackTraceElement[]> traced = tracedExecutor.submit(stackTraceCallable);
            assertThat(traced.get())
                    .describedAs("Tracing should only add one additional stack frame")
                    .hasSize(raw.get().length + 1);
        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS))
                    .describedAs("Executor failed to shut down")
                    .isTrue();
        }
    }

    /**
     * n.b. When this test fails it may be fair to update the expected values if the new frames make sense, this should
     * not necessarily be a blocker, but a function to ensure we consider the consequences of future changes. Keep in
     * mind that Tracing is used heavily and will appear in stack traces for a lot of exceptions, where we want to
     * maintain high signal for quick debugging.
     */
    @Test
    public void testExecutorStackDepth_runnable() {
        // Specifically testing runnables which don't return a value like callable.
        AtomicReference<StackTraceElement[]> rawStackTrace = new AtomicReference<>();
        AtomicReference<StackTraceElement[]> tracedStackTrace = new AtomicReference<>();
        ExecutorService executor = Executors.newCachedThreadPool();
        ExecutorService tracedExecutor = Tracers.wrap(executor);
        try {
            executor.execute(() -> rawStackTrace.set(new Exception().getStackTrace()));
            tracedExecutor.execute(() -> tracedStackTrace.set(new Exception().getStackTrace()));

        } finally {
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS))
                    .describedAs("Executor failed to shut down")
                    .isTrue();
        }
        assertThat(tracedStackTrace.get())
                .describedAs("Tracing should only add one additional stack frame")
                .hasSize(rawStackTrace.get().length + 1);
    }

    @Test
    public void testAddTracingHeaders_populates() {
        Map<String, String> headers = new HashMap<>();
        TracingHeadersEnrichingFunction<Map<String, String>> enrichingFunction =
                (headerName, headerValue, state) -> state.put(headerName, headerValue);

        Tracers.addTracingHeaders(headers, enrichingFunction);

        assertThat(headers)
                .containsAllEntriesOf(Map.of(
                        "For-User-Agent", "forUserAgent",
                        "X-B3-TraceId", "defaultTraceId",
                        "X-B3-Sampled", "1"));
        assertThat(headers).containsKey("X-B3-SpanId");
    }

    private static Callable<Void> newTraceExpectingCallable(String expectedOperation) {
        final Set<String> seenTraceIds = new HashSet<>();
        seenTraceIds.add(Tracer.getTraceId());

        return () -> {
            String newTraceId = Tracer.getTraceId();
            List<OpenSpan> spans = getCurrentTrace();

            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(newTraceId);
            assertThat(seenTraceIds).doesNotContain(newTraceId);
            assertThat(spans).hasSize(1);
            assertThat(spans.get(0).getOperation()).isEqualTo(expectedOperation);
            assertThat(spans.get(0).getParentSpanId()).isEmpty();
            seenTraceIds.add(newTraceId);
            return null;
        };
    }

    private static Runnable newTraceExpectingRunnable(String expectedOperation) {
        final Set<String> seenTraceIds = new HashSet<>();
        seenTraceIds.add(Tracer.getTraceId());

        return () -> {
            String newTraceId = Tracer.getTraceId();
            List<OpenSpan> spans = getCurrentTrace();

            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(newTraceId);
            assertThat(seenTraceIds).doesNotContain(newTraceId);
            assertThat(spans).hasSize(1);
            assertThat(spans.get(0).getOperation()).isEqualTo(expectedOperation);
            assertThat(spans.get(0).getParentSpanId()).isEmpty();
            seenTraceIds.add(newTraceId);
        };
    }

    private static Callable<Void> traceExpectingCallableWithSingleSpan(String operation) {
        return traceExpectingCallableWithSingleSpan(operation, Optional.empty());
    }

    private static Callable<Void> traceExpectingCallableWithSingleSpan(
            String operation, Optional<String> expectedRequestId) {
        final String outsideTraceId = Tracer.getTraceId();

        return () -> {
            String traceId = Tracer.getTraceId();
            List<OpenSpan> trace = getCurrentTrace();
            OpenSpan span = trace.remove(trace.size() - 1);
            assertThat(trace).isEmpty();

            assertThat(traceId).isEqualTo(outsideTraceId);
            assertThat(span.getOperation()).isEqualTo(operation);
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(outsideTraceId);
            assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isEqualTo(expectedRequestId.orElse(null));
            return null;
        };
    }

    private static Runnable traceExpectingRunnableWithSingleSpan(String operation) {
        return traceExpectingRunnableWithSingleSpan(operation, Optional.empty());
    }

    private static Runnable traceExpectingRunnableWithSingleSpan(String operation, Optional<String> expectedRequestId) {
        final String outsideTraceId = Tracer.getTraceId();

        return () -> {
            String traceId = Tracer.getTraceId();
            List<OpenSpan> trace = getCurrentTrace();
            OpenSpan span = trace.remove(trace.size() - 1);
            assertThat(trace).isEmpty();

            assertThat(traceId).isEqualTo(outsideTraceId);
            assertThat(span.getOperation()).isEqualTo(operation);
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(outsideTraceId);
            assertThat(MDC.get(Tracers.REQUEST_ID_KEY)).isEqualTo(expectedRequestId.orElse(null));
        };
    }

    private static List<OpenSpan> getCurrentTrace() {
        return Tracer.copyTrace()
                .map(trace -> {
                    List<OpenSpan> spans = new ArrayList<>();
                    while (!trace.isEmpty()) {
                        spans.add(trace.pop().get());
                    }
                    return Lists.reverse(spans);
                })
                .orElseGet(Collections::emptyList);
    }

    private static <T extends ExecutorService> void withExecutor(Supplier<T> factory, ThrowingConsumer<T> test) {
        T executor = factory.get();
        try {
            test.accept(executor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            executor.shutdownNow();
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS))
                    .describedAs("Executor failed to shutdown within 5 seconds")
                    .isTrue();
        }
    }

    interface ThrowingConsumer<T> {
        void accept(T executor) throws Exception;
    }
}
