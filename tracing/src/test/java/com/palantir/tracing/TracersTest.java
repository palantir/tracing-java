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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;

@SuppressWarnings("deprecation")
public final class TracersTest {

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        MDC.clear();

        Tracer.setSampler(AlwaysSampler.INSTANCE);
        // Initialize a new trace for each test
        Tracer.initTrace(Observability.UNDECIDED, "defaultTraceId");
    }

    @After
    public void after() {
        // Clear out the old trace from each test
        Tracer.getAndClearTraceIfPresent();
    }

    @Test
    public void testWrapExecutorService() throws Exception {
        ExecutorService wrappedService =
                Tracers.wrap(Executors.newSingleThreadExecutor());

        // Empty trace
        wrappedService.submit(traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)")).get();
        wrappedService.submit(traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)")).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.submit(traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)")).get();
        wrappedService.submit(traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)")).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapExecutorService_withSpan() throws Exception {
        ExecutorService wrappedService =
                Tracers.wrap("operation", Executors.newSingleThreadExecutor());

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
    }

    @Test
    public void testWrapScheduledExecutorService() throws Exception {
        ScheduledExecutorService wrappedService =
                Tracers.wrap(Executors.newSingleThreadScheduledExecutor());

        // Empty trace
        wrappedService.schedule(
                traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(
                traceExpectingRunnableWithSingleSpan("DeferredTracer(unnamed operation)"), 0, TimeUnit.SECONDS).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.schedule(
                traceExpectingCallableWithSingleSpan("DeferredTracer(unnamed operation)"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(
                traceExpectingRunnableWithSingleSpan("DeferredTracer(unnamed operation)"), 0, TimeUnit.SECONDS).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapScheduledExecutorService_withSpan() throws Exception {
        ScheduledExecutorService wrappedService =
                Tracers.wrap("operation", Executors.newSingleThreadScheduledExecutor());

        // Empty trace
        wrappedService.schedule(traceExpectingCallableWithSingleSpan("operation"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnableWithSingleSpan("operation"), 0, TimeUnit.SECONDS).get();

        // Non-empty trace
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        wrappedService.schedule(traceExpectingCallableWithSingleSpan("operation"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnableWithSingleSpan("operation"), 0, TimeUnit.SECONDS).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapExecutorServiceWithNewTrace() throws Exception {
        ExecutorService wrappedService =
                Tracers.wrapWithNewTrace("operation", Executors.newSingleThreadExecutor());

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
        Callable<Void> callable = Tracers.wrap(() -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("DeferredTracer(unnamed operation)");
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
        Runnable runnable = Tracers.wrap(() -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("DeferredTracer(unnamed operation)");
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
            assertThat(observed).hasSize(2);
            // Inner operation must complete first to avoid confusion
            assertThat(observed.get(0))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName + " initial");
            assertThat(observed.get(1))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span -> assertThat(span)
                            .extracting(Span::getTraceId)
                            .isEqualTo("defaultTraceId"));
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
            ListenableFuture<String> traced = Tracers.wrapListenableFuture(operationName,
                    () -> Futures.immediateFailedFuture(new SafeRuntimeException("result")));
            assertThat(traced).isDone();
            assertThat(observed).hasSize(2);
            // Inner operation must complete first to avoid confusion
            assertThat(observed.get(0))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName + " initial");
            assertThat(observed.get(1))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span -> assertThat(span)
                            .extracting(Span::getTraceId)
                            .isEqualTo("defaultTraceId"));
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
        // Inner operation has completed
        assertThat(observed).hasSize(1);
        assertThat(observed.get(0))
                .extracting(Span::getOperation)
                .isEqualTo(operationName + " initial");
        // Complete the future
        rawFuture.set("complete");
        assertThat(traced).isDone();
        assertThat(observed).hasSize(2);
        assertThat(observed.get(1))
                .extracting(Span::getOperation)
                .isEqualTo(operationName);
        assertThat(observed)
                .allSatisfy(span -> assertThat(span)
                        .extracting(Span::getTraceId)
                        .isEqualTo("defaultTraceId"));
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
            assertThat(observed).hasSize(2);
            assertThat(observed.get(0))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName + " initial");
            assertThat(observed.get(1))
                    .extracting(Span::getOperation)
                    .isEqualTo(operationName);
            assertThat(observed)
                    .allSatisfy(span -> assertThat(span)
                            .extracting(Span::getTraceId)
                            .isEqualTo("defaultTraceId"));
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

        assertThat(traceIdSecondCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCalls);

        assertThat(traceIdBeforeConstruction)
                .isEqualTo(traceIdAfterCalls);
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

        assertThatThrownBy(() -> wrappedCallable.call()).isInstanceOf(IllegalStateException.class);

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
        Callable<Boolean> unSampledCallable = Tracers.wrapWithNewTrace("someTraceId", Observability.DO_NOT_SAMPLE,
                rawCallable);

        assertThat(sampledCallable.call()).isTrue();
        assertThat(unSampledCallable.call()).isFalse();
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateInsideRunnableIsIsolated() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        List<String> traceIds = Lists.newArrayList();

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

        assertThat(traceIdSecondCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(traceIdAfterCalls);

        assertThat(traceIdBeforeConstruction)
                .isEqualTo(traceIdAfterCalls);
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateInsideRunnableHasSpan() throws Exception {
        List<List<OpenSpan>> spans = Lists.newArrayList();

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
        List<List<OpenSpan>> spans = Lists.newArrayList();

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

        assertThatThrownBy(() -> wrappedRunnable.run()).isInstanceOf(IllegalStateException.class);

        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapRunnableWithNewTrace_traceStateRestoredToCleared() {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithNewTrace(() -> {
            // no-op
        }).run();
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

        Runnable unSampledRunnable = Tracers.wrapWithNewTrace("someTraceId", Observability.DO_NOT_SAMPLE,
                rawUnSampledRunnable);

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
        List<List<OpenSpan>> spans = Lists.newArrayList();

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
        List<List<OpenSpan>> spans = Lists.newArrayList();

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

        assertThatThrownBy(() -> wrappedRunnable.run()).isInstanceOf(IllegalStateException.class);
        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_traceStateRestoredToCleared() {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();
        Tracers.wrapWithAlternateTraceId("someTraceId", () -> {
            // no-op
        }).run();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrapRunnableWithAlternateTraceId_canSpecifyObservability() {
        Runnable sampledRunnable = Tracers.wrapWithAlternateTraceId(
                "someTraceId",
                "operation",
                Observability.SAMPLE,
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isTrue());

        sampledRunnable.run();

        Runnable unSampledRunnable = Tracers.wrapWithAlternateTraceId(
                "someTraceId",
                "operation",
                Observability.DO_NOT_SAMPLE,
                () -> assertThat(Tracer.copyTrace().get().isObservable()).isFalse());

        unSampledRunnable.run();
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
     * n.b. When this test fails it may be fair to update the expected values if the new frames make sense, this
     * should not necessarily be a blocker, but a function to ensure we consider the consequences of future changes.
     * Keep in mind that Tracing is used heavily and will appear in stack traces for a lot of exceptions, where we
     * want to maintain high signal for quick debugging.
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
     * n.b. When this test fails it may be fair to update the expected values if the new frames make sense, this
     * should not necessarily be a blocker, but a function to ensure we consider the consequences of future changes.
     * Keep in mind that Tracing is used heavily and will appear in stack traces for a lot of exceptions, where we
     * want to maintain high signal for quick debugging.
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
        final String outsideTraceId = Tracer.getTraceId();

        return () -> {
            String traceId = Tracer.getTraceId();
            List<OpenSpan> trace = getCurrentTrace();
            OpenSpan span = trace.remove(trace.size() - 1);
            assertThat(trace).isEmpty();

            assertThat(traceId).isEqualTo(outsideTraceId);
            assertThat(span.getOperation()).isEqualTo(operation);
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(outsideTraceId);
            return null;
        };
    }

    private static Runnable traceExpectingRunnableWithSingleSpan(String operation) {
        final String outsideTraceId = Tracer.getTraceId();

        return () -> {
            String traceId = Tracer.getTraceId();
            List<OpenSpan> trace = getCurrentTrace();
            OpenSpan span = trace.remove(trace.size() - 1);
            assertThat(trace).isEmpty();

            assertThat(traceId).isEqualTo(outsideTraceId);
            assertThat(span.getOperation()).isEqualTo(operation);
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(outsideTraceId);
        };
    }

    private static List<OpenSpan> getCurrentTrace() {
        return Tracer.copyTrace().map(trace -> {
            List<OpenSpan> spans = Lists.newArrayList();
            while (!trace.isEmpty()) {
                spans.add(trace.pop().get());
            }
            return Lists.reverse(spans);
        }).orElseGet(Collections::emptyList);
    }
}
