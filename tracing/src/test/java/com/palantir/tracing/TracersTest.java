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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
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

        // Initialize a new trace for each test
        Tracer.initTrace(Optional.empty(), "defaultTraceId");
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
        wrappedService.submit(traceExpectingCallableWithSingleSpan("deferred")).get();
        wrappedService.submit(traceExpectingCallableWithSingleSpan("deferred")).get();

        // Non-empty trace
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
        wrappedService.submit(traceExpectingCallableWithSingleSpan("baz")).get();
        wrappedService.submit(traceExpectingCallableWithSingleSpan("baz")).get();
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
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
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
        wrappedService.schedule(traceExpectingCallableWithSingleSpan("deferred"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnableWithSingleSpan("deferred"), 0, TimeUnit.SECONDS).get();

        // Non-empty trace
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
        wrappedService.schedule(traceExpectingCallableWithSingleSpan("baz"), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnableWithSingleSpan("baz"), 0, TimeUnit.SECONDS).get();
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
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
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
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
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
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
        wrappedService.schedule(callable, 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(runnable, 0, TimeUnit.SECONDS).get();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
        Tracer.fastCompleteSpan();
    }

    @Test
    public void testWrapCallable_callableTraceIsIsolated() throws Exception {
        Tracer.startSpan("outside");
        Callable<Void> callable = Tracers.wrap(() -> {
            Tracer.startSpan("inside"); // never completed
            return null;
        });
        callable.call();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapCallable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.startSpan("before-construction");
        Callable<Void> callable = Tracers.wrap(() -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
            return null;
        });
        Tracer.startSpan("after-construction");
        callable.call();
    }

    @Test
    public void testWrapCallable_withSpan() throws Exception {
        Tracer.startSpan("outside");
        Runnable runnable = Tracers.wrap("operation", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("operation");
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapRunnable_runnableTraceIsIsolated() throws Exception {
        Tracer.startSpan("outside");
        Runnable runnable = Tracers.wrap(() -> {
            Tracer.startSpan("inside"); // never completed
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapRunnable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.startSpan("before-construction");
        Runnable runnable = Tracers.wrap(() -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
        });
        Tracer.startSpan("after-construction");
        runnable.run();
    }

    @Test
    public void testWrapRunnable_startsNewSpan() throws Exception {
        Tracer.startSpan("outside");
        Runnable runnable = Tracers.wrap("operation", () -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("operation");
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableIsIsolated() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        Callable<String> wrappedCallable = Tracers.wrapWithNewTrace(() -> {
            return Tracer.getTraceId();
        });

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
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithNewTrace(() -> {
            return getCurrentTrace();
        });

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("root");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableHasGivenSpan() throws Exception {
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithNewTrace("operation", () -> {
            return getCurrentTrace();
        });

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
    public void testWrappingFutureCallback_futureCallbackTraceIsIsolated_success() throws Exception {
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> Tracer.startSpan("inside"));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> null);

        Tracer.startSpan("outside");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        // Using direct executor to use same thread to verify callback doesn't modify thread state
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isTrue();
        future.get();

        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingFutureCallback_futureCallbackTraceIsIsolated_failure() throws Exception {
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> Tracer.startSpan("inside"));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Tracer.startSpan("outside");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        // Using direct executor to use same thread to verify callback doesn't modify thread state
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isFalse();
        assertThatThrownBy(future::get).isInstanceOf(ExecutionException.class);

        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingFutureCallback_traceStateShowsCorrectlyParentedNewOperation_success_sameThread()
            throws Exception {
        AtomicReference<Span> span = new AtomicReference<>();
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> span.set(Tracer.completeSpan().get()));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> null);

        OpenSpan beforeSpan = Tracer.startSpan("before-construction");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isTrue();
        future.get();

        assertThat(span.get().getOperation()).isEqualTo("callback");
        assertThat(span.get().getParentSpanId().get()).isEqualTo(beforeSpan.getSpanId());
    }

    @Test
    public void testWrappingFutureCallback_traceStateShowsCorrectlyParentedNewOperation_success_differentThread()
            throws Exception {
        AtomicReference<Span> span = new AtomicReference<>();
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> span.set(Tracer.completeSpan().get()));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> null);

        OpenSpan beforeSpan = Tracer.startSpan("before-construction");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(future, callback, Executors.newSingleThreadExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isTrue();
        future.get();

        assertThat(span.get().getOperation()).isEqualTo("callback");
        assertThat(span.get().getParentSpanId().get()).isEqualTo(beforeSpan.getSpanId());
    }

    @Test
    public void testWrappingFutureCallback_traceStateShowsCorrectlyParentedNewOperation_failure_sameThread()
            throws Exception {
        AtomicReference<Span> span = new AtomicReference<>();
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> span.set(Tracer.completeSpan().get()));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        OpenSpan beforeSpan = Tracer.startSpan("before-construction");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isFalse();
        assertThatThrownBy(future::get).isInstanceOf(ExecutionException.class);

        assertThat(span.get().getOperation()).isEqualTo("callback");
        assertThat(span.get().getParentSpanId().get()).isEqualTo(beforeSpan.getSpanId());
    }

    @Test
    public void testWrappingFutureCallback_traceStateShowsCorrectlyParentedNewOperation_failure_differentThread()
            throws Exception {
        AtomicReference<Span> span = new AtomicReference<>();
        CompletionAwareFutureCallback<Void> futureCallback = createFutureCallbackWithFunction(
                () -> span.set(Tracer.completeSpan().get()));

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> future = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        OpenSpan beforeSpan = Tracer.startSpan("before-construction");
        FutureCallback<Void> callback = Tracers.wrap("callback", futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(future, callback, Executors.newSingleThreadExecutor());

        assertThat(futureCallback.waitForCompletion(10, TimeUnit.SECONDS)).isTrue();
        assertThat(futureCallback.wasSuccess()).isFalse();
        assertThatThrownBy(future::get).isInstanceOf(ExecutionException.class);

        assertThat(span.get().getOperation()).isEqualTo("callback");
        assertThat(span.get().getParentSpanId().get()).isEqualTo(beforeSpan.getSpanId());
    }

    @Test
    public void testTraceIdGeneration() throws Exception {
        assertThat(Tracers.randomId()).hasSize(16); // fails with p=1/16 if generated string is not padded
        assertThat(Tracers.longToPaddedHex(0)).isEqualTo("0000000000000000");
        assertThat(Tracers.longToPaddedHex(42)).isEqualTo("000000000000002a");
        assertThat(Tracers.longToPaddedHex(-42)).isEqualTo("ffffffffffffffd6");
        assertThat(Tracers.longToPaddedHex(123456789L)).isEqualTo("00000000075bcd15");
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
            assertThat(trace.size()).isEqualTo(0);

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
            assertThat(trace.size()).isEqualTo(0);

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
        }).orElse(Collections.emptyList());
    }

    private static <V> CompletionAwareFutureCallback<V> createFutureCallbackWithFunction(Runnable runnable) {
        return new CompletionAwareFutureCallback<>(new FutureCallback<V>() {
            @Override
            public void onSuccess(@NullableDecl V result) {
                runnable.run();
            }

            @Override
            public void onFailure(Throwable throwable) {
                runnable.run();
            }
        });
    }

    /**
     * Defines an interface for providing information on completion of execution and the ability to wait on completion
     * for use with tests involving FutureCallback.
     */
    private interface CompletionAware {
        boolean waitForCompletion(long timeout, TimeUnit unit) throws InterruptedException;
        boolean wasSuccess();
    }

    /**
     * There is no guarantee when a FutureCallback will actually run. In order to verify state from inside the
     * FutureCallback, a CountDownLatch is used to track when the FutureCallback has finished executing and to provide
     * a condition to wait on.
     */
    private static final class CompletionAwareFutureCallback<V> implements FutureCallback<V>, CompletionAware {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final FutureCallback<V> delegate;
        private AtomicReference<Boolean> success = new AtomicReference<>(Boolean.FALSE);

        CompletionAwareFutureCallback(FutureCallback<V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSuccess(@NullableDecl V result) {
            try {
                success.set(Boolean.TRUE);
                delegate.onSuccess(result);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            try {
                delegate.onFailure(throwable);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public boolean waitForCompletion(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        @Override
        public boolean wasSuccess() {
            return success.get();
        }
    }
}
