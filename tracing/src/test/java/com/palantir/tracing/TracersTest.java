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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
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
    public void testExecutorServiceWrapsCallables() throws Exception {
        ExecutorService wrappedService = Tracers.wrap(Executors.newSingleThreadExecutor());

        // Empty trace
        wrappedService.submit(traceExpectingCallable()).get();
        wrappedService.submit(traceExpectingRunnable()).get();

        // Non-empty trace
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
        wrappedService.submit(traceExpectingCallable()).get();
        wrappedService.submit(traceExpectingRunnable()).get();
        Tracer.completeSpan();
        Tracer.completeSpan();
        Tracer.completeSpan();
    }

    @Test
    public void testScheduledExecutorServiceWrapsCallables() throws Exception {
        ScheduledExecutorService wrappedService = Tracers.wrap(Executors.newSingleThreadScheduledExecutor());

        // Empty trace
        wrappedService.schedule(traceExpectingCallable(), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnable(), 0, TimeUnit.SECONDS).get();

        // Non-empty trace
        Tracer.startSpan("foo");
        Tracer.startSpan("bar");
        Tracer.startSpan("baz");
        wrappedService.schedule(traceExpectingCallable(), 0, TimeUnit.SECONDS).get();
        wrappedService.schedule(traceExpectingRunnable(), 0, TimeUnit.SECONDS).get();
        Tracer.completeSpan();
        Tracer.completeSpan();
        Tracer.completeSpan();
    }

    @Test
    public void testScheduledExecutorServiceWrapsCallablesWithNewTraces() throws Exception {
        ScheduledExecutorService wrappedService =
                Tracers.wrapWithNewTrace("operationToUse", Executors.newSingleThreadScheduledExecutor());

        Callable<Void> callable = newTraceExpectingCallable("operationToUse");
        Runnable runnable = newTraceExpectingRunnable("operationToUse");

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
        Tracer.completeSpan();
        Tracer.completeSpan();
        Tracer.completeSpan();
    }

    @Test
    public void testExecutorServiceWrapsCallablesWithNewTraces() throws Exception {
        ExecutorService wrappedService =
                Tracers.wrapWithNewTrace("operationToUse", Executors.newSingleThreadExecutor());

        Callable<Void> callable = newTraceExpectingCallable("operationToUse");
        Runnable runnable = newTraceExpectingRunnable("operationToUse");

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
        Tracer.completeSpan();
        Tracer.completeSpan();
        Tracer.completeSpan();
    }

    @Test
    public void testWrappingRunnable_runnableTraceIsIsolated() throws Exception {
        Tracer.startSpan("outside");
        Runnable runnable = Tracers.wrap(new Runnable() {
            @Override
            public void run() {
                Tracer.startSpan("inside"); // never completed
            }
        });
        runnable.run();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingRunnable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.startSpan("before-construction");
        Runnable runnable = Tracers.wrap(new Runnable() {
            @Override
            public void run() {
                assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
            }
        });
        Tracer.startSpan("after-construction");
        runnable.run();
    }

    @Test
    public void testWrappingCallable_callableTraceIsIsolated() throws Exception {
        Tracer.startSpan("outside");
        Callable<Void> runnable = Tracers.wrap(new Callable<Void>() {
            @Override
            public Void call() {
                Tracer.startSpan("inside"); // never completed
                return null;
            }
        });
        runnable.call();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingCallable_traceStateIsCapturedAtConstructionTime() throws Exception {
        Tracer.startSpan("before-construction");
        Callable<Void> callable = Tracers.wrap(new Callable<Void>() {
            @Override
            public Void call() {
                assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
                return null;
            }
        });
        Tracer.startSpan("after-construction");
        callable.call();
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
            return getCurrentFullTrace();
        });

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("root");
        assertThat(span.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapCallableWithNewTrace_traceStateInsideCallableHasGivenSpan() throws Exception {
        Callable<List<OpenSpan>> wrappedCallable = Tracers.wrapWithNewTrace("someOperation", () -> {
            return getCurrentFullTrace();
        });

        List<OpenSpan> spans = wrappedCallable.call();

        assertThat(spans).hasSize(1);

        OpenSpan span = spans.get(0);

        assertThat(span.getOperation()).isEqualTo("someOperation");
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
            spans.add(getCurrentFullTrace());
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

        Runnable wrappedRunnable = Tracers.wrapWithNewTrace("someOperation", () -> {
            spans.add(getCurrentFullTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("someOperation");
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
            spans.add(getCurrentFullTrace());
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
        Runnable wrappedRunnable = Tracers.wrapWithAlternateTraceId(traceIdToUse, "someOperation", () -> {
            spans.add(getCurrentFullTrace());
        });

        wrappedRunnable.run();

        assertThat(spans.get(0)).hasSize(1);

        OpenSpan span = spans.get(0).get(0);

        assertThat(span.getOperation()).isEqualTo("someOperation");
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
    public void testWrappingFutureCallback_futureCallbackTraceIsIsolated() throws Exception {
        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                Tracer.startSpan("inside"); // never completed
            }

            @Override
            public void onFailure(Throwable throwable) {
                Tracer.startSpan("inside"); // never completed
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Tracer.startSpan("outside");
        FutureCallback<Void> successCallback = Tracers.wrap(futureCallback);
        Futures.addCallback(success, successCallback, MoreExecutors.directExecutor());
        success.get();
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");

        Tracer.getAndClearTrace();
        Tracer.startSpan("outside");
        FutureCallback<Void> failureCallback = Tracers.wrap(futureCallback);
        Futures.addCallback(failure, failureCallback, MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingFutureCallback_traceStateIsCapturedAtConstructionTime() throws Exception {
        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
            }

            @Override
            public void onFailure(Throwable throwable) {
                assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("before-construction");
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Tracer.startSpan("before-construction");
        FutureCallback<Void> successCallback = Tracers.wrap(futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(success, successCallback, MoreExecutors.directExecutor());
        success.get();

        Tracer.startSpan("before-construction");
        FutureCallback<Void> failureCallback = Tracers.wrap(futureCallback);
        Tracer.startSpan("after-construction");
        Futures.addCallback(failure, failureCallback, MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
    }

    @Test
    public void testWrapFutureCallbackWithNewTrace_traceStateInsideFutureCallbackIsIsolated() throws Exception {
        List<String> traceIds = Lists.newArrayList();

        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                traceIds.add(Tracer.getTraceId());
            }

            @Override
            public void onFailure(Throwable throwable) {
                traceIds.add(Tracer.getTraceId());
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        String traceIdBeforeConstruction = Tracer.getTraceId();
        Futures.addCallback(success, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        success.get();
        Futures.addCallback(success, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        success.get();

        String successTraceIdFirstCall = traceIds.get(0);
        String successTraceIdSecondCall = traceIds.get(1);

        String successTraceIdAfterCalls = Tracer.getTraceId();

        assertThat(successTraceIdFirstCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(successTraceIdAfterCalls)
                .isNotEqualTo(successTraceIdSecondCall);

        assertThat(successTraceIdSecondCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(successTraceIdAfterCalls);

        assertThat(traceIdBeforeConstruction)
                .isEqualTo(successTraceIdAfterCalls);

        traceIds.clear();

        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);

        String failureTraceIdFirstCall = traceIds.get(0);
        String failureTraceIdSecondCall = traceIds.get(1);

        String failureTraceIdAfterCalls = Tracer.getTraceId();

        assertThat(failureTraceIdFirstCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(failureTraceIdAfterCalls)
                .isNotEqualTo(failureTraceIdSecondCall);

        assertThat(failureTraceIdSecondCall)
                .isNotEqualTo(traceIdBeforeConstruction)
                .isNotEqualTo(failureTraceIdAfterCalls);

        assertThat(traceIdBeforeConstruction)
                .isEqualTo(failureTraceIdAfterCalls);
    }

    @Test
    public void testWrapFutureCallbackWithNewTrace_traceStateInsideFutureCallbackHasSpan() throws Exception {
        List<List<OpenSpan>> spans = Lists.newArrayList();

        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                spans.add(getCurrentFullTrace());
            }

            @Override
            public void onFailure(Throwable throwable) {
                spans.add(getCurrentFullTrace());
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Futures.addCallback(success, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        success.get();
        List<OpenSpan> successSpans = spans.get(0);
        assertThat(successSpans).hasSize(1);
        OpenSpan successSpan = successSpans.get(0);
        assertThat(successSpan.getOperation()).isEqualTo("root");
        assertThat(successSpan.getParentSpanId()).isEmpty();

        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        List<OpenSpan> failureSpans = spans.get(1);
        assertThat(failureSpans).hasSize(1);
        OpenSpan failureSpan = failureSpans.get(0);
        assertThat(failureSpan.getOperation()).isEqualTo("root");
        assertThat(failureSpan.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapFutureCallbackWithNewTrace_traceStateInsideFutureCallbackHasGivenSpan() throws Exception {
        List<List<OpenSpan>> spans = Lists.newArrayList();

        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                spans.add(getCurrentFullTrace());
            }

            @Override
            public void onFailure(Throwable throwable) {
                spans.add(getCurrentFullTrace());
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Futures.addCallback(success, Tracers.wrapWithNewTrace("someOperation", futureCallback),
                MoreExecutors.directExecutor());
        success.get();
        List<OpenSpan> successSpans = spans.get(0);
        assertThat(successSpans).hasSize(1);
        OpenSpan successSpan = successSpans.get(0);
        assertThat(successSpan.getOperation()).isEqualTo("someOperation");
        assertThat(successSpan.getParentSpanId()).isEmpty();

        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        List<OpenSpan> failureSpans = spans.get(0);
        assertThat(failureSpans).hasSize(1);
        OpenSpan failureSpan = failureSpans.get(0);
        assertThat(failureSpan.getOperation()).isEqualTo("someOperation");
        assertThat(failureSpan.getParentSpanId()).isEmpty();
    }

    @Test
    public void testWrapFutureCallbackWithNewTrace_traceStateRestoredWhenThrows() throws Exception {
        String traceIdBeforeConstruction = Tracer.getTraceId();

        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                throw new IllegalStateException();
            }

            @Override
            public void onFailure(Throwable throwable) {
                throw new IllegalStateException();
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Futures.addCallback(success, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        success.get();
        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);

        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        assertThat(Tracer.getTraceId()).isEqualTo(traceIdBeforeConstruction);
    }

    @Test
    public void testWrapFutureCallbackWithNewTrace_traceStateRestoredToCleared() throws Exception {
        // Clear out the default initialized trace
        Tracer.getAndClearTraceIfPresent();

        FutureCallback<Void> futureCallback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(@NullableDecl Void result) {
                Tracer.startSpan("inside");
            }

            @Override
            public void onFailure(Throwable throwable) {
                Tracer.startSpan("inside");
            }
        };

        ListeningExecutorService listeningExecutorService =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListenableFuture<Void> success = listeningExecutorService.submit(() -> null);
        ListenableFuture<Void> failure = listeningExecutorService.submit(() -> {
            throw new IllegalStateException();
        });

        Futures.addCallback(success, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        success.get();
        assertThat(Tracer.hasTraceId()).isFalse();

        Futures.addCallback(failure, Tracers.wrapWithNewTrace(futureCallback), MoreExecutors.directExecutor());
        assertThatThrownBy(failure::get).isInstanceOf(ExecutionException.class);
        assertThat(Tracer.hasTraceId()).isFalse();
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

        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String newTraceId = Tracer.getTraceId();
                List<OpenSpan> spans = getCurrentFullTrace();

                assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(newTraceId);
                assertThat(seenTraceIds).doesNotContain(newTraceId);
                assertThat(spans).hasSize(1);
                assertThat(spans.get(0).getOperation()).isEqualTo(expectedOperation);
                assertThat(spans.get(0).getParentSpanId()).isEmpty();
                seenTraceIds.add(newTraceId);
                return null;
            }
        };
    }

    private static Runnable newTraceExpectingRunnable(String expectedOperation) {
        final Set<String> seenTraceIds = new HashSet<>();
        seenTraceIds.add(Tracer.getTraceId());

        return new Runnable() {
            @Override
            public void run() {
                String newTraceId = Tracer.getTraceId();
                List<OpenSpan> spans = getCurrentFullTrace();

                assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(newTraceId);
                assertThat(seenTraceIds).doesNotContain(newTraceId);
                assertThat(spans).hasSize(1);
                assertThat(spans.get(0).getOperation()).isEqualTo(expectedOperation);
                assertThat(spans.get(0).getParentSpanId()).isEmpty();
                seenTraceIds.add(newTraceId);
            }
        };
    }

    private static Callable<Void> traceExpectingCallable() {
        final String expectedTraceId = Tracer.getTraceId();
        final List<OpenSpan> expectedTrace = getCurrentFullTrace();
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertThat(Tracer.getTraceId()).isEqualTo(expectedTraceId);
                assertThat(getCurrentFullTrace()).isEqualTo(expectedTrace);
                assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(expectedTraceId);
                return null;
            }
        };
    }

    private static Runnable traceExpectingRunnable() {
        final String expectedTraceId = Tracer.getTraceId();
        final List<OpenSpan> expectedTrace = getCurrentFullTrace();
        return new Runnable() {
            @Override
            public void run() {
                assertThat(Tracer.getTraceId()).isEqualTo(expectedTraceId);
                assertThat(getCurrentFullTrace()).isEqualTo(expectedTrace);
                assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(expectedTraceId);
            }
        };
    }

    private static List<OpenSpan> getCurrentFullTrace() {
        return Tracer.copyTrace().map(trace -> {
            List<OpenSpan> spans = Lists.newArrayList();
            while (!trace.isEmpty()) {
                spans.add(trace.pop().get());
            }
            return spans;
        }).orElse(Collections.emptyList());
    }
}
