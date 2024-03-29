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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Utility methods for making {@link ExecutorService} and {@link Runnable} instances tracing-aware.
 */
public final class Tracers {
    /**
     * The key under which trace ids are inserted into SLF4J {@link org.slf4j.MDC MDCs}.
     */
    public static final String TRACE_ID_KEY = "traceId";
    /**
     * The key under which trace sampling state are inserted into SLF4J {@link org.slf4j.MDC MDCs}. If present, the
     * field can take the values of "1" or "0", where "1" indicates the trace was sampled.
     */
    public static final String TRACE_SAMPLED_KEY = "_sampled";

    /**
     * The key under which tracing request ids are inserted into SLF4J {@link org.slf4j.MDC MDCs}.
     */
    public static final String REQUEST_ID_KEY = "_requestId";

    private static final String DEFAULT_ROOT_SPAN_OPERATION = "root";
    private static final char[] HEX_DIGITS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    private Tracers() {}

    /**
     * Returns a random ID suitable for span and trace IDs.
     */
    public static String randomId() {
        return longToPaddedHex(ThreadLocalRandom.current().nextLong());
    }

    /**
     * Convert a long to a big-endian hex string. Hand-coded implementation is more efficient than
     * Strings.pad(Long.toHexString) because that code has to deal with mixed length longs, and then mixed length
     * amounts of padding - we want to minimise the overhead of tracing.
     */
    static String longToPaddedHex(long number) {
        char[] data = new char[16];
        data[0] = HEX_DIGITS[(int) ((number >> 60) & 0xF)];
        data[1] = HEX_DIGITS[(int) ((number >> 56) & 0xF)];
        data[2] = HEX_DIGITS[(int) ((number >> 52) & 0xF)];
        data[3] = HEX_DIGITS[(int) ((number >> 48) & 0xF)];
        data[4] = HEX_DIGITS[(int) ((number >> 44) & 0xF)];
        data[5] = HEX_DIGITS[(int) ((number >> 40) & 0xF)];
        data[6] = HEX_DIGITS[(int) ((number >> 36) & 0xF)];
        data[7] = HEX_DIGITS[(int) ((number >> 32) & 0xF)];
        data[8] = HEX_DIGITS[(int) ((number >> 28) & 0xF)];
        data[9] = HEX_DIGITS[(int) ((number >> 24) & 0xF)];
        data[10] = HEX_DIGITS[(int) ((number >> 20) & 0xF)];
        data[11] = HEX_DIGITS[(int) ((number >> 16) & 0xF)];
        data[12] = HEX_DIGITS[(int) ((number >> 12) & 0xF)];
        data[13] = HEX_DIGITS[(int) ((number >> 8) & 0xF)];
        data[14] = HEX_DIGITS[(int) ((number >> 4) & 0xF)];
        data[15] = HEX_DIGITS[(int) ((number >> 0) & 0xF)];
        return new String(data);
    }

    /**
     * Wraps the provided executor service such that any submitted {@link Callable} (or {@link Runnable}) is
     * {@link #wrap wrapped} in order to be trace-aware.
     */
    public static ExecutorService wrap(ExecutorService executorService) {
        return new WrappingExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrap(command);
            }
        };
    }

    /**
     * Like {@link #wrap(ExecutorService)}, but using the given {@link String operation} is used to create a span for
     * submitted tasks.
     */
    public static ExecutorService wrap(String operation, ExecutorService executorService) {
        return new WrappingExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(operation, callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrap(operation, command);
            }
        };
    }

    /**
     * Wraps the provided scheduled executor service to make submitted tasks traceable, see
     * {@link #wrap(ScheduledExecutorService)}. This method should not be used to wrap a ScheduledExecutorService that
     * has already been {@link #wrapWithNewTrace(ScheduledExecutorService) wrapped with new trace}. If this is done, a
     * new trace will be generated for each execution, effectively bypassing the intent of this method.
     */
    public static ScheduledExecutorService wrap(ScheduledExecutorService executorService) {
        return new WrappingScheduledExecutorService(executorService) {
            @Override
            protected Runnable wrapRecurring(Runnable runnable) {
                return wrapWithNewTrace(runnable);
            }

            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrap(command);
            }
        };
    }

    /**
     * Like {@link #wrap(ScheduledExecutorService)}, but using the given {@link String operation} is used to create a
     * span for submitted tasks.
     */
    public static ScheduledExecutorService wrap(String operation, ScheduledExecutorService executorService) {
        return new WrappingScheduledExecutorService(executorService) {
            @Override
            protected Runnable wrapRecurring(Runnable runnable) {
                return wrapWithNewTrace(operation, runnable);
            }

            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(operation, callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrap(operation, command);
            }
        };
    }

    /**
     * Wraps the given {@link Callable} such that it uses the thread-local {@link Trace tracing state} at the time of
     * it's construction during its {@link Callable#call() execution}.
     */
    public static <V> Callable<V> wrap(Callable<V> delegate) {
        return new AnonymousTracingAwareCallable<>(delegate);
    }

    /**
     * Like {@link #wrap(Callable)}, but using the given {@link String operation} is used to create a span for the
     * execution.
     */
    public static <V> Callable<V> wrap(String operation, Callable<V> delegate) {
        return new TracingAwareCallable<>(operation, ImmutableMap.of(), delegate);
    }

    /**
     * Like {@link #wrap(String, Callable)}, but using the given {@link String metadata} is used to create a span for
     * the execution.
     */
    public static <V> Callable<V> wrap(String operation, Map<String, String> metadata, Callable<V> delegate) {
        return new TracingAwareCallable<>(operation, metadata, delegate);
    }

    /**
     * Wraps the given {@link Runnable} such that it uses the thread-local {@link Trace tracing state} at the time of
     * it's construction during its {@link Runnable#run()} execution}.
     */
    public static Runnable wrap(Runnable delegate) {
        return new AnonymousTracingAwareRunnable(delegate);
    }

    /**
     * Like {@link #wrap(Runnable)}, but the given {@link String operation} is used to create a span for the
     * execution.
     */
    public static Runnable wrap(String operation, Runnable delegate) {
        return wrap(operation, ImmutableMap.of(), delegate);
    }

    /**
     * Like {@link #wrap(String, Runnable)}, but using the given {@link String operation} and
     * {@link Map metadata} is used to create a span for the
     * execution.
     */
    public static Runnable wrap(String operation, Map<String, String> metadata, Runnable delegate) {
        return new TracingAwareRunnable(operation, metadata, delegate);
    }

    /**
     * Traces the the execution of the {@code delegateFactory}, completing a span when the returned
     * {@link ListenableFuture#isDone() ListenableFuture is done}.
     * <p>
     * Example usage:
     * <pre>{@code
     * ListenableFuture<Result> future = Tracers.wrapListenableFuture(
     *     "remote operation",
     *     () -> retrofitClient.doRequest());
     * }</pre>
     * <p>
     * Note that this function is not named {@code wrap} in order to avoid conflicting with potential utility methods
     * for {@link Supplier suppliers}.
     */
    public static <T, U extends ListenableFuture<T>> U wrapListenableFuture(
            String operation, Supplier<U> delegateFactory) {
        return wrapListenableFuture(operation, ImmutableMap.of(), delegateFactory);
    }

    public static <T, U extends ListenableFuture<T>> U wrapListenableFuture(
            String operation, Map<String, String> metadata, Supplier<U> delegateFactory) {
        DetachedSpan span = DetachedSpan.start(operation);
        U result = null;
        // n.b. This span is required to apply tracing thread state to an initial request. Otherwise if there is
        // no active trace, the detached span would not be associated with work initiated by delegateFactory.
        try (CloseableSpan ignored = span.attach()) {
            result = Preconditions.checkNotNull(delegateFactory.get(), "Expected a ListenableFuture");
        } finally {
            if (result != null) {
                // In the successful case we add a listener in the finally block to prevent confusing traces
                // when delegateFactory returns a completed future. This way the detached span cannot complete
                // prior to its child.
                result.addListener(new ListenableFutureSpanListener(span, metadata), MoreExecutors.directExecutor());
            } else {
                // Complete the detached span, even if the delegateFactory throws.
                span.complete(MapTagTranslator.INSTANCE, metadata);
            }
        }
        return result;
    }

    public static <T> void addTracingHeaders(
            T state, TracingHeadersEnrichingFunction<T> tracingHeadersEnrichingFunction) {
        Optional<TraceMetadata> maybeTraceMetadata = Tracer.maybeGetTraceMetadata();
        if (maybeTraceMetadata.isEmpty()) {
            return;
        }
        TraceMetadata traceMetadata = maybeTraceMetadata.get();
        tracingHeadersEnrichingFunction.addHeader(TraceHttpHeaders.TRACE_ID, traceMetadata.getTraceId(), state);
        tracingHeadersEnrichingFunction.addHeader(TraceHttpHeaders.SPAN_ID, traceMetadata.getSpanId(), state);
        tracingHeadersEnrichingFunction.addHeader(
                TraceHttpHeaders.IS_SAMPLED, Tracer.isTraceObservable() ? "1" : "0", state);
        Optional<String> forUserAgent = Tracer.getForUserAgent();
        if (forUserAgent.isPresent()) {
            tracingHeadersEnrichingFunction.addHeader(TraceHttpHeaders.FOR_USER_AGENT, forUserAgent.get(), state);
        }
    }

    private static final class ListenableFutureSpanListener implements Runnable {

        private final DetachedSpan span;
        private final Map<String, String> metadata;

        ListenableFutureSpanListener(DetachedSpan span, Map<String, String> metadata) {
            this.span = span;
            this.metadata = metadata;
        }

        @Override
        public void run() {
            span.complete(MapTagTranslator.INSTANCE, metadata);
        }

        @Override
        public String toString() {
            return "ListenableFutureSpanListener{span=" + span + ", metadata=" + metadata + '}';
        }
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #wrapWithNewTrace(String, ExecutorService)}
     */
    @Deprecated
    public static ExecutorService wrapWithNewTrace(ExecutorService executorService) {
        return wrapWithNewTrace(DEFAULT_ROOT_SPAN_OPERATION, executorService);
    }

    /**
     * Wraps the provided executor service to make submitted tasks traceable with a fresh {@link Trace trace} for each
     * execution, see {@link #wrapWithNewTrace(String, ExecutorService)}. This method should not be used to wrap a
     * ScheduledExecutorService that has already been {@link #wrap(ExecutorService) wrapped}. If this is done, a new
     * trace will be generated for each execution, effectively bypassing the intent of the previous wrapping. The given
     * {@link String operation} is used to create the initial span.
     */
    public static ExecutorService wrapWithNewTrace(String operation, ExecutorService executorService) {
        return new WrappingExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrapWithNewTrace(operation, callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrapWithNewTrace(operation, command);
            }
        };
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #wrapWithNewTrace(String, ScheduledExecutorService)}
     */
    @Deprecated
    public static ScheduledExecutorService wrapWithNewTrace(ScheduledExecutorService executorService) {
        return wrapWithNewTrace(DEFAULT_ROOT_SPAN_OPERATION, executorService);
    }

    /**
     * Wraps the provided scheduled executor service to make submitted tasks traceable with a fresh {@link Trace trace}
     * for each execution, see {@link #wrapWithNewTrace(String, ScheduledExecutorService)}. This method should not be
     * used to wrap a ScheduledExecutorService that has already been {@link #wrap(ScheduledExecutorService) wrapped}. If
     * this is done, a new trace will be generated for each execution, effectively bypassing the intent of the previous
     * wrapping. The given {@link String operation} is used to create the initial span.
     */
    public static ScheduledExecutorService wrapWithNewTrace(
            String operation, ScheduledExecutorService executorService) {
        return new WrappingScheduledExecutorService(executorService) {
            @Override
            protected Runnable wrapRecurring(Runnable runnable) {
                return wrapTask(runnable);
            }

            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrapWithNewTrace(operation, callable);
            }

            @Override
            protected Runnable wrapTask(Runnable command) {
                return wrapWithNewTrace(operation, command);
            }
        };
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #wrapWithNewTrace(String, Callable)}
     */
    @Deprecated
    public static <V> Callable<V> wrapWithNewTrace(Callable<V> delegate) {
        return wrapWithNewTrace(DEFAULT_ROOT_SPAN_OPERATION, delegate);
    }

    public static <V> Callable<V> wrapWithNewTrace(String operation, Callable<V> delegate) {
        return wrapWithNewTrace(operation, Observability.UNDECIDED, delegate);
    }

    /**
     * Wraps the given {@link Callable} such that it creates a fresh {@link Trace tracing state} for its execution. That
     * is, the trace during its {@link Callable#call() execution} is entirely separate from the trace at construction or
     * any trace already set on the thread used to execute the callable. Each execution of the callable will have a
     * fresh trace. The given {@link String operation} is used to create the initial span.
     */
    public static <V> Callable<V> wrapWithNewTrace(
            String operation, Observability observability, Callable<V> delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTraceWithSpan(observability, Tracers.randomId(), operation, SpanType.LOCAL);
                return delegate.call();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #wrapWithNewTrace(String, Runnable)}
     */
    @Deprecated
    public static Runnable wrapWithNewTrace(Runnable delegate) {
        return wrapWithNewTrace(DEFAULT_ROOT_SPAN_OPERATION, delegate);
    }

    public static Runnable wrapWithNewTrace(String operation, Runnable delegate) {
        return wrapWithNewTrace(operation, Observability.UNDECIDED, delegate);
    }

    /**
     * Wraps the given {@link Runnable} such that it creates a fresh {@link Trace tracing state} for its execution. That
     * is, the trace during its {@link Runnable#run() execution} is entirely separate from the trace at construction or
     * any trace already set on the thread used to execute the runnable. Each execution of the runnable will have a
     * fresh trace. The given {@link String operation} is used to create the initial span.
     */
    public static Runnable wrapWithNewTrace(String operation, Observability observability, Runnable delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTraceWithSpan(observability, Tracers.randomId(), operation, SpanType.LOCAL);
                delegate.run();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Wraps the given {@link Callable} such that it creates a fresh {@link Trace tracing state with the given traceId}
     * for its execution. That is, the trace during its {@link Callable#call() execution} will use the traceId provided
     * instead of any trace already set on the thread used to execute the callable. Each execution of the callable will
     * use a new {@link Trace tracing state} with the same given traceId. The given {@link String operation} is used to
     * create the initial span.
     */
    public static <V> Callable<V> wrapWithAlternateTraceId(
            String traceId, String operation, Observability observability, Callable<V> delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTraceWithSpan(observability, traceId, operation, SpanType.LOCAL);
                return delegate.call();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #wrapWithAlternateTraceId(String, String, Runnable)}
     */
    @Deprecated
    public static Runnable wrapWithAlternateTraceId(String traceId, Runnable delegate) {
        return wrapWithAlternateTraceId(traceId, DEFAULT_ROOT_SPAN_OPERATION, delegate);
    }

    public static Runnable wrapWithAlternateTraceId(String traceId, String operation, Runnable delegate) {
        return wrapWithAlternateTraceId(traceId, operation, Observability.UNDECIDED, delegate);
    }

    /**
     * Wraps the given {@link Runnable} such that it creates a fresh {@link Trace tracing state with the given traceId}
     * for its execution. That is, the trace during its {@link Runnable#run() execution} will use the traceId provided
     * instead of any trace already set on the thread used to execute the runnable. Each execution of the runnable will
     * use a new {@link Trace tracing state} with the same given traceId. The given {@link String operation} is used to
     * create the initial span.
     */
    public static Runnable wrapWithAlternateTraceId(
            String traceId, String operation, Observability observability, Runnable delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTraceWithSpan(observability, traceId, operation, SpanType.LOCAL);
                delegate.run();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Restores or clears trace state based on provided {@link Trace}. Used to cleanup trace state for
     * {@link #wrapWithNewTrace} calls.
     */
    private static void restoreTrace(Optional<Trace> trace) {
        if (trace.isPresent()) {
            Tracer.setTrace(trace.get());
        } else {
            // Ignoring returned value, used to clear trace only
            Tracer.getAndClearTraceIfPresent();
        }
    }

    /**
     * Wraps a given callable such that its execution operates with the {@link Trace thread-local Trace} of the thread
     * that constructs the {@link TracingAwareCallable} instance rather than the thread that executes the callable.
     *
     * <p>The constructor is typically called by a tracing-aware executor service on the same thread on which a user
     * creates {@link Callable delegate}, and the {@link #call()} method is executed on an arbitrary (likely different)
     * thread with different {@link Trace tracing state}. In order to execute the task with the original (and
     * intuitively expected) tracing state, we remember the original state and set it for the duration of the
     * {@link #call() execution}.
     */
    private static class TracingAwareCallable<V> implements Callable<V> {
        private final Callable<V> delegate;
        private final Detached detached;
        private final String operation;
        private final Map<String, String> metadata;

        TracingAwareCallable(String operation, Map<String, String> metadata, Callable<V> delegate) {
            this.delegate = delegate;
            this.detached = DetachedSpan.detach();
            this.operation = operation;
            this.metadata = metadata;
        }

        @Override
        public V call() throws Exception {
            try (CloseableSpan ignored = detached.childSpan(operation, metadata)) {
                return delegate.call();
            }
        }
    }

    private static class AnonymousTracingAwareCallable<V> implements Callable<V> {
        private final Callable<V> delegate;
        private final Detached detached;

        AnonymousTracingAwareCallable(Callable<V> delegate) {
            this.delegate = delegate;
            this.detached = DetachedSpan.detach();
        }

        @Override
        public V call() throws Exception {
            try (CloseableSpan ignored = detached.attach()) {
                return delegate.call();
            }
        }
    }

    /**
     * Wraps a given runnable such that its execution operates with the {@link Trace thread-local Trace} of the thread
     * that constructs the {@link TracingAwareRunnable} instance rather than the thread that executes the runnable.
     */
    private static class TracingAwareRunnable implements Runnable {
        private final Runnable delegate;
        private final Detached detached;
        private final String operation;
        private final Map<String, String> metadata;

        TracingAwareRunnable(String operation, Map<String, String> metadata, Runnable delegate) {
            this.delegate = delegate;
            this.detached = DetachedSpan.detach();
            this.operation = operation;
            this.metadata = metadata;
        }

        @Override
        public void run() {
            try (CloseableSpan ignored = detached.childSpan(operation, metadata)) {
                delegate.run();
            }
        }
    }

    private static class AnonymousTracingAwareRunnable implements Runnable {
        private final Runnable delegate;
        private final Detached detached;

        AnonymousTracingAwareRunnable(Runnable delegate) {
            this.delegate = delegate;
            this.detached = DetachedSpan.detach();
        }

        @Override
        public void run() {
            try (CloseableSpan ignored = detached.attach()) {
                delegate.run();
            }
        }
    }

    public interface ThrowingCallable<T, E extends Throwable> {
        T call() throws E;
    }
}
