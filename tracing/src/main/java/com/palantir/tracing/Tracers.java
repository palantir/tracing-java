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

import com.google.common.util.concurrent.FutureCallback;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/** Utility methods for making {@link ExecutorService} and {@link Runnable} instances tracing-aware. */
public final class Tracers {
    /** The key under which trace ids are inserted into SLF4J {@link org.slf4j.MDC MDCs}. */
    public static final String TRACE_ID_KEY = "traceId";
    private static final String ROOT_SPAN_OPERATION = "root";
    private static final char[] HEX_DIGITS =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private Tracers() {}

    /** Returns a random ID suitable for span and trace IDs. */
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
     * Wraps the provided executor service such that any submitted {@link Callable} (or {@link Runnable}) is {@link
     * #wrap wrapped} in order to be trace-aware.
     */
    public static ExecutorService wrap(ExecutorService executorService) {
        return new WrappingExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(callable);
            }
        };
    }

    /**
     * Wraps the provided scheduled executor service to make submitted tasks traceable, see {@link
     * #wrap(ScheduledExecutorService)}. This method should not be used to wrap a ScheduledExecutorService that has
     * already been {@link #wrapWithNewTrace(ScheduledExecutorService) wrapped with new trace}. If this is done, a new
     * trace will be generated for each execution, effectively bypassing the intent of this method.
     */
    public static ScheduledExecutorService wrap(ScheduledExecutorService executorService) {
        return new WrappingScheduledExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrap(callable);
            }
        };
    }

    /**
     * Wraps the given {@link Callable} such that it uses the thread-local {@link Trace tracing state} at the time of
     * it's construction during its {@link Callable#call() execution}.
     */
    public static <V> Callable<V> wrap(Callable<V> delegate) {
        return new TracingAwareCallable<>(delegate);
    }

    /** Like {@link #wrap(Callable)}, but for Runnables. */
    public static Runnable wrap(Runnable delegate) {
        return new TracingAwareRunnable(delegate);
    }

    /** Like {@link #wrap(Callable)}, but for Guava's FutureCallback. */
    public static <V> FutureCallback<V> wrap(FutureCallback<V> delegate) {
        return new TracingAwareFutureCallback<>(delegate);
    }

    /**
     * Like {@link #wrapWithNewTrace(String, ExecutorService)}, but with a default initial span operation.
     */
    public static ExecutorService wrapWithNewTrace(ExecutorService executorService) {
        return wrapWithNewTrace(ROOT_SPAN_OPERATION, executorService);
    }

    /**
     * Wraps the provided executor service to make submitted tasks traceable with a fresh {@link Trace trace}
     * for each execution, see {@link #wrapWithNewTrace(String, ExecutorService)}. This method should not be used to
     * wrap a ScheduledExecutorService that has already been {@link #wrap(ExecutorService) wrapped}. If this is
     * done, a new trace will be generated for each execution, effectively bypassing the intent of the previous
     * wrapping.  The given {@link String operation} is used to create the initial span.
     */
    public static ExecutorService wrapWithNewTrace(String operation, ExecutorService executorService) {
        return new WrappingExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrapWithNewTrace(operation, callable);
            }
        };
    }

    /**
     * Like {@link #wrapWithNewTrace(String, ScheduledExecutorService)}, but with a default initial span operation.
     */
    public static ScheduledExecutorService wrapWithNewTrace(ScheduledExecutorService executorService) {
        return wrapWithNewTrace(ROOT_SPAN_OPERATION, executorService);
    }

    /**
     * Wraps the provided scheduled executor service to make submitted tasks traceable with a fresh {@link Trace trace}
     * for each execution, see {@link #wrapWithNewTrace(String, ScheduledExecutorService)}. This method should not be
     * used to wrap a ScheduledExecutorService that has already been {@link #wrap(ScheduledExecutorService) wrapped}.
     * If this is done, a new trace will be generated for each execution, effectively bypassing the intent of the
     * previous wrapping.  The given {@link String operation} is used to create the initial span.
     */
    public static ScheduledExecutorService wrapWithNewTrace(String operation,
            ScheduledExecutorService executorService) {
        return new WrappingScheduledExecutorService(executorService) {
            @Override
            protected <T> Callable<T> wrapTask(Callable<T> callable) {
                return wrapWithNewTrace(operation, callable);
            }
        };
    }

    /**
     * Like {@link #wrapWithNewTrace(String, Callable)}, but with a default initial span operation.
     */
    public static <V> Callable<V> wrapWithNewTrace(Callable<V> delegate) {
        return wrapWithNewTrace(ROOT_SPAN_OPERATION, delegate);
    }

    /**
     * Wraps the given {@link Callable} such that it creates a fresh {@link Trace tracing state} for its execution.
     * That is, the trace during its {@link Callable#call() execution} is entirely separate from the trace at
     * construction or any trace already set on the thread used to execute the callable. Each execution of the callable
     * will have a fresh trace. The given {@link String operation} is used to create the initial span.
     */
    public static <V> Callable<V> wrapWithNewTrace(String operation, Callable<V> delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTrace(Optional.empty(), Tracers.randomId());
                Tracer.startSpan(operation);
                return delegate.call();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Like {@link #wrapWithAlternateTraceId(String, Runnable)}, but with a default initial span operation.
     */
    public static Runnable wrapWithNewTrace(Runnable delegate) {
        return wrapWithNewTrace(ROOT_SPAN_OPERATION, delegate);
    }

    /**
     * Like {@link #wrapWithNewTrace(String, Callable)}, but for Runnables.
     */
    public static Runnable wrapWithNewTrace(String operation, Runnable delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTrace(Optional.empty(), Tracers.randomId());
                Tracer.startSpan(operation);
                delegate.run();
            } finally {
                Tracer.fastCompleteSpan();
                restoreTrace(originalTrace);
            }
        };
    }

    /**
     * Like {@link #wrapWithNewTrace(Callable)}, but for Guava's FutureCallback.
     */
    public static <V> FutureCallback<V> wrapWithNewTrace(FutureCallback<V> delegate) {
        return wrapWithNewTrace(ROOT_SPAN_OPERATION, delegate);
    }

    /**
     * Like {@link #wrapWithNewTrace(String, Callable)}, but for Guava's FutureCallback.
     */
    public static <V> FutureCallback<V> wrapWithNewTrace(String operation, FutureCallback<V> delegate) {
        return new FutureCallback<V>() {
            @Override
            public void onSuccess(@NullableDecl V result) {
                // clear the existing trace and keep it around for restoration when we're done
                Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

                try {
                    Tracer.initTrace(Optional.empty(), Tracers.randomId());
                    Tracer.startSpan(operation);
                    delegate.onSuccess(result);
                } finally {
                    Tracer.fastCompleteSpan();
                    restoreTrace(originalTrace);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                // clear the existing trace and keep it around for restoration when we're done
                Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

                try {
                    Tracer.initTrace(Optional.empty(), Tracers.randomId());
                    Tracer.startSpan(operation);
                    delegate.onFailure(throwable);
                } finally {
                    Tracer.fastCompleteSpan();
                    restoreTrace(originalTrace);
                }
            }
        };
    }

    /**
     * Like {@link #wrapWithAlternateTraceId(String, String, Runnable)}, but with a default initial span operation.
     */
    public static Runnable wrapWithAlternateTraceId(String traceId, Runnable delegate) {
        return wrapWithAlternateTraceId(traceId, ROOT_SPAN_OPERATION, delegate);
    }

    /**
     * Wraps the given {@link Runnable} such that it creates a fresh {@link Trace tracing state with the given traceId}
     * for its execution. That is, the trace during its {@link Runnable#run() execution} will use the traceId provided
     * instead of any trace already set on the thread used to execute the runnable. Each execution of the runnable
     * will use a new {@link Trace tracing state} with the same given traceId.  The given {@link String operation} is
     * used to create the initial span.
     */
    public static Runnable wrapWithAlternateTraceId(String traceId, String operation, Runnable
            delegate) {
        return () -> {
            // clear the existing trace and keep it around for restoration when we're done
            Optional<Trace> originalTrace = Tracer.getAndClearTraceIfPresent();

            try {
                Tracer.initTrace(Optional.empty(), traceId);
                Tracer.startSpan(operation);
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
     * <p>
     * The constructor is typically called by a tracing-aware executor service on the same thread on which a user
     * creates {@link Callable delegate}, and the {@link #call()} method is executed on an arbitrary (likely different)
     * thread with different {@link Trace tracing state}. In order to execute the task with the original (and
     * intuitively expected) tracing state, we remember the original state and set it for the duration of the
     * {@link #call() execution}.
     */
    private static class TracingAwareCallable<V> implements Callable<V> {
        private final Callable<V> delegate;
        private final DeferredTracer deferredTracer;

        TracingAwareCallable(Callable<V> delegate) {
            this.delegate = delegate;
            this.deferredTracer = new DeferredTracer();
        }

        @Override
        public V call() throws Exception {
            return this.deferredTracer.withTrace(delegate::call);
        }
    }

    /**
     * Wraps a given runnable such that its execution operates with the {@link Trace thread-local Trace} of the thread
     * that constructs the {@link TracingAwareRunnable} instance rather than the thread that executes the runnable.
     */
    private static class TracingAwareRunnable implements Runnable {
        private final Runnable delegate;
        private DeferredTracer deferredTracer;

        TracingAwareRunnable(Runnable delegate) {
            this.delegate = delegate;
            this.deferredTracer = new DeferredTracer();
        }

        @Override
        public void run() {
            deferredTracer.withTrace(() -> {
                delegate.run();
                return null;
            });
        }
    }

    /**
     * Wrap a given guava future callback such that its execution operated with the {@link Trace thread-local Trace} of
     * the thread the constructs the {@link TracingAwareFutureCallback} instance rather than the thread that executes
     * the callback.
     */
    private static class TracingAwareFutureCallback<V> implements FutureCallback<V> {
        private final FutureCallback<V> delegate;
        private DeferredTracer deferredTracer;

        TracingAwareFutureCallback(FutureCallback<V> delegate) {
            this.delegate = delegate;
            this.deferredTracer = new DeferredTracer();
        }

        @Override
        public void onSuccess(@NullableDecl V result) {
            deferredTracer.withTrace(() -> {
                delegate.onSuccess(result);
                return null;
            });
        }

        @Override
        public void onFailure(Throwable throwable) {
            deferredTracer.withTrace(() -> {
                delegate.onFailure(throwable);
                return null;
            });
        }
    }

    public interface ThrowingCallable<T, E extends Throwable> {
        T call() throws E;
    }
}
