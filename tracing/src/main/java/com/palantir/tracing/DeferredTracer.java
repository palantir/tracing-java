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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanType;
import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Utility class for capturing the current trace at time of construction, and then
 * running callables at some later time with that captured trace.
 * <pre>
 * <code>
 * DeferredTracer deferredTracer = new DeferredTracer();
 *
 * //...
 *
 * // some time later
 * deferredTracer.withTrace(() -> {
 *     doThings();
 *     System.out.println(Tracer.getTraceId()); // prints trace id at time of construction of deferred tracer
 *     return null;
 * });
 *
 * N.b. the captured trace is restored without the full stack of spans, and so it's not possible to complete spans
 * not started within the deferred context.
 *
 * </code>
 * </pre>
 */
public final class DeferredTracer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_OPERATION = "DeferredTracer(unnamed operation)";

    @Nullable
    private final String traceId;
    private final boolean isObservable;
    @Nullable
    private final String operation;
    @Nullable
    private final String parentSpanId;

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #DeferredTracer(String)}
     */
    @Deprecated
    public DeferredTracer() {
        this(Optional.empty());
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #DeferredTracer(String)}
     */
    @Deprecated
    public DeferredTracer(Optional<String> operation) {
        this(operation.orElse(DEFAULT_OPERATION));
    }

    public DeferredTracer(String operation) {
        Optional<Trace> maybeTrace = Tracer.copyTrace();
        if (maybeTrace.isPresent()) {
            Trace trace = maybeTrace.get();
            this.traceId = trace.getTraceId();
            this.isObservable = trace.isObservable();
            this.parentSpanId = trace.top().map(OpenSpan::getSpanId).orElse(null);
            this.operation = operation;
        } else {
            this.traceId = null;
            this.isObservable = false;
            this.parentSpanId = null;
            this.operation = null;
        }
    }

    /**
     * Runs the given callable with the current trace at
     * the time of construction of this {@link DeferredTracer}.
     */
    public <T, E extends Throwable> T withTrace(Tracers.ThrowingCallable<T, E> inner) throws E {
        try (CloseableTrace ignored = withTrace()) {
            return inner.call();
        }
    }

    @MustBeClosed
    @SuppressWarnings("NullAway") // either both operation & parentSpanId are nullable or neither are
    CloseableTrace withTrace() {
        if (traceId == null) {
            return NopCloseableTrace.INSTANCE;
        }

        Optional<Trace> originalTrace = Tracer.copyTrace();

        Tracer.setTrace(Trace.of(isObservable, traceId));
        if (parentSpanId != null) {
            Tracer.fastStartSpan(operation, parentSpanId, SpanType.LOCAL);
        } else {
            Tracer.fastStartSpan(operation);
        }

        return originalTrace.map(CLOSEABLE_TRACE_FUNCTION)
                .orElse(DefaultCloseableTrace.INSTANCE);
    }

    private enum NopCloseableTrace implements CloseableTrace {
        INSTANCE;

        @Override
        public void close() {}
    }

    private enum DefaultCloseableTrace implements CloseableTrace {
        INSTANCE;

        @Override
        public void close() {
            Tracer.fastCompleteSpan();
            if (Tracer.hasTraceId()) {
                Tracer.getAndClearTrace();
            }
        }
    }

    private static final Function<Trace, CloseableTrace> CLOSEABLE_TRACE_FUNCTION = originalTrace -> () -> {
        DefaultCloseableTrace.INSTANCE.close();
        Tracer.setTrace(originalTrace);
    };

    /** Package private mechanism to simplify internal {@link DeferredTracer} use. */
    interface CloseableTrace extends Closeable {
        @Override
        void close();
    }
}
