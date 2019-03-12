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

import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanType;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Utility class for capturing the current trace at time of construction, and then
 * running callables at some later time, potentially on some other jvm (e.g. a worker node)
 * with that captured trace.
 *
 * <pre>
 * <code>
 * SerializableDeferredTracer deferredTracer = new SerializableDeferredTracer();
 *
 * //...
 *
 * // some time later, maybe on another jvm
 * deferredTracer.withTrace(() -> {
 *     doThings();
 *     System.out.println(Tracer.getTraceId()); // prints trace id at time of construction of deferred tracer
 *     return null;
 * });
 *
 * </code>
 * </pre>
 *
 * N.b. the captured trace is restored without the full stack of spans, and so it's not possible to complete spans
 * not started within the deferred context.
 */
public class SerializableDeferredTracer implements Serializable {

    @Nullable
    private final String traceId;
    private final boolean isObservable;
    private final String operation;
    @Nullable
    private final String parentSpanId;

    public SerializableDeferredTracer(String operation) {
        Optional<Trace> maybeTrace = Tracer.copyTrace();
        if (maybeTrace.isPresent()) {
            Trace trace = maybeTrace.get();

            this.traceId = trace.getTraceId();
            this.isObservable = trace.isObservable();
            this.parentSpanId = trace.top().map(OpenSpan::getSpanId).orElse(null);

        } else {
            this.traceId = null;
            this.isObservable = false;
            this.parentSpanId = null;
        }

        this.operation = operation;
    }

    /**
     * Runs the given callable with the current trace at
     * the time of construction of this {@link SerializableDeferredTracer}.
     */
    public <T, E extends Throwable> T withTrace(Tracers.ThrowingCallable<T, E> inner) throws E {
        if (traceId == null) {
            return inner.call();
        }

        Optional<Trace> originalTrace = Tracer.copyTrace();

        Tracer.setTrace(new Trace(isObservable, traceId));
        if (parentSpanId != null) {
            Tracer.startSpan(operation, parentSpanId, SpanType.LOCAL);
        } else {
            Tracer.startSpan(operation);
        }

        try {
            return inner.call();
        } finally {
            Tracer.fastCompleteSpan();
            if (originalTrace.isPresent()) {
                Tracer.setTrace(originalTrace.get());
            } else {
                Tracer.getAndClearTrace();
            }
        }
    }
}
