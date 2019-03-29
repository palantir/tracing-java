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

import java.io.Serializable;
import java.util.Optional;

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
 * </code>
 * </pre>
 */
public final class DeferredTracer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_OPERATION = "deferred";

    private final Trace deferredTrace;
    private final String operation;

    public DeferredTracer() {
        this(Optional.empty());
    }

    public DeferredTracer(String operation) {
        this(Optional.of(operation));
    }

    /**
     * Create a new deferred tracer, optionally specifying an operation.
     * If no operation is specified, will attempt to use the parent span's operation name.
     */
    public DeferredTracer(Optional<String> operation) {
        this.operation = operation.orElse(DEFAULT_OPERATION);
        // Necessary to preserve the trace between "enqueue" and "run"
        Tracer.startSpan(this.operation + "-enqueue");
        deferredTrace = Tracer.copyTrace().get();
        Tracer.fastDiscardSpan(); // span will completed in the deferred execution
    }

    /**
     * Runs the given callable with the current trace at
     * the time of construction of this {@link DeferredTracer}.
     */
    public <T, E extends Throwable> T withTrace(Tracers.ThrowingCallable<T, E> inner) throws E {
        Trace originalTrace = Tracer.getAndClearTrace();
        Tracer.setTrace(deferredTrace);
        // Finish the enqueue span
        Tracer.fastCompleteSpan();
        Tracer.startSpan(operation + "-run");
        try {
            return inner.call();
        } finally {
            // Finish the run span
            Tracer.fastCompleteSpan();
            Tracer.setTrace(originalTrace);
        }
    }
}
