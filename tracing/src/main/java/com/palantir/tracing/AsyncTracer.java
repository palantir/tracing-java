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

import java.util.Optional;

/**
 * Utility for tracking an operation that will be run asynchronously. It tracks the time it spent before
 * {@link #withTrace} is called.
 * <pre>
 * <code>
 * AsyncTracer asyncTracer = new AsyncTracer();
 *
 * //...
 *
 * // some time later
 * asyncTracer.withTrace(() -> {
 *     doThings();
 *     System.out.println(Tracer.getTraceId()); // prints trace id at time of construction of async tracer
 *     return null;
 * });
 *
 * </code>
 * </pre>
 *
 * @deprecated prefer {@link DetachedSpan#start}, which gives you much more granular control.
 */
@Deprecated
public final class AsyncTracer {

    private static final String DEFAULT_OPERATION = "async";

    private final Trace deferredTrace;
    private final String operation;

    public AsyncTracer() {
        this(Optional.empty());
    }

    public AsyncTracer(String operation) {
        this(Optional.of(operation));
    }

    /**
     * Create a new deferred tracer, optionally specifying an operation.
     * If no operation is specified, will attempt to use the parent span's operation name.
     */
    public AsyncTracer(Optional<String> operation) {
        this.operation = operation.orElse(DEFAULT_OPERATION);
        Tracer.fastStartSpan(this.operation + "-enqueue");
        deferredTrace = Tracer.copyTrace().get();
        Tracer.fastDiscardSpan(); // span will completed in the deferred execution
    }

    /**
     * Runs the given callable with the current trace at
     * the time of construction of this {@link AsyncTracer}.
     */
    public <T, E extends Throwable> T withTrace(Tracers.ThrowingCallable<T, E> inner) throws E {
        Trace originalTrace = Tracer.getAndClearTrace();
        Tracer.setTrace(deferredTrace);
        // Finish the enqueue span
        Tracer.fastCompleteSpan();
        Tracer.fastStartSpan(operation + "-run");
        try {
            return inner.call();
        } finally {
            // Finish the run span
            Tracer.fastCompleteSpan();
            Tracer.setTrace(originalTrace);
        }
    }
}
