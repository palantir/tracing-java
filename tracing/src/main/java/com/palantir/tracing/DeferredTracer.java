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
import com.palantir.logsafe.Safe;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for capturing the current trace at time of construction, and then running callables at some later time
 * with that captured trace.
 *
 * <pre>
 * <code>{@code
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
 * }</code>
 * </pre>
 */
public final class DeferredTracer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_OPERATION = "DeferredTracer(unnamed operation)";

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
    public DeferredTracer(@Safe Optional<String> operation) {
        this(operation.orElse(DEFAULT_OPERATION));
    }

    public DeferredTracer(@Safe String operation, @Safe Map<String, String> metadata) {}

    public DeferredTracer(@Safe String operation) {
        this(operation, ImmutableMap.of());
    }

    /** Runs the given callable with the current trace at the time of construction of this {@link DeferredTracer}. */
    public <T, E extends Throwable> T withTrace(Tracers.ThrowingCallable<T, E> inner) throws E {}
}
