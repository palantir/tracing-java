/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides trace local variables. Each trace gets its own independent copy of the variable.
 *
 * A trace local is either set (has a value) or unset (has no value).
 *
 * Outside of a trace (i.e. when {@link Tracer#hasTraceId()} is false) then the trace local is always unset.
 */
public final class TraceLocal<T> {

    @Nullable
    private final Function<? super TraceLocal<?>, T> initialValue;

    private TraceLocal(@Nullable Supplier<T> initialValue) {
        if (initialValue == null) {
            this.initialValue = null;
        } else {
            // eagerly transform supplier to avoid allocation per invocation
            // (computeIfAbsent takes a Function)
            this.initialValue = _ignored ->
                    Preconditions.checkNotNull(initialValue.get(), "TraceLocal initial value must not be null");
        }
    }

    public static <T> TraceLocal<T> of() {
        return new TraceLocal<>(null);
    }

    /**
     * Creates a trace local variable, with a way of supplying an initial value.
     *
     * When not already set, and this variable is accessed in a trace with the {@link #get()} method, the supplier
     * will be invoked to supply a value (which will then be stored for future accesses).
     *
     * The supplier is thus normally invoked once per trace, but may be invoked again in case of subsequent
     * invocations of {@link #remove()} followed by get.
     */
    public static <T> TraceLocal<T> withInitialValue(@Nonnull Supplier<T> initialValue) {
        return new TraceLocal<>(Preconditions.checkNotNull(initialValue, "initial value supplier must not be null"));
    }

    /**
     * Retrieve the current value of the trace local, with respect to the current trace.
     *
     * If there is no current trace, i.e. {@link Tracer#hasTraceId()} is null, then this will return null.
     *
     * If the value of this trace local has not been set for the current trace, then the supplier passed in the
     * constructor will be called to supply a value.
     */
    @Nullable
    public T get() {
        TraceState traceState = Tracer.getTraceState();

        if (traceState == null) {
            return null;
        }

        if (initialValue == null) {
            // not potentially setting a value, so just grab any current set value

            Map<TraceLocal<?>, Object> traceLocals = traceState.getTraceLocals();

            if (traceLocals == null) {
                return null;
            }

            return (T) traceLocals.get(this);
        } else {
            Map<TraceLocal<?>, Object> traceLocals = traceState.getOrCreateTraceLocals();
            return (T) traceLocals.computeIfAbsent(this, initialValue);
        }
    }

    /**
     * Sets the value of this trace local for the current trace.
     *
     * Returns the previous value of this trace local if set, or null if the value was previously unset.
     */
    @Nullable
    public T set(@Nonnull T value) {
        if (value == null) {
            throw new SafeIllegalArgumentException("value must not be null");
        }

        TraceState traceState = Tracer.getTraceState();

        if (traceState == null) {
            return null;
        }

        return (T) traceState.getOrCreateTraceLocals().put(this, value);
    }

    /**
     * Unsets the value of this trace local.
     *
     * Returns the previous value of this trace local if set, or null if the value was previously unset.
     */
    @Nullable
    public T remove() {
        TraceState traceState = Tracer.getTraceState();

        if (traceState == null) {
            return null;
        }

        Map<TraceLocal<?>, Object> traceLocals = traceState.getTraceLocals();
        if (traceLocals == null) {
            // no trace locals ever set, short circuit (avoid creating the trace local map)
            return null;
        }

        return (T) traceLocals.remove(this);
    }

    @Override
    public boolean equals(Object obj) {
        // identity semantics
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        // identity semantics
        return super.hashCode();
    }
}
