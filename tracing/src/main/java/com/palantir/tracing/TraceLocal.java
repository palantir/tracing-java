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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class TraceLocal<T> {

    private static final SafeLogger log = SafeLoggerFactory.get(TraceLocal.class);

    private final Supplier<T> initialValue;

    /**
     * Creates a trace local variable.
     *
     * When not already set, and this variable is accessed in a trace with the {@link #get()} method, the supplier
     * will be invoked to supply a value (which will then be stored for future accesses).
     *
     * The supplier is thus normally invoked once per trace, but may be invoked again in case of subsequent
     * invocations of {@link #remove()} followed by get.
     */
    public TraceLocal(Supplier<T> initialValue) {
        this.initialValue = initialValue;
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
        return Tracer.getTraceLocalValue(this, initialValue);
    }

    /**
     * Sets the value of this trace local for the current trace.
     */
    public void set(@Nonnull T value) {
        if (value == null) {
            throw new SafeIllegalArgumentException("value must not be null");
        }

        Tracer.setTraceLocalValue(this, value);
    }

    public void remove() {
        Tracer.setTraceLocalValue(this, null);
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
