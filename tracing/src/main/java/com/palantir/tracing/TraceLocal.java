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

    public TraceLocal(Supplier<T> initialValue) {
        this.initialValue = initialValue;
    }

    @Nullable
    public T get() {
        return Tracer.getTraceLocalValue(this, initialValue);
    }

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
