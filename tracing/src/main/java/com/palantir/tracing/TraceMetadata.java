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
import org.immutables.value.Value;

/** Ids necessary to write headers onto network requests. */
@Value.Immutable
@ImmutablesStyle
public interface TraceMetadata {

    /** Corresponds to {@link com.palantir.tracing.api.TraceHttpHeaders#TRACE_ID}. */
    String getTraceId();

    /**
     * Returns the unique request identifier for this thread's trace.
     * Corresponds to {@link com.palantir.tracing.Tracers#REQUEST_ID_KEY}.
     */
    Optional<String> getRequestId();

    /** Corresponds to {@link com.palantir.tracing.api.TraceHttpHeaders#SPAN_ID} on outgoing requests. */
    String getSpanId();

    /** Corresponds to {@link com.palantir.tracing.api.TraceHttpHeaders#SPAN_ID} on incoming requests. */
    Optional<String> getParentSpanId();

    /**
     * Corresponds to {@link com.palantir.tracing.api.TraceHttpHeaders#ORIGINATING_SPAN_ID} which is no longer used.
     *
     * @deprecated No longer used
     */
    @Deprecated
    // Intentionally does not set @Value.Default because the value is always empty.
    default Optional<String> getOriginatingSpanId() {
        return Optional.empty();
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder extends ImmutableTraceMetadata.Builder {}
}
