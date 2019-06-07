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

import com.palantir.tracing.api.SpanObserver;

/**
 * Represents the desired observability of a new trace.
 */
public enum Observability {
    /**
     * Force the trace to be sampled, i.e.s spans will be sent to each {@link SpanObserver}, and any outgoing
     * network requests will have the X-B3-SAMPLED header set to "1", so that all downstream services do the same.
     */
    SAMPLE,

    /** Do not send the trace's spans to any {@link SpanObserver} (on this service). */
    DO_NOT_SAMPLE,

    /** Allow the {@link TraceSampler} to decide whether this trace will be observed or not. */
    UNDECIDED
}
