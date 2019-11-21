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

package com.palantir.tracing.api;

/** Zipkin-compatible HTTP header names. */
public interface TraceHttpHeaders {
    String TRACE_ID = "X-B3-TraceId";
    String PARENT_SPAN_ID = "X-B3-ParentSpanId";
    String SPAN_ID = "X-B3-SpanId";
    String IS_SAMPLED = "X-B3-Sampled"; // Boolean (either “1” or “0”, can be absent)

    /**
     * Conceptually, a trace is a stack of spans. In implementation, this is actually many stacks, in many servers,
     * where a server's stack will typically contain a single parent span from a different server at the bottom, and
     * many spans of its own above it.
     *
     * <p>By communicating this deepest span id with future network calls as an 'originating' span id, this enables
     * network-level tracing to be enabled always in a low-fidelity form, with request logs containing enough
     * information to reconstruct a request-level trace, even when the trace is not sampled. For server-internal
     * tracing, the typical trace logs (with sampling) are still required.
     */
    String ORIGINATING_SPAN_ID = "X-OrigSpanId";
}
