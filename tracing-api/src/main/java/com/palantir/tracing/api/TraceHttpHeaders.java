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

/** HTTP header names used by tracing-java. */
@SuppressWarnings("for-rollout:InterfaceWithOnlyStatics")
public interface TraceHttpHeaders {
    String TRACE_ID = "X-B3-TraceId";
    /**
     * Field is no longer used by the tracing library.
     *
     * @deprecated No longer used
     */
    @Deprecated
    String PARENT_SPAN_ID = "X-B3-ParentSpanId";

    String SPAN_ID = "X-B3-SpanId";
    String IS_SAMPLED = "X-B3-Sampled"; // Boolean (either “1” or “0”, can be absent)

    String FOR_USER_AGENT = "For-User-Agent";

    /**
     * Field is no longer used by the tracing library.
     *
     * @deprecated No longer used
     */
    @Deprecated
    String ORIGINATING_SPAN_ID = "X-OrigSpanId";

    /**
     * The {@code Parent-Request-Id} header represents the requestId of the caller, if present.
     * This provides a direct causal connection between requests up a call stack. This can be an advantage
     * over traceId which provides a broad set of data without information to understand the call
     * pathing unless spans are sampled.
     */
    String PARENT_REQUEST_ID = "Parent-Request-Id";
}
