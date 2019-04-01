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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.tracing.api.SpanType;

/**
 * Span operation which is not bound to thread state, and can measure operations which
 * themselves aren't bound to individual threads.
 */
public interface DetachedSpan {

    /**
     * Equivalent to {@link Tracer#startSpan(String, SpanType)}, but using this {@link DetachedSpan}
     * as the parent instead of thread state.
     */
    @MustBeClosed
    SpanToken attach(String operationName, SpanType type);

    /**
     * Equivalent to {@link Tracer#startSpan(String)}, but using this {@link DetachedSpan} as the parent instead
     * of thread state.
     */
    @MustBeClosed
    default SpanToken attach(String operationName) {
        return attach(operationName, SpanType.LOCAL);
    }

    /** Starts a child {@link DetachedSpan} using this instance as the parent. */
    DetachedSpan detach(String operation, SpanType type);

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent.
     * Equivalent to {@link #attach(String, SpanType)} using {@link SpanType#LOCAL}.
     */
    default DetachedSpan detach(String operation) {
        return detach(operation, SpanType.LOCAL);
    }

    /**
     * Completes this span. After complete is invoked, other methods are not expected to produce spans, but
     * they must not throw either in order to avoid confusing failures.
     */
    void complete();
}
