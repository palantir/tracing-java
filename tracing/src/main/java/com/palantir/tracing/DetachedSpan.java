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
import java.util.Optional;
import javax.annotation.CheckReturnValue;

/** Span which is not bound to thread state, and can be completed on any other thread. */
public interface DetachedSpan {

    /**
     * Marks the beginning of a span, which you can {@link #complete} on any other thread. Further work on this
     * originating thread will not automatically parented to this span (because it does not modify any thread local
     * tracing state). If you don't need this cross-thread functionality, use {@link CloseableTracer}.
     *
     * <p>On the destination thread, you can call {@link #completeAndStartChild} to mark the end of this
     * {@link DetachedSpan} and continue tracing regular thread-local work. Alternatively, if you want to keep this
     * DetachedSpan open, you can instrument 'sub tasks' using {@link #childSpan} or {@link #childDetachedSpan}, but
     * must remember to call {@link #complete} eventually.
     */
    @CheckReturnValue
    static DetachedSpan start(String operation) {
        return start(operation, SpanType.LOCAL);
    }

    /**
     * Marks the beginning of a span, which you can {@link #complete} on any other thread.
     *
     * @see DetachedSpan#start(String)
     */
    @CheckReturnValue
    static DetachedSpan start(String operation, SpanType type) {
        return Tracer.detachInternal(operation, type);
    }

    /**
     * Marks the beginning of a span, which you can {@link #complete} on any other thread.
     *
     * @see DetachedSpan#start(String)
     */
    @CheckReturnValue
    static DetachedSpan start(
            Observability observability,
            String traceId,
            Optional<String> parentSpanId,
            String operation,
            SpanType type) {
        return Tracer.detachInternal(observability, traceId, parentSpanId, operation, type);
    }

    /**
     * Equivalent to {@link Tracer#startSpan(String, SpanType)}, but using this {@link DetachedSpan} as the parent
     * instead of thread state.
     */
    @MustBeClosed
    CloseableSpan childSpan(String operationName, SpanType type);

    /**
     * Equivalent to {@link Tracer#startSpan(String)}, but using this {@link DetachedSpan} as the parent instead of
     * thread state.
     */
    @MustBeClosed
    default CloseableSpan childSpan(String operationName) {
        return childSpan(operationName, SpanType.LOCAL);
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    default CloseableSpan completeAndStartChild(String operationName, SpanType type) {
        CloseableSpan child = childSpan(operationName, type);
        complete();
        return child;
    }

    @MustBeClosed
    default CloseableSpan completeAndStartChild(String operationName) {
        return completeAndStartChild(operationName, SpanType.LOCAL);
    }

    /** Starts a child {@link DetachedSpan} using this instance as the parent. */
    @CheckReturnValue
    DetachedSpan childDetachedSpan(String operation, SpanType type);

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent. Equivalent to
     * {@link #childDetachedSpan(String, SpanType)} using {@link SpanType#LOCAL}.
     */
    @CheckReturnValue
    default DetachedSpan childDetachedSpan(String operation) {
        return childDetachedSpan(operation, SpanType.LOCAL);
    }

    /**
     * Completes this span. After complete is invoked, other methods are not expected to produce spans, but they must
     * not throw either in order to avoid confusing failures.
     */
    void complete();
}
