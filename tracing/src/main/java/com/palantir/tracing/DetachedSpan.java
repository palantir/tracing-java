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
import com.palantir.logsafe.Safe;
import com.palantir.tracing.api.SpanType;
import java.util.Map;
import java.util.Optional;
import javax.annotation.CheckReturnValue;

/**
 * Span which is not bound to thread state, and can be completed on any other thread.
 *
 * This class must not be implemented externally.
 */
public interface DetachedSpan extends Detached {

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
    static DetachedSpan start(@Safe String operation) {
        return start(operation, SpanType.LOCAL);
    }

    /**
     * Marks the beginning of a span, which you can {@link #complete} on any other thread.
     *
     * @see DetachedSpan#start(String)
     */
    @CheckReturnValue
    static DetachedSpan start(@Safe String operation, SpanType type) {
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
            @Safe String operation,
            SpanType type) {
        return start(observability, traceId, Optional.empty(), parentSpanId, operation, type);
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
            Optional<String> forUserAgent,
            Optional<String> parentSpanId,
            @Safe String operation,
            SpanType type) {
        return Tracer.detachInternal(observability, traceId, forUserAgent, parentSpanId, operation, type);
    }

    /**
     * Creates a {@link Detached} instance based on the current tracing state without adding a new span.
     * Note that if there is no tracing state present a no-op instance is returned. This is the inverse of
     * {@link #attach()}.
     *
     * @see DetachedSpan#attach()
     */
    @CheckReturnValue
    static Detached detach() {
        return Tracer.detachInternal();
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    default CloseableSpan completeAndStartChild(@Safe String operationName, SpanType type) {
        CloseableSpan child = childSpan(operationName, type);
        complete();
        return child;
    }

    @MustBeClosed
    default CloseableSpan completeAndStartChild(String operationName) {
        return completeAndStartChild(operationName, SpanType.LOCAL);
    }

    /**
     * Completes this span. After complete is invoked, other methods are not expected to produce spans, but they must
     * not throw either in order to avoid confusing failures.
     */
    void complete();

    /**
     * Completes this span. After complete is invoked, other methods are not expected to produce spans, but they must
     * not throw either in order to avoid confusing failures.
     */
    default void complete(@Safe Map<String, String> metadata) {
        complete(MapTagTranslator.INSTANCE, metadata);
    }

    /**
     * Completes this span. After complete is invoked, other methods are not expected to produce spans, but they must
     * not throw either in order to avoid confusing failures.
     */
    <T> void complete(TagTranslator<? super T> tagTranslator, T data);
}
