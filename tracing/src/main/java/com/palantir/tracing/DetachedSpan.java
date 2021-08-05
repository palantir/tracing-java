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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.Optional;
import javax.annotation.CheckReturnValue;

/**
 * Span which is not bound to thread state, and can be completed on any other thread.
 */
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
        Span span = Tracer.getSpanBuilder()
                .spanBuilder(operation)
                .setSpanKind(Translation.toOpenTelemetry(type))
                .setParent(Context.current())
                .startSpan();

        return DetachedSpanImpl.createAndMakeCurrent(span);
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

        Span parentSpan = Tracer.createMadeUpSpan(traceId, parentSpanId, observability);
        Context context2 = Context.current().with(parentSpan);

        Span span = Tracer.getSpanBuilder()
                .spanBuilder(operation)
                .setSpanKind(Translation.toOpenTelemetry(type))
                .setParent(context2)
                .startSpan();

        return DetachedSpanImpl.createAndMakeCurrent(span);
    }

    final class DetachedSpanImpl implements DetachedSpan {
        // Gotta keep these two around so we can close them at the end
        private final Span span;
        private final Scope scope;
        // Allows creating new child spans parented correctly
        private final Context context;

        private DetachedSpanImpl(Span span, Scope scope, Context context) {
            this.span = span;
            this.scope = scope;
            this.context = context;
        }

        static DetachedSpan createAndMakeCurrent(Span span) {
            Context context2 = Context.current().with(span);
            Scope scope = context2.makeCurrent();
            return new DetachedSpanImpl(span, scope, context2);
        }

        @Override
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> translator, T data, SpanType type) {

            Span childSpan = Tracer.getSpanBuilder()
                    .spanBuilder(operationName)
                    .setSpanKind(Translation.toOpenTelemetry(type))
                    .setParent(context)
                    .startSpan();
            Tracer.setSpanAttributes(childSpan, translator, data);

            Scope scope2 = childSpan.makeCurrent();
            return new CloseableSpan() {
                @Override
                public void close() {
                    childSpan.end();
                    scope2.close();
                }
            };
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            Span childDetachedSpan = Tracer.getSpanBuilder()
                    .spanBuilder(operation)
                    .setSpanKind(Translation.toOpenTelemetry(type))
                    .setParent(context)
                    .startSpan();

            return DetachedSpanImpl.createAndMakeCurrent(childDetachedSpan);
        }

        @Override
        public CloseableSpan completeAndStartChild(String operationName, SpanType type) {
            complete();
            return childSpan(operationName, type);
        }

        @Override
        public void complete() {
            span.end();
            scope.close();
        }

        @Override
        public <T> void complete(TagTranslator<? super T> tagTranslator, T data) {
            Tracer.setSpanAttributes(span, tagTranslator, data);
            span.end();
            scope.close();
        }
    }

    /**
     * Equivalent to {@link Tracer#startSpan(String, SpanType)}, but using this {@link DetachedSpan} as the parent
     * instead of thread state.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, SpanType type) {
        return childSpan(operationName, NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE, type);
    }

    /**
     * Equivalent to {@link #childSpan(String, Map, SpanType)} using a {@link SpanType#LOCAL span}.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, @Safe Map<String, String> metadata) {
        return childSpan(operationName, metadata, SpanType.LOCAL);
    }

    @MustBeClosed
    default <T> CloseableSpan childSpan(@Safe String operationName, TagTranslator<? super T> translator, T data) {
        return childSpan(operationName, translator, data, SpanType.LOCAL);
    }

    /**
     * Equivalent to {@link #childSpan(String, SpanType)}, but using {@link Map metadata} tags.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, @Safe Map<String, String> metadata, SpanType type) {
        return childSpan(operationName, MapTagTranslator.INSTANCE, metadata, type);
    }

    @MustBeClosed
    <T> CloseableSpan childSpan(@Safe String operationName, TagTranslator<? super T> translator, T data, SpanType type);

    /**
     * Equivalent to {@link Tracer#startSpan(String)}, but using this {@link DetachedSpan} as the parent instead of
     * thread state.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName) {
        return childSpan(operationName, SpanType.LOCAL);
    }

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    CloseableSpan completeAndStartChild(@Safe String operationName, SpanType type);

    @MustBeClosed
    default CloseableSpan completeAndStartChild(@Safe String operationName) {
        return completeAndStartChild(operationName, SpanType.LOCAL);
    }

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent.
     */
    @CheckReturnValue
    DetachedSpan childDetachedSpan(String operation, SpanType type);

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent. Equivalent to
     * {@link #childDetachedSpan(String, SpanType)} using {@link SpanType#LOCAL}.
     */
    @CheckReturnValue
    default DetachedSpan childDetachedSpan(@Safe String operation) {
        return childDetachedSpan(operation, SpanType.LOCAL);
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
