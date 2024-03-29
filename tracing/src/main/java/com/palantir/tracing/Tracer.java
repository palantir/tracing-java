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

package com.palantir.tracing;

import static com.palantir.logsafe.Preconditions.checkArgument;
import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.slf4j.MDC;

/**
 * The singleton entry point for handling Zipkin-style traces and spans. Provides functionality for starting and
 * completing spans, and for subscribing observers to span completion events.
 *
 * <p>This class is thread-safe.
 */
public final class Tracer {

    private static final SafeLogger log = SafeLoggerFactory.get(Tracer.class);

    private Tracer() {}

    // Thread-safe since thread-local
    private static final ThreadLocal<Trace> currentTrace = new ThreadLocal<>();

    // Only access in a class-synchronized fashion
    private static final Map<String, SpanObserver> observers = new HashMap<>();
    // we want iterating through tracers to be very fast, and it's faster to pre-define observer execution
    // when our observers are modified.
    private static volatile Consumer<Span> compositeObserver = _span -> {};

    // Thread-safe since stateless
    private static volatile TraceSampler sampler = RandomSampler.create(0.0005f);

    /**
     * Creates a new trace, but does not set it as the current trace.
     */
    private static Trace createTrace(Observability observability, String traceId, Optional<String> requestId) {
        return createTrace(observability, traceId, requestId, Optional.empty());
    }

    /**
     * Creates a new trace, but does not set it as the current trace.
     */
    private static Trace createTrace(
            Observability observability, String traceId, Optional<String> requestId, Optional<String> forUserAgent) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        boolean observable = shouldObserve(observability);
        return Trace.of(observable, TraceState.of(traceId, requestId, forUserAgent));
    }

    private static boolean shouldObserve(Observability observability) {
        // Simplified implementation of 'switch(observability) {' for fast inlining (30 bytes)
        return observability == Observability.SAMPLE || (observability == Observability.UNDECIDED && sampler.sample());
    }

    /**
     * Deprecated. This exists to avoid ABI breaks due to a cross-jar package private call that existed in <=4.1.0.
     *
     * @deprecated Use {@link Tracer#maybeGetTraceMetadata()} instead.
     */
    @SuppressWarnings("InlineMeSuggester")
    @Deprecated
    static TraceMetadata getTraceMetadata() {
        return maybeGetTraceMetadata().orElseThrow(() -> new SafeRuntimeException("Trace with no spans in progress"));
    }

    /**
     * In the unsampled case, the Trace.Unsampled class doesn't actually store a spanId/parentSpanId stack, so we just
     * make one up (just in time). This matches the behaviour of Tracer#startSpan.
     *
     * <p>n.b. this is a bit funky because calling maybeGetTraceMetadata multiple times will return different spanIds
     */
    public static Optional<TraceMetadata> maybeGetTraceMetadata() {
        Trace trace = currentTrace.get();
        if (trace == null) {
            return Optional.empty();
        }

        TraceMetadata.Builder builder = TraceMetadata.builder().traceId(trace.getTraceId());
        String requestId = trace.maybeGetRequestId();
        if (requestId != null) {
            builder.requestId(requestId);
        }

        if (trace.isObservable()) {
            return trace.top().map(openSpan -> builder.spanId(openSpan.getSpanId())
                    .parentSpanId(openSpan.getParentSpanId())
                    .build());
        } else {
            return Optional.of(builder.spanId(Tracers.randomId())
                    .parentSpanId(Optional.empty())
                    .build());
        }
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, String, SpanType)}
     */
    @Deprecated
    public static void initTrace(Optional<Boolean> isObservable, String traceId) {
        Observability observability = isObservable
                .map(observable -> observable ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE)
                .orElse(Observability.UNDECIDED);

        setTrace(createTrace(observability, traceId, Optional.empty()));
    }

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, String, SpanType)}
     */
    @Deprecated
    public static void initTrace(Observability observability, String traceId) {
        setTrace(createTrace(observability, traceId, Optional.empty()));
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, Optional, String, String, SpanType)}
     */
    @Deprecated
    public static void initTraceWithSpan(
            Observability observability, String traceId, @Safe String operation, String parentSpanId, SpanType type) {
        setTrace(createTrace(
                observability,
                traceId,
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty()));
        fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability, String traceId, @Safe String operation, SpanType type) {
        setTrace(createTrace(
                observability,
                traceId,
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty()));
        fastStartSpan(operation, type);
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability,
            String traceId,
            Optional<String> forUserAgent,
            @Safe String operation,
            String parentSpanId,
            SpanType type) {
        setTrace(createTrace(
                observability,
                traceId,
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty(),
                forUserAgent));
        fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability,
            String traceId,
            Optional<String> forUserAgent,
            @Safe String operation,
            SpanType type) {
        setTrace(createTrace(
                observability,
                traceId,
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty(),
                forUserAgent));
        fastStartSpan(operation, type);
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty. If the return value is not used, prefer {@link Tracer#fastStartSpan(String,
     * String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation, String parentSpanId, SpanType type) {
        return getOrCreateCurrentTrace().startSpan(operation, parentSpanId, type);
    }

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}. If the
     * return value is not used, prefer {@link Tracer#fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation, SpanType type) {
        return getOrCreateCurrentTrace().startSpan(operation, type);
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, String parentSpanId, SpanType type) {
        getOrCreateCurrentTrace().fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, SpanType type) {
        getOrCreateCurrentTrace().fastStartSpan(operation, type);
    }

    /**
     * Like {@link #startSpan(String)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation) {
        fastStartSpan(operation, SpanType.LOCAL);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(@Safe String operation, SpanType type) {
        Trace maybeCurrentTrace = currentTrace.get();
        TraceState traceState = getTraceState(maybeCurrentTrace, type);
        boolean sampled = maybeCurrentTrace != null ? maybeCurrentTrace.isObservable() : sampler.sample();
        Optional<String> parentSpan = getParentSpanId(maybeCurrentTrace);
        return sampled
                ? new SampledDetachedSpan(operation, type, traceState, parentSpan)
                : new UnsampledDetachedSpan(traceState, Optional.empty());
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(
            Observability observability,
            String traceId,
            Optional<String> forUserAgent,
            Optional<String> parentSpanId,
            @Safe String operation,
            SpanType type) {
        Optional<String> requestId =
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty();
        return detachInternal(observability, traceId, requestId, forUserAgent, parentSpanId, operation, type);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(
            Observability observability,
            String traceId,
            Optional<String> requestId,
            Optional<String> forUserAgent,
            Optional<String> parentSpanId,
            @Safe String operation,
            SpanType type) {
        // The current trace has no impact on this function, a new trace is spawned and existing thread state
        // is not modified.
        TraceState traceState = TraceState.of(traceId, requestId, forUserAgent);
        return shouldObserve(observability)
                ? new SampledDetachedSpan(operation, type, traceState, parentSpanId)
                : new UnsampledDetachedSpan(traceState, parentSpanId);
    }

    /**
     * Like {@link #detachInternal(String, SpanType)} but does not create a new span and may return a
     * no-op implementation if no tracing state is currently set.
     */
    static Detached detachInternal() {
        Trace trace = currentTrace.get();
        if (trace == null) {
            return NopDetached.INSTANCE;
        }

        if (trace.isObservable()) {
            OpenSpan maybeOpenSpan = trace.top().orElse(null);
            if (maybeOpenSpan == null) {
                return NopDetached.INSTANCE;
            }
            return new SampledDetached(trace.getTraceState(), maybeOpenSpan);
        } else {
            return new UnsampledDetachedSpan(trace.getTraceState(), Optional.empty());
        }
    }

    private static Optional<String> getParentSpanId(@Nullable Trace trace) {
        if (trace != null) {
            Optional<OpenSpan> maybeOpenSpan = trace.top();
            if (maybeOpenSpan.isPresent()) {
                return Optional.of(maybeOpenSpan.get().getSpanId());
            }
        }
        return Optional.empty();
    }

    @Nullable
    static TraceState getTraceState() {
        Trace maybeCurrentTrace = currentTrace.get();

        if (maybeCurrentTrace == null) {
            return null;
        }

        return maybeCurrentTrace.getTraceState();
    }

    private static TraceState getTraceState(@Nullable Trace maybeCurrentTrace, SpanType newSpanType) {
        if (maybeCurrentTrace != null) {
            return maybeCurrentTrace.getTraceState();
        }
        return TraceState.of(Tracers.randomId(), getRequestIdForSpan(newSpanType), Optional.empty());
    }

    private static Optional<String> getRequestIdForSpan(SpanType newSpanType) {
        if (newSpanType == SpanType.SERVER_INCOMING) {
            return Optional.of(Tracers.randomId());
        }
        return Optional.empty();
    }

    @Nullable
    static String getRequestId(DetachedSpan detachedSpan) {
        if (detachedSpan instanceof SampledDetachedSpan) {
            return ((SampledDetachedSpan) detachedSpan).traceState.requestId();
        }
        if (detachedSpan instanceof UnsampledDetachedSpan) {
            return ((UnsampledDetachedSpan) detachedSpan).traceState.requestId();
        }
        throw new SafeIllegalStateException("Unknown span type", SafeArg.of("detachedSpan", detachedSpan));
    }

    static boolean isSampled(DetachedSpan detachedSpan) {
        return detachedSpan instanceof SampledDetachedSpan;
    }

    private static final class TraceRestoringCloseableSpanWithMetadata<T> implements CloseableSpan {

        @Nullable
        private final Trace original;

        private final TagTranslator<? super T> translator;
        private final T data;

        static <T> CloseableSpan of(@Nullable Trace original, TagTranslator<? super T> translator, T data) {
            if (original != null || !translator.isEmpty(data)) {
                return new TraceRestoringCloseableSpanWithMetadata<>(original, translator, data);
            }
            return DEFAULT_CLOSEABLE_SPAN;
        }

        TraceRestoringCloseableSpanWithMetadata(@Nullable Trace original, TagTranslator<? super T> translator, T data) {
            this.original = original;
            this.translator = translator;
            this.data = data;
        }

        @Override
        public void close() {
            Tracer.fastCompleteSpan(translator, data);
            Trace originalTrace = original;
            if (originalTrace != null) {
                Tracer.setTrace(originalTrace);
            }
        }
    }

    private static final class SampledDetachedSpan implements DetachedSpan {
        private static final int NOT_COMPLETE = 0;
        private static final int COMPLETE = 1;

        private static final AtomicIntegerFieldUpdater<SampledDetachedSpan> completedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(SampledDetachedSpan.class, "completed");

        private final TraceState traceState;
        private final OpenSpan openSpan;

        private volatile int completed;

        @SuppressWarnings("ImmutablesBuilderMissingInitialization")
        // OpenSpan#builder sets these
        SampledDetachedSpan(String operation, SpanType type, TraceState traceState, Optional<String> parentSpanId) {
            this.traceState = traceState;
            this.openSpan = OpenSpan.of(operation, Tracers.randomId(), type, parentSpanId);
        }

        @MustBeClosed
        private static <T> CloseableSpan childSpan(
                TraceState traceState,
                OpenSpan openSpan,
                String operationName,
                TagTranslator<? super T> translator,
                T data,
                SpanType type) {
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(true, traceState));
            Tracer.fastStartSpan(operationName, openSpan.getSpanId(), type);
            return TraceRestoringCloseableSpanWithMetadata.of(maybeCurrentTrace, translator, data);
        }

        @Override
        @MustBeClosed
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> translator, T data, SpanType type) {
            return childSpan(traceState, openSpan, operationName, translator, data, type);
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            return new SampledDetachedSpan(operation, type, traceState, Optional.of(openSpan.getSpanId()));
        }

        @MustBeClosed
        private static CloseableSpan attach(OpenSpan openSpan, TraceState traceState) {
            Trace maybeCurrentTrace = currentTrace.get();
            Trace newTrace = Trace.of(true, traceState);
            // Push the DetachedSpan OpenSpan to provide the correct parent information
            // to child spans created within the context of this attach.
            // It is VITAL that this span is never completed, it exists only for attribution.
            newTrace.push(openSpan);
            setTrace(newTrace);
            // Do not complete the synthetic root span, it simply prevents nested spans from removing trace state, and
            // allows
            return maybeCurrentTrace == null ? REMOVE_TRACE : () -> Tracer.setTrace(maybeCurrentTrace);
        }

        @Override
        @MustBeClosed
        public CloseableSpan attach() {
            return attach(openSpan, traceState);
        }

        @Override
        public void complete() {
            complete(NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE);
        }

        @Override
        public <T> void complete(TagTranslator<? super T> tagTranslator, T data) {
            if (NOT_COMPLETE == completedUpdater.getAndSet(this, COMPLETE)) {
                Tracer.notifyObservers(toSpan(openSpan, tagTranslator, data, traceState.traceId()));
            }
        }

        @Override
        public String toString() {
            return "SampledDetachedSpan{completed="
                    + (completed == COMPLETE)
                    + ", traceState="
                    + traceState
                    + ", openSpan="
                    + openSpan
                    + '}';
        }
    }

    private static final class SampledDetached implements Detached {

        private final TraceState traceState;
        private final OpenSpan openSpan;

        SampledDetached(TraceState traceState, OpenSpan openSpan) {
            this.traceState = traceState;
            this.openSpan = openSpan;
        }

        @Override
        @MustBeClosed
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> translator, T data, SpanType type) {
            return SampledDetachedSpan.childSpan(traceState, openSpan, operationName, translator, data, type);
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            return new SampledDetachedSpan(operation, type, traceState, Optional.of(openSpan.getSpanId()));
        }

        @Override
        @MustBeClosed
        public CloseableSpan attach() {
            return SampledDetachedSpan.attach(openSpan, traceState);
        }

        @Override
        public String toString() {
            return "SampledDetached{traceState=" + traceState + ", openSpan=" + openSpan + '}';
        }
    }

    private static final class UnsampledDetachedSpan implements DetachedSpan {

        private final TraceState traceState;
        private final Optional<String> parentSpanId;

        UnsampledDetachedSpan(TraceState traceState, Optional<String> parentSpanId) {
            this.traceState = traceState;
            this.parentSpanId = parentSpanId;
        }

        @Override
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> _translator, T _data, SpanType type) {
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(false, traceState));
            if (parentSpanId.isPresent()) {
                Tracer.fastStartSpan(operationName, parentSpanId.get(), type);
            } else {
                Tracer.fastStartSpan(operationName, type);
            }
            return maybeCurrentTrace == null
                    ? DEFAULT_CLOSEABLE_SPAN
                    : new TraceRestoringCloseableSpan(maybeCurrentTrace);
        }

        @Override
        public DetachedSpan childDetachedSpan(String _operation, SpanType _type) {
            return this;
        }

        @Override
        @MustBeClosed
        public CloseableSpan attach() {
            // In the unsampled case this method is equivalent to 'childSpan' because spans are neither
            // measured nor emitted.
            return childSpan("SYNTHETIC_ATTACH");
        }

        @Override
        public void complete() {
            // nop
        }

        @Override
        public <T> void complete(TagTranslator<? super T> _tag, T _state) {
            // nop
        }

        @Override
        public String toString() {
            return "UnsampledDetachedSpan{traceState=" + traceState + '}';
        }
    }

    // Complete the current span.
    private static final CloseableSpan DEFAULT_CLOSEABLE_SPAN = Tracer::fastCompleteSpan;
    private static final CloseableSpan REMOVE_TRACE = Tracer::clearCurrentTrace;

    private static final class TraceRestoringCloseableSpan implements CloseableSpan {

        private final Trace original;

        TraceRestoringCloseableSpan(Trace original) {
            this.original = original;
        }

        @Override
        public void close() {
            Tracer.fastCompleteSpan();
            Tracer.setTrace(original);
        }
    }

    /**
     * Completes the current span (if it exists) and notifies all {@link #observers subscribers} about the completed
     * span.
     *
     * <p>Does not construct the Span object if no subscriber will see it.
     */
    public static void fastCompleteSpan() {
        fastCompleteSpan(NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE);
    }

    /**
     * Like {@link #fastCompleteSpan()}, but adds {@code metadata} to the current span being completed.
     */
    public static void fastCompleteSpan(@Safe Map<String, String> metadata) {
        fastCompleteSpan(MapTagTranslator.INSTANCE, metadata);
    }

    public static <T> void fastCompleteSpan(TagTranslator<? super T> tag, T state) {
        Trace trace = currentTrace.get();
        if (trace != null) {
            Optional<OpenSpan> span = popCurrentSpan(trace);
            if (trace.isObservable()) {
                completeSpanAndNotifyObservers(span, tag, state, trace.getTraceId());
            }
        } else if (log.isDebugEnabled()) {
            log.debug(
                    "Attempted to complete spans when there is no active Trace. This may be the "
                            + "result of calling completeSpan more times than startSpan",
                    new SafeRuntimeException("not a real exception"));
        }
    }

    private static <T> void completeSpanAndNotifyObservers(
            Optional<OpenSpan> openSpan, TagTranslator<? super T> tag, T state, String traceId) {
        //noinspection OptionalIsPresent - Avoid lambda allocation in hot paths
        if (openSpan.isPresent()) {
            Tracer.notifyObservers(toSpan(openSpan.get(), tag, state, traceId));
        }
    }

    /**
     * Completes and returns the current span (if it exists) and notifies all {@link #observers subscribers} about the
     * completed span. If the return value is not used, prefer {@link Tracer#fastCompleteSpan()}.
     */
    @CheckReturnValue
    public static Optional<Span> completeSpan() {
        return completeSpan(Collections.emptyMap());
    }

    /**
     * Like {@link #completeSpan()}, but adds {@code metadata} to the current span being completed.
     * If the return value is not used, prefer {@link Tracer#fastCompleteSpan(Map)}.
     *
     * @deprecated Use {@link #fastCompleteSpan()}
     */
    @CheckReturnValue
    @Deprecated
    public static Optional<Span> completeSpan(@Safe Map<String, String> metadata) {
        Trace trace = currentTrace.get();
        if (trace == null) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Attempted to complete spans when there is no active Trace. This may be the "
                                + "result of calling completeSpan more times than startSpan",
                        new SafeRuntimeException("not a real exception"));
            }
            return Optional.empty();
        }
        Optional<Span> maybeSpan = popCurrentSpan(trace)
                .map(openSpan -> toSpan(openSpan, MapTagTranslator.INSTANCE, metadata, trace.getTraceId()));

        // Notify subscribers iff trace is observable
        if (maybeSpan.isPresent() && trace.isObservable()) {
            // Avoid lambda allocation in hot paths
            notifyObservers(maybeSpan.get());
        }

        return maybeSpan;
    }

    private static void notifyObservers(Span span) {
        compositeObserver.accept(span);
    }

    private static Optional<OpenSpan> popCurrentSpan(Trace trace) {
        Optional<OpenSpan> span = trace.pop();
        if (trace.isEmpty()) {
            clearCurrentTrace();
        }
        return span;
    }

    private static <T> Span toSpan(OpenSpan openSpan, TagTranslator<? super T> translator, T state, String traceId) {
        Span.Builder builder = Span.builder()
                .traceId(traceId)
                .spanId(openSpan.getSpanId())
                .type(openSpan.type())
                .parentSpanId(openSpan.getParentSpanId())
                .operation(openSpan.getOperation())
                .startTimeMicroSeconds(openSpan.getStartTimeMicroSeconds())
                .durationNanoSeconds(System.nanoTime() - openSpan.getStartClockNanoSeconds());
        if (!translator.isEmpty(state)) {
            translator.translate(SpanBuilderTagAdapter.INSTANCE, builder, state);
        }
        return builder.build();
    }

    private enum SpanBuilderTagAdapter implements TagTranslator.TagAdapter<Span.Builder> {
        INSTANCE;

        @Override
        public void tag(Span.Builder target, String key, String value) {
            if (key != null && value != null) {
                target.putMetadata(key, value);
            }
        }

        @Override
        public void tag(Span.Builder target, Map<String, String> tags) {
            target.putAllMetadata(tags);
        }
    }

    /**
     * Subscribes the given (named) span observer to all "span completed" events. Observers are expected to be "cheap",
     * i.e., do all non-trivial work (logging, sending network messages, etc.) asynchronously. If an observer is already
     * registered for the given name, then it gets overwritten by this call. Returns the observer previously associated
     * with the given name, or null if there is no such observer.
     */
    public static synchronized SpanObserver subscribe(String name, SpanObserver observer) {
        if (observers.containsKey(name)) {
            log.warn(
                    "Overwriting existing SpanObserver with name {} by new observer: {}",
                    SafeArg.of("name", name),
                    UnsafeArg.of("observer", observer));
        }
        if (observers.size() >= 5) {
            log.warn("Five or more SpanObservers registered: {}", SafeArg.of("observers", observers.keySet()));
        }
        SpanObserver currentValue = observers.put(name, observer);
        computeObserversList();
        return currentValue;
    }

    /**
     * The inverse of {@link #subscribe}: removes the observer registered for the given name. Returns the removed
     * observer if it existed, or null otherwise.
     */
    public static synchronized SpanObserver unsubscribe(String name) {
        SpanObserver removedObserver = observers.remove(name);
        computeObserversList();
        return removedObserver;
    }

    private static void computeObserversList() {
        Consumer<Span> newCompositeObserver = _span -> {};
        for (Map.Entry<String, SpanObserver> entry : observers.entrySet()) {
            String observerName = entry.getKey();
            SpanObserver spanObserver = entry.getValue();
            newCompositeObserver = newCompositeObserver.andThen(span -> {
                try {
                    spanObserver.consume(span);
                } catch (RuntimeException e) {
                    log.error(
                            "Failed to invoke observer {} registered as {}",
                            SafeArg.of("observer", spanObserver),
                            SafeArg.of("name", observerName),
                            e);
                }
            });
        }
        // Single volatile write, updating observers should not disrupt tracing
        compositeObserver = newCompositeObserver;
    }

    /**
     * Sets the sampler (for all threads).
     */
    public static void setSampler(TraceSampler sampler) {
        Tracer.sampler = sampler;
    }

    /**
     * Returns true if there is an active trace on this thread.
     */
    public static boolean hasTraceId() {
        return currentTrace.get() != null;
    }

    /**
     * Returns the globally unique identifier for this thread's trace.
     */
    public static String getTraceId() {
        return checkNotNull(currentTrace.get(), "There is no trace").getTraceId();
    }

    /**
     * Returns the forUserAgent propagated inside the trace.
     */
    static Optional<String> getForUserAgent() {
        Trace trace = currentTrace.get();
        return trace == null ? Optional.empty() : trace.getForUserAgent();
    }

    /**
     * Returns the forUserAgent propagated inside the trace.
     */
    @Nullable
    static String getForUserAgent(DetachedSpan detachedSpan) {
        if (detachedSpan instanceof SampledDetachedSpan) {
            return ((SampledDetachedSpan) detachedSpan).traceState.forUserAgent();
        }
        if (detachedSpan instanceof UnsampledDetachedSpan) {
            return ((UnsampledDetachedSpan) detachedSpan).traceState.forUserAgent();
        }
        throw new SafeIllegalStateException("Unknown span type", SafeArg.of("detachedSpan", detachedSpan));
    }

    /**
     * Clears the current trace id and returns it if present.
     */
    static Optional<Trace> getAndClearTraceIfPresent() {
        Optional<Trace> trace = Optional.ofNullable(currentTrace.get());
        clearCurrentTrace();
        return trace;
    }

    /**
     * Clears the current trace id and returns (a copy of) it.
     */
    public static Trace getAndClearTrace() {
        Trace trace = getOrCreateCurrentTrace();
        clearCurrentTrace();
        return trace;
    }

    /**
     * True iff the spans of this thread's trace are to be observed by {@link SpanObserver span obververs} upon
     * {@link Tracer#completeSpan span completion}.
     */
    public static boolean isTraceObservable() {
        Trace trace = currentTrace.get();
        return trace != null && trace.isObservable();
    }

    /**
     * Returns true if there is an active trace which is not observable. This is equivalent to the result of
     * {@code Tracer.hasTraceId() && !Tracer.isTraceObservable()}.
     * This check is used frequently in hot paths to avoid unnecessary overhead in unsampled traces.
     */
    public static boolean hasUnobservableTrace() {
        Trace trace = currentTrace.get();
        return trace != null && !trace.isObservable();
    }

    /**
     * Returns an independent copy of this thread's {@link Trace}.
     */
    static Optional<Trace> copyTrace() {
        Trace trace = currentTrace.get();
        if (trace != null) {
            return Optional.of(trace.deepCopy());
        }
        return Optional.empty();
    }

    /**
     * Sets the thread-local trace. Considered an internal API used only for propagating the trace state across threads.
     */
    static void setTrace(Trace trace) {
        currentTrace.set(trace);

        // Give log appenders access to the trace id and whether the trace is being sampled
        MDC.put(Tracers.TRACE_ID_KEY, trace.getTraceId());
        setTraceSampledMdcIfObservable(trace.isObservable());
        setTraceRequestId(trace.maybeGetRequestId());

        logSettingTrace();
    }

    private static void setTraceSampledMdcIfObservable(boolean observable) {
        if (observable) {
            // Set to 1 to be consistent with values associated with http header key TraceHttpHeaders.IS_SAMPLED
            MDC.put(Tracers.TRACE_SAMPLED_KEY, "1");
        } else {
            // To ensure MDC state is cleared when trace is not observable
            MDC.remove(Tracers.TRACE_SAMPLED_KEY);
        }
    }

    private static void setTraceRequestId(@Nullable String requestId) {
        if (requestId == null) {
            // Ensure MDC state is cleared when there is no request identifier
            MDC.remove(Tracers.REQUEST_ID_KEY);
        } else {
            MDC.put(Tracers.REQUEST_ID_KEY, requestId);
        }
    }

    private static void logSettingTrace() {
        log.debug("Setting trace");
    }

    private static Trace getOrCreateCurrentTrace() {
        Trace trace = currentTrace.get();
        if (trace == null) {
            trace = createTrace(Observability.UNDECIDED, Tracers.randomId(), Optional.empty());
            setTrace(trace);
        }
        return trace;
    }

    @VisibleForTesting
    static void clearCurrentTrace() {
        logClearingTrace();
        currentTrace.set(null);
        MDC.remove(Tracers.TRACE_ID_KEY);
        MDC.remove(Tracers.TRACE_SAMPLED_KEY);
        MDC.remove(Tracers.REQUEST_ID_KEY);
    }

    private static void logClearingTrace() {
        if (log.isDebugEnabled()) {
            log.debug("Clearing current trace", SafeArg.of("trace", currentTrace.get()));
            if (log.isTraceEnabled()) {
                log.trace("Stacktrace at time of clearing trace", new SafeRuntimeException("not a real exception"));
            }
        }
    }
}
