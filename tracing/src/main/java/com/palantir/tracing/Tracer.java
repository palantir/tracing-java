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
import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
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
    private static Trace createTrace(
            Observability observability, String traceId, Optional<String> requestId, String originUserAgent) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        checkArgument(!Strings.isNullOrEmpty(originUserAgent), "originUserAgent must ne non-empty");
        boolean observable = shouldObserve(observability);
        return Trace.of(observable, traceId, requestId, originUserAgent);
    }

    /**
     * Creates a new trace, but does not set it as the current trace.
     */
    private static Trace createTrace(Observability observability, String traceId, Optional<String> requestId) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        boolean observable = shouldObserve(observability);
        return Trace.of(observable, traceId, requestId);
    }

    private static boolean shouldObserve(Observability observability) {
        switch (observability) {
            case SAMPLE:
                return true;
            case DO_NOT_SAMPLE:
                return false;
            case UNDECIDED:
                return sampler.sample();
        }

        throw new SafeIllegalArgumentException("Unknown observability", SafeArg.of("observability", observability));
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

        if (trace.isObservable()) {
            return trace.top().map(openSpan -> TraceMetadata.builder()
                    .spanId(openSpan.getSpanId())
                    .parentSpanId(openSpan.getParentSpanId())
                    .originatingSpanId(trace.getOriginatingSpanId())
                    .traceId(trace.getTraceId())
                    .requestId(trace.getRequestId())
                    .build());
        } else {
            return Optional.of(TraceMetadata.builder()
                    .spanId(Tracers.randomId())
                    .parentSpanId(Optional.empty())
                    .originatingSpanId(trace.getOriginatingSpanId())
                    .traceId(trace.getTraceId())
                    .requestId(trace.getRequestId())
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
     */
    public static void initTraceWithSpan(
            Observability observability,
            String traceId,
            @Safe String operation,
            String parentSpanId,
            SpanType type,
            String originUserAgent) {
        setTrace(createTrace(
                observability,
                traceId,
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty(),
                originUserAgent));
        fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
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
        String traceId = maybeCurrentTrace != null ? maybeCurrentTrace.getTraceId() : Tracers.randomId();
        boolean sampled = maybeCurrentTrace != null ? maybeCurrentTrace.isObservable() : sampler.sample();
        Optional<String> parentSpan = getParentSpanId(maybeCurrentTrace);
        Optional<String> requestId = getRequestId(maybeCurrentTrace, type);
        return sampled
                ? new SampledDetachedSpan(operation, type, traceId, requestId, parentSpan)
                : new UnsampledDetachedSpan(traceId, requestId, parentSpan);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(
            Observability observability,
            String traceId,
            Optional<String> parentSpanId,
            @Safe String operation,
            SpanType type) {
        Optional<String> requestId =
                type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty();
        return detachInternal(observability, traceId, requestId, parentSpanId, operation, type);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(
            Observability observability,
            String traceId,
            Optional<String> requestId,
            Optional<String> parentSpanId,
            @Safe String operation,
            SpanType type) {
        // The current trace has no impact on this function, a new trace is spawned and existing thread state
        // is not modified.
        return shouldObserve(observability)
                ? new SampledDetachedSpan(operation, type, traceId, requestId, parentSpanId)
                : new UnsampledDetachedSpan(traceId, requestId, parentSpanId);
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

    private static Optional<String> getRequestId(@Nullable Trace maybeCurrentTrace, SpanType newSpanType) {
        if (maybeCurrentTrace != null) {
            return maybeCurrentTrace.getRequestId();
        }
        if (newSpanType == SpanType.SERVER_INCOMING) {
            return Optional.of(Tracers.randomId());
        }
        return Optional.empty();
    }

    static Optional<String> getRequestId(DetachedSpan detachedSpan) {
        if (detachedSpan instanceof SampledDetachedSpan) {
            return ((SampledDetachedSpan) detachedSpan).requestId;
        }
        if (detachedSpan instanceof UnsampledDetachedSpan) {
            return ((UnsampledDetachedSpan) detachedSpan).requestId;
        }
        throw new SafeIllegalStateException("Unknown span type", SafeArg.of("detachedSpan", detachedSpan));
    }

    static boolean isSampled(DetachedSpan detachedSpan) {
        return detachedSpan instanceof SampledDetachedSpan;
    }

    private static final class SampledDetachedSpan implements DetachedSpan {
        private static final int NOT_COMPLETE = 0;
        private static final int COMPLETE = 1;

        private static final AtomicIntegerFieldUpdater<SampledDetachedSpan> completedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(SampledDetachedSpan.class, "completed");

        private final String traceId;
        private final Optional<String> requestId;
        private final OpenSpan openSpan;

        private volatile int completed;

        @SuppressWarnings("ImmutablesBuilderMissingInitialization")
        // OpenSpan#builder sets these
        SampledDetachedSpan(
                String operation,
                SpanType type,
                String traceId,
                Optional<String> requestId,
                Optional<String> parentSpanId) {
            this.traceId = traceId;
            this.requestId = requestId;
            this.openSpan = OpenSpan.builder()
                    .parentSpanId(parentSpanId)
                    .spanId(Tracers.randomId())
                    .operation(operation)
                    .type(type)
                    .build();
        }

        @Override
        @MustBeClosed
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> translator, T data, SpanType type) {
            warnIfCompleted("startSpanOnCurrentThread");
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(true, traceId, requestId));
            Tracer.fastStartSpan(operationName, openSpan.getSpanId(), type);
            return TraceRestoringCloseableSpanWithMetadata.of(maybeCurrentTrace, translator, data);
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            warnIfCompleted("startDetachedSpan");
            return new SampledDetachedSpan(operation, type, traceId, requestId, Optional.of(openSpan.getSpanId()));
        }

        @Override
        public void complete() {
            complete(NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE);
        }

        @Override
        public <T> void complete(TagTranslator<? super T> tagTranslator, T data) {
            if (NOT_COMPLETE == completedUpdater.getAndSet(this, COMPLETE)) {
                Tracer.notifyObservers(toSpan(openSpan, tagTranslator, data, traceId));
            }
        }

        @Override
        public String toString() {
            return "SampledDetachedSpan{completed="
                    + (completed == COMPLETE)
                    + ", traceId='"
                    + traceId
                    + '\''
                    + ", requestId='"
                    + requestId.orElse(null)
                    + '\''
                    + ", openSpan="
                    + openSpan
                    + '}';
        }

        private void warnIfCompleted(String feature) {
            if (completed == COMPLETE) {
                log.warn(
                        "{} called after span {} completed",
                        SafeArg.of("feature", feature),
                        SafeArg.of("detachedSpan", this));
            }
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

            TraceRestoringCloseableSpanWithMetadata(
                    @Nullable Trace original, TagTranslator<? super T> translator, T data) {
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
    }

    private static final class UnsampledDetachedSpan implements DetachedSpan {

        private final String traceId;
        private final Optional<String> requestId;
        private final Optional<String> parentSpanId;

        UnsampledDetachedSpan(String traceId, Optional<String> requestId, Optional<String> parentSpanId) {
            this.traceId = traceId;
            this.requestId = requestId;
            this.parentSpanId = parentSpanId;
        }

        @Override
        public <T> CloseableSpan childSpan(
                String operationName, TagTranslator<? super T> _translator, T _data, SpanType type) {
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(false, traceId, requestId));
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
        public void complete() {
            // nop
        }

        @Override
        public <T> void complete(TagTranslator<? super T> _tag, T _state) {
            // nop
        }

        @Override
        public String toString() {
            return "UnsampledDetachedSpan{traceId='" + traceId + "', requestId='" + requestId.orElse(null) + "'}";
        }
    }

    // Complete the current span.
    private static final CloseableSpan DEFAULT_CLOSEABLE_SPAN = Tracer::fastCompleteSpan;

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
     * Discards the current span without emitting it.
     */
    static void fastDiscardSpan() {
        Trace trace = currentTrace.get();
        checkNotNull(trace, "Expected current trace to exist");
        checkState(!trace.isEmpty(), "Expected span to exist before discarding");
        popCurrentSpan(trace);
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
        }
    }

    private static <T> void completeSpanAndNotifyObservers(
            Optional<OpenSpan> openSpan, TagTranslator<? super T> tag, T state, String traceId) {
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
     * i.e., do all non-trivial work (logging, sending network messages, etc) asynchronously. If an observer is already
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
        setTraceRequestId(trace.getRequestId());

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

    private static void setTraceRequestId(Optional<String> requestId) {
        if (requestId.isPresent()) {
            MDC.put(Tracers.REQUEST_ID_KEY, requestId.get());
        } else {
            // Ensure MDC state is cleared when there is no request identifier
            MDC.remove(Tracers.REQUEST_ID_KEY);
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
        currentTrace.remove();
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
