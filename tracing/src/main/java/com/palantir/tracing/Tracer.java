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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.OpenSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

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
    private static final ThreadLocal<Trace> currentTrace = ThreadLocal.withInitial(Trace::new);

    // Only access in a class-synchronized fashion
    @GuardedBy("Tracer.class")
    private static final ConcurrentHashMap<String, SpanObserver> spanObservers = new ConcurrentHashMap<>();
    // we want iterating through tracers to be very fast, and it's faster to pre-define observer execution
    // when our observers are modified.
    private static volatile SpanObserver spanObserver = _span -> {};

    // Thread-safe since stateless
    private static volatile TraceSampler sampler = RandomSampler.create(0.0005f);

    // Thread-safe since stateless
    private static volatile SpanFilter filter = DisabledSpanFilter.INSTANCE;

    private static boolean shouldObserve(Observability observability) {
        // Simplified implementation of 'switch(observability) {' for fast inlining (30 bytes)
        return observability == Observability.SAMPLE || (observability == Observability.UNDECIDED && sampler.sample());
    }

    private static Optional<String> toSpanId(@Nullable EnabledSpan span) {
        if (span == null) {
            return Optional.empty();
        }

        return Optional.of(span.spanId());
    }

    private static Optional<String> toRequestId(SpanType type) {
        return type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty();
    }

    /**
     * In the unsampled case, the Trace.Unsampled class doesn't actually store a span stack, so we just
     * make one up (just in time). This matches the behaviour of Tracer#startSpan.
     *
     * <p>n.b. this is a bit funky because calling maybeGetTraceMetadata multiple times will return different spanIds
     */
    // TODO(pkoenig): This implementation needs to be fixed, do we add nullable methods to SpanStackEntry?
    public static Optional<TraceMetadata> maybeGetTraceMetadata() {
        EnabledSpan currentSpan = currentTrace.get().current();
        if (currentSpan == null) {
            return Optional.empty();
        }

        return Optional.of(TraceMetadata.builder()
                .traceId(currentSpan.traceState().traceId())
                .spanId(currentSpan.spanId())
                .requestId(Optional.ofNullable(currentSpan.traceState().requestId()))
                .build());
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

        initTrace(observability, traceId);
    }

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, String, SpanType)}
     */
    @Deprecated
    public static void initTrace(Observability observability, String traceId) {
        TraceState traceState =
                TraceState.of(traceId, Optional.empty(), Optional.empty(), shouldObserve(observability));

        currentTrace.get().reset(traceState);
    }

    static EnabledTrace newTrace(Observability observability, String traceId) {
        return new EnabledTrace(
                TraceState.of(traceId, Optional.empty(), Optional.empty(), shouldObserve(observability)));
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
        TraceState traceState =
                TraceState.of(traceId, toRequestId(type), Optional.empty(), shouldObserve(observability));

        currentTrace.get().reset(traceState);

        fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability, String traceId, @Safe String operation, SpanType type) {
        TraceState traceState =
                TraceState.of(traceId, toRequestId(type), Optional.empty(), shouldObserve(observability));

        currentTrace.get().reset(traceState);

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
        TraceState traceState = TraceState.of(traceId, toRequestId(type), forUserAgent, shouldObserve(observability));

        currentTrace.get().reset(traceState);

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
        TraceState traceState = TraceState.of(traceId, toRequestId(type), forUserAgent, shouldObserve(observability));

        currentTrace.get().reset(traceState);

        fastStartSpan(operation, type);
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty. If the return value is not used, prefer {@link Tracer#fastStartSpan(String,
     * String, SpanType)}}.
     */
    // TODO(pkoenig): Can we just return dummy value here?
    @CheckReturnValue
    public static com.palantir.tracing.api.OpenSpan startSpan(
            @Safe String operation, String parentSpanId, SpanType type) {
        checkArgument(!Strings.isNullOrEmpty(parentSpanId), "parentSpanId must be non-empty");

        Trace trace = getOrCreateCurrentTrace();
        TraceState traceState = checkNotNull(trace.traceState());

        EnabledSpan span = new EnabledSpan(traceState, Tracers.randomId(), Optional.of(parentSpanId), operation, type);
        span.start();

        if (traceState.isObservable()) {
            trace.push(span);
        } else {
            // We need to push some state because callers are going to call completeSpan()
            trace.push(traceState);
        }

        return toOpenSpan(span);
    }

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}. If the
     * return value is not used, prefer {@link Tracer#fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    public static com.palantir.tracing.api.OpenSpan startSpan(@Safe String operation, SpanType type) {
        Trace trace = getOrCreateCurrentTrace();
        TraceState traceState = checkNotNull(trace.traceState());

        EnabledSpan span = new EnabledSpan(traceState, Tracers.randomId(), toSpanId(trace.current()), operation, type);
        span.start();

        if (traceState.isObservable()) {
            trace.push(span);
        } else {
            // We need to push some state because callers are going to call completeSpan()
            trace.push(traceState);
        }

        return toOpenSpan(span);
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String)}}.
     */
    @CheckReturnValue
    public static com.palantir.tracing.api.OpenSpan startSpan(@Safe String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, String parentSpanId, SpanType type) {
        checkArgument(!Strings.isNullOrEmpty(parentSpanId), "parentSpanId must be non-empty");

        Trace trace = getOrCreateCurrentTrace();
        TraceState traceState = checkNotNull(trace.traceState());

        if (traceState.isObservable()) {
            EnabledSpan span =
                    new EnabledSpan(traceState, Tracers.randomId(), Optional.of(parentSpanId), operation, type);
            span.start();
            trace.push(span);
        } else {
            // We need to push some state because callers are going to call fastCompleteSpan()
            trace.push(traceState);
        }
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, SpanType type) {
        Trace trace = getOrCreateCurrentTrace();
        TraceState traceState = checkNotNull(trace.traceState());

        if (traceState.isObservable()) {
            EnabledSpan span =
                    new EnabledSpan(traceState, Tracers.randomId(), toSpanId(trace.current()), operation, type);
            span.start();
            trace.push(span);
        } else {
            // We need to push some state because callers are going to call fastCompleteSpan()
            trace.push(traceState);
        }
    }

    /**
     * Like {@link #startSpan(String)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation) {
        fastStartSpan(operation, SpanType.LOCAL);
    }

    // TODO(pkoenig): Define parent type?
    static InternalSpan newSpan(String operation, SpanType type) {
        Trace trace = currentTrace.get();
        TraceState traceState = trace.traceState();

        if (traceState != null && traceState.isObservable()) {
            return new EnabledSpan(traceState, Tracers.randomId(), toSpanId(trace.current()), operation, type);
        }

        return DisabledSpan.INSTANCE;
    }

    static InternalSpan newSpan(String operation, Optional<String> parentSpanId, SpanType type) {
        return newSpan(currentTrace.get().traceState(), operation, parentSpanId, type);
    }

    static InternalSpan newSpan(
            @Nullable TraceState traceState, String operation, Optional<String> parentSpanId, SpanType type) {
        if (traceState != null && traceState.isObservable()) {
            return new EnabledSpan(traceState, Tracers.randomId(), parentSpanId, operation, type);
        }

        return DisabledSpan.INSTANCE;
    }

    static InternalSpan newSpan(Level level, String operation, SpanType type) {
        Trace trace = currentTrace.get();
        TraceState traceState = trace.traceState();

        if (traceState != null && filter.isEnabled(level, operation, traceState.isObservable())) {
            return new EnabledSpan(traceState, Tracers.randomId(), toSpanId(trace.current()), operation, type);
        }

        return DisabledSpan.INSTANCE;
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state. This is an internal
     * utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(@Safe String operation, SpanType type) {
        TraceState traceState;
        Optional<String> parentSpanId;

        EnabledSpan currentSpan = currentTrace.get().current();
        if (currentSpan != null) {
            traceState = currentSpan.traceState();
            parentSpanId = Optional.of(currentSpan.spanId());
        } else {
            traceState = TraceState.of(
                    Tracers.randomId(),
                    type == SpanType.SERVER_INCOMING ? Optional.of(Tracers.randomId()) : Optional.empty(),
                    Optional.empty(),
                    sampler.sample());
            parentSpanId = Optional.empty();
        }

        return new DetachedSpanImpl(traceState, parentSpanId, operation, type);
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
        // The current trace has no impact on this function, a new trace is spawned and existing thread state
        // is not modified.
        TraceState traceState = TraceState.of(traceId, requestId, forUserAgent, shouldObserve(observability));
        return new DetachedSpanImpl(traceState, parentSpanId, operation, type);
    }

    /**
     * Like {@link #detachInternal(String, SpanType)} but does not create a new span and may return a
     * no-op implementation if no tracing state is currently set.
     */
    static Detached detachInternal() {
        EnabledSpan currentSpan = currentTrace.get().current();
        if (currentSpan == null) {
            return NopDetached.INSTANCE;
        }

        if (currentSpan.traceState().isObservable()) {
            if (currentSpan == null) {
                return NopDetached.INSTANCE;
            }
            return new DetachedSpanImpl(
                    currentSpan.traceState(),
                    currentSpan.spanId(),
                    currentSpan.parentSpanId(),
                    currentSpan.operation(),
                    currentSpan.type());
        } else {
            return new UnsampledDetachedSpan(currentSpan.traceState(), Optional.empty());
        }
    }

    static boolean isEnabled(Level level, String operation) {
        return filter.isEnabled(level, operation, Tracer.isTraceObservable());
    }

    /**
     * Completes the current span (if it exists) and notifies all {@link #spanObservers subscribers} about the completed
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

    public static <T> void fastCompleteSpan(TagTranslator<? super T> tagTranslator, T data) {
        Trace trace = currentTrace.get();

        EnabledSpan span = trace.pop();
        if (span == null) {
            return;
        }

        span.complete();
        notifyObservers(span, tagTranslator, data);
    }

    /**
     * Completes and returns the current span (if it exists) and notifies all {@link #spanObservers subscribers} about the
     * completed span. If the return value is not used, prefer {@link Tracer#fastCompleteSpan()}.
     */
    @CheckReturnValue
    public static Optional<com.palantir.tracing.api.Span> completeSpan() {
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
    public static Optional<com.palantir.tracing.api.Span> completeSpan(@Safe Map<String, String> metadata) {
        Trace trace = currentTrace.get();

        EnabledSpan span = trace.pop();
        if (span == null) {
            return Optional.empty();
        }

        span.complete();
        com.palantir.tracing.api.Span completedSpan = notifyObservers(span, MapTagTranslator.INSTANCE, metadata);

        return Optional.of(completedSpan);
    }

    static void pushEntry(SpanStackEntry entry) {
        currentTrace.get().push(entry);
    }

    static void removeEntry(SpanStackEntry entry) {
        Trace trace = currentTrace.get();

        trace.remove(entry);
    }

    static void notifyObservers(EnabledSpan span) {
        notifyObservers(span, NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE);
    }

    static <T> com.palantir.tracing.api.Span notifyObservers(
            EnabledSpan span, TagTranslator<? super T> translator, T state) {
        com.palantir.tracing.api.Span completedSpan = toSpan(span, translator, state);
        spanObserver.consume(completedSpan);

        return completedSpan;
    }

    private static com.palantir.tracing.api.OpenSpan toOpenSpan(EnabledSpan span) {
        return new com.palantir.tracing.api.OpenSpan.Builder()
                .spanId(span.spanId())
                .parentSpanId(span.parentSpanId())
                .operation(span.operation())
                .type(span.type())
                .startTimeMicroSeconds(span.startTimeMicroSeconds())
                .startClockNanoSeconds(span.startClockNanoSeconds())
                .build();
    }

    private static <T> com.palantir.tracing.api.Span toSpan(
            EnabledSpan span, TagTranslator<? super T> translator, T state) {
        com.palantir.tracing.api.Span.Builder builder = com.palantir.tracing.api.Span.builder()
                .traceId(span.traceState().traceId())
                .spanId(span.spanId())
                .type(span.type())
                .parentSpanId(span.parentSpanId())
                .operation(span.operation())
                .startTimeMicroSeconds(span.startTimeMicroSeconds())
                .durationNanoSeconds(System.nanoTime() - span.startClockNanoSeconds())
                .putAllMetadata(span.tags());
        if (!translator.isEmpty(state)) {
            translator.translate(SpanBuilderTagAdapter.INSTANCE, builder, state);
        }
        return builder.build();
    }

    /**
     * Subscribes the given (named) span observer to all "span completed" events. Observers are expected to be "cheap",
     * i.e., do all non-trivial work (logging, sending network messages, etc.) asynchronously. If an observer is already
     * registered for the given name, then it gets overwritten by this call. Returns the observer previously associated
     * with the given name, or null if there is no such observer.
     */
    @Nullable
    public static synchronized SpanObserver subscribe(String name, SpanObserver observer) {
        SpanObserver currentValue = spanObservers.put(name, observer);
        if (currentValue != null) {
            log.warn(
                    "Overwriting existing SpanObserver with name {} by new observer: {}",
                    SafeArg.of("name", name),
                    UnsafeArg.of("observer", observer));
        }
        if (spanObservers.size() >= 5) {
            log.warn("Five or more SpanObservers registered: {}", SafeArg.of("observers", spanObservers.keySet()));
        }
        computeSpanObserver();
        return currentValue;
    }

    /**
     * The inverse of {@link #subscribe}: removes the observer registered for the given name. Returns the removed
     * observer if it existed, or null otherwise.
     */
    public static synchronized SpanObserver unsubscribe(String name) {
        SpanObserver removedObserver = spanObservers.remove(name);
        computeSpanObserver();
        return removedObserver;
    }

    @GuardedBy("Tracer.class")
    private static void computeSpanObserver() {
        List<Entry<String, SpanObserver>> observers = new ArrayList<>(spanObservers.entrySet());
        spanObserver = span -> {
            for (Entry<String, SpanObserver> entry : observers) {
                try {
                    entry.getValue().consume(span);
                } catch (RuntimeException e) {
                    log.error(
                            "Failed to invoke span observer",
                            SafeArg.of("name", entry.getKey()),
                            SafeArg.of("observer", entry.getValue()),
                            e);
                }
            }
        };
    }

    /**
     * Sets the sampler (for all threads).
     */
    public static void setSampler(TraceSampler sampler) {
        Tracer.sampler = sampler;
    }

    /**
     * Sets the filter (for all threads).
     */
    public static void setFilter(SpanFilter filter) {
        Tracer.filter = filter;
    }

    /**
     * Returns true if there is an active trace on this thread.
     */
    public static boolean hasTraceId() {
        return !currentTrace.get().isEmpty();
    }

    /**
     * Returns the globally unique identifier for this thread's trace.
     */
    public static String getTraceId() {
        return checkNotNull(currentTrace.get().current(), "There is no trace")
                .traceState()
                .traceId();
    }

    /**
     * Returns the forUserAgent propagated inside the trace.
     */
    static Optional<String> getForUserAgent() {
        EnabledSpan currentSpan = currentTrace.get().current();
        if (currentSpan == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(currentSpan.traceState().forUserAgent());
    }

    /**
     * Clears the current trace id and returns it if present.
     */
    static Optional<Trace> getAndClearTraceIfPresent() {
        currentTrace.get().reset(null);
        return Optional.empty();
    }

    /**
     * Clears the current trace id and returns (a copy of) it.
     */
    public static Trace getAndClearTrace() {
        Trace trace = currentTrace.get();
        trace.reset(null);
        return trace;
    }

    /**
     * True iff the spans of this thread's trace are to be observed by {@link SpanObserver span obververs} upon
     * {@link Tracer#completeSpan span completion}.
     */
    public static boolean isTraceObservable() {
        TraceState traceState = currentTrace.get().traceState();
        if (traceState == null) {
            return false;
        }

        return traceState.isObservable();
    }

    /**
     * Returns true if there is an active trace which is not observable. This is equivalent to the result of
     * {@code Tracer.hasTraceId() && !Tracer.isTraceObservable()}.
     * This check is used frequently in hot paths to avoid unnecessary overhead in unsampled traces.
     */
    public static boolean hasUnobservableTrace() {
        TraceState traceState = currentTrace.get().traceState();
        if (traceState == null) {
            return false;
        }

        return !traceState.isObservable();
    }

    @Nullable
    static Trace getTrace() {
        return currentTrace.get();
    }

    @Nullable
    static TraceState getTraceState() {
        return currentTrace.get().traceState();
    }

    @Nullable
    static TraceState getTraceState(DetachedSpan detachedSpan) {
        if (detachedSpan instanceof DetachedSpanImpl detachedSpanImpl) {
            return detachedSpanImpl.traceState();
        } else {
            return null;
        }
    }

    private static Trace getOrCreateCurrentTrace() {
        Trace trace = currentTrace.get();

        if (trace.isEmpty()) {
            TraceState traceState = TraceState.of(
                    Tracers.randomId(), Optional.empty(), Optional.empty(), shouldObserve(Observability.UNDECIDED));
            trace.push(traceState);
        }

        return trace;
    }

    /**
     * Sets the thread-local trace. Considered an internal API used only for propagating the trace state across threads.
     */
    static void setTrace(Trace trace) {
        currentTrace.set(trace);
    }

    @VisibleForTesting
    static void clearCurrentTrace() {
        //noinspection ThreadLocalSetWithNull explicitly not removing thread local to avoid churn, see PR #849
        currentTrace.get().reset(null);
    }
}
