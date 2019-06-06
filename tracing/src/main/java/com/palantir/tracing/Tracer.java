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

import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The singleton entry point for handling Zipkin-style traces and spans. Provides functionality for starting and
 * completing spans, and for subscribing observers to span completion events.
 * <p>
 * This class is thread-safe.
 */
public final class Tracer {

    private static final Logger log = LoggerFactory.getLogger(Tracer.class);

    private Tracer() {}

    // Thread-safe since thread-local
    private static final ThreadLocal<Trace> currentTrace = new ThreadLocal<>();

    // Only access in a class-synchronized fashion
    private static final Map<String, SpanObserver> observers = new HashMap<>();
    // we want iterating through tracers to be very fast, and it's faster to pre-define observer execution
    // when our observers are modified.
    private static volatile Consumer<Span> compositeObserver = span -> { };

    // Thread-safe since stateless
    private static volatile TraceSampler sampler = new RandomSampler(0.01f);

    /**
     * Creates a new trace, but does not set it as the current trace.
     */
    private static Trace createTrace(Observability observability, CharSequence traceId) {
        checkArgument(traceId != null && traceId.length() != 0, "traceId must be non-empty");
        boolean observable = shouldObserve(observability);
        return Trace.create(observable, traceId);
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
     * Deprecated.
     *
     * @deprecated Use {@link #initTrace(Observability, String)}
     */
    @Deprecated
    public static void initTrace(Optional<Boolean> isObservable, String traceId) {
        Observability observability = isObservable
                .map(value -> Boolean.TRUE.equals(value) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE)
                .orElse(Observability.UNDECIDED);
        createAndInitTrace(observability, traceId);
    }

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans.
     */
    public static void initTrace(Observability observability, String traceId) {
        createAndInitTrace(observability, traceId);
    }

    public static void initTrace(Observability observability, CharSequence traceId) {
        createAndInitTrace(observability, traceId);
    }

    private static void createAndInitTrace(Observability observability, CharSequence traceId) {
        setTrace(createTrace(observability, traceId));
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty.
     */
    public static OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        Trace current = getOrCreateCurrentTrace();
        checkState(current.isEmpty(),
                "Cannot start a span with explicit parent if the current thread's trace is non-empty");
        checkArgument(!Strings.isNullOrEmpty(parentSpanId), "parentSpanId must be non-empty");
        OpenSpan span = OpenSpan.builder()
                .spanId(Tracers.randomId())
                .operation(operation)
                .parentSpanId(parentSpanId)
                .type(type)
                .build();
        current.push(span);
        return span;
    }

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}.
     */
    public static OpenSpan startSpan(String operation, SpanType type) {
        return startSpanInternal(operation, type);
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     */
    public static OpenSpan startSpan(String operation) {
        return startSpanInternal(operation, SpanType.LOCAL);
    }

    private static OpenSpan startSpanInternal(String operation, SpanType type) {
        OpenSpan.Builder spanBuilder = OpenSpan.builder()
                .operation(operation)
                .spanId(Tracers.randomId())
                .type(type);

        Trace trace = getOrCreateCurrentTrace();
        Optional<OpenSpan> prevState = trace.top();
        // Avoid lambda allocation in hot paths
        if (prevState.isPresent()) {
            spanBuilder.parentSpanId(prevState.get().getSpanId());
        }

        OpenSpan span = spanBuilder.build();
        trace.push(span);
        return span;
    }

    /** Discards the current span without emitting it. */
    static void fastDiscardSpan() {
        Trace trace = currentTrace.get();
        checkNotNull(trace, "Expected current trace to exist");
        checkState(popCurrentSpan().isPresent(), "Expected span to exist before discarding");
    }

    /**
     * Completes the current span (if it exists) and notifies all {@link #observers subscribers} about the
     * completed span.
     *
     * Does not construct the Span object if no subscriber will see it.
     */
    public static void fastCompleteSpan() {
        fastCompleteSpan(Collections.emptyMap());
    }

    /**
     * Like {@link #fastCompleteSpan()}, but adds {@code metadata} to the current span being completed.
     */
    public static void fastCompleteSpan(Map<String, String> metadata) {
        Trace trace = currentTrace.get();
        if (trace != null) {
            Optional<OpenSpan> span = popCurrentSpan();
            if (trace.isObservable()) {
                span.map(openSpan -> toSpan(openSpan, metadata, trace.getTraceId()))
                        .ifPresent(Tracer::notifyObservers);
            }
        }
    }

    /**
     * Completes and returns the current span (if it exists) and notifies all {@link #observers subscribers} about the
     * completed span.
     * If the return value is not used, prefer {@link Tracer#fastCompleteSpan()}.
     */
    @CheckReturnValue
    public static Optional<Span> completeSpan() {
        return completeSpan(Collections.emptyMap());
    }

    /**
     * Like {@link #completeSpan()}, but adds {@code metadata} to the current span being completed.
     * If the return value is not used, prefer {@link Tracer#fastCompleteSpan(Map)}.
     */
    @CheckReturnValue
    public static Optional<Span> completeSpan(Map<String, String> metadata) {
        Trace trace = currentTrace.get();
        if (trace == null) {
            return Optional.empty();
        }
        Optional<Span> maybeSpan = popCurrentSpan()
                .map(openSpan -> toSpan(openSpan, metadata, trace.getTraceId()));

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

    private static Optional<OpenSpan> popCurrentSpan() {
        Trace trace = currentTrace.get();
        if (trace != null) {
            Optional<OpenSpan> span = trace.pop();
            if (trace.isEmpty()) {
                clearCurrentTrace();
            }
            return span;
        }
        return Optional.empty();
    }

    private static Span toSpan(OpenSpan openSpan, Map<String, String> metadata, String traceId) {
        return Span.builder()
                .traceId(traceId)
                .spanId(openSpan.getSpanId())
                .type(openSpan.type())
                .parentSpanId(openSpan.getParentSpanId())
                .operation(openSpan.getOperation())
                .startTimeMicroSeconds(openSpan.getStartTimeMicroSeconds())
                .durationNanoSeconds(System.nanoTime() - openSpan.getStartClockNanoSeconds())
                .putAllMetadata(metadata)
                .build();
    }

    /**
     * Subscribes the given (named) span observer to all "span completed" events. Observers are expected to be "cheap",
     * i.e., do all non-trivial work (logging, sending network messages, etc) asynchronously. If an observer is already
     * registered for the given name, then it gets overwritten by this call. Returns the observer previously associated
     * with the given name, or null if there is no such observer.
     */
    public static synchronized SpanObserver subscribe(String name, SpanObserver observer) {
        if (observers.containsKey(name)) {
            log.warn("Overwriting existing SpanObserver with name {} by new observer: {}",
                    SafeArg.of("name", name),
                    UnsafeArg.of("observer", observer));
        }
        if (observers.size() >= 5) {
            log.warn("Five or more SpanObservers registered: {}",
                    SafeArg.of("observers", observers.keySet()));
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
        Consumer<Span> newCompositeObserver = span -> { };
        for (Map.Entry<String, SpanObserver> entry : observers.entrySet()) {
            String observerName = entry.getKey();
            SpanObserver spanObserver = entry.getValue();
            newCompositeObserver = newCompositeObserver.andThen(span -> {
                try {
                    spanObserver.consume(span);
                } catch (RuntimeException e) {
                    log.error("Failed to invoke observer {} registered as {}",
                            SafeArg.of("observer", spanObserver),
                            SafeArg.of("name", observerName),
                            e);
                }
            });
        }
        // Single volatile write, updating observers should not disrupt tracing
        compositeObserver = newCompositeObserver;
    }

    /** Sets the sampler (for all threads). */
    public static void setSampler(TraceSampler sampler) {
        Tracer.sampler = sampler;
    }

    /** Returns true if there is an active trace on this thread. */
    public static boolean hasTraceId() {
        return currentTrace.get() != null;
    }

    /** Returns the globally unique identifier for this thread's trace. */
    public static String getTraceId() {
        return checkNotNull(currentTrace.get(), "There is no trace").getTraceId();
    }

    /** Clears the current trace id and returns it if present. */
    static Optional<Trace> getAndClearTraceIfPresent() {
        Optional<Trace> trace = Optional.ofNullable(currentTrace.get());
        clearCurrentTrace();
        return trace;
    }

    /** Clears the current trace id and returns (a copy of) it. */
    public static Trace getAndClearTrace() {
        Trace trace = getOrCreateCurrentTrace();
        clearCurrentTrace();
        return trace;
    }

    /**
     * True iff the spans of this thread's trace are to be observed by {@link SpanObserver span obververs} upon {@link
     * Tracer#completeSpan span completion}.
     */
    public static boolean isTraceObservable() {
        Trace trace = currentTrace.get();
        return trace != null && trace.isObservable();
    }

    /** Returns an independent copy of this thread's {@link Trace}. */
    static Optional<Trace> copyTrace() {
        Trace trace = currentTrace.get();
        if (trace != null) {
            return Optional.of(trace.deepCopy());
        }
        return Optional.empty();
    }

    /**
     * Sets the thread-local trace. Considered an internal API used only for propagating the trace state across
     * threads.
     */
    static void setTrace(Trace trace) {
        currentTrace.set(trace);

        // Give log appenders access to the trace id and whether the trace is being sampled
        MDC.put(Tracers.TRACE_ID_KEY, trace.getTraceId());
        setTraceSampledMdcIfObservable(trace.isObservable());
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

    private static Trace getOrCreateCurrentTrace() {
        Trace trace = currentTrace.get();
        if (trace == null) {
            trace = createTrace(Observability.UNDECIDED, Tracers.lazyRandomId());
            setTrace(trace);
        }
        return trace;
    }

    private static void clearCurrentTrace() {
        currentTrace.remove();
        MDC.remove(Tracers.TRACE_ID_KEY);
        MDC.remove(Tracers.TRACE_SAMPLED_KEY);
    }
}
