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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    // we want iterating through tracers to be very fast, and it's faster to iterate through a list than a Map.values()
    private static volatile List<SpanObserver> observersList = ImmutableList.of();

    // Thread-safe since stateless
    private static volatile TraceSampler sampler = AlwaysSampler.INSTANCE;

    /**
     * Creates a new trace, but does not set it as the current trace. The new trace is {@link Trace#isObservable
     * observable} iff the given flag is true, or, iff {@code isObservable} is absent, if the {@link #setSampler
     * configured sampler} returns true.
     */
    private static Trace createTrace(Optional<Boolean> isObservable, String traceId) {
        Preconditions.checkArgument(traceId != null && !traceId.isEmpty(), "traceId must be non-empty: %s", traceId);
        boolean observable = isObservable.orElseGet(sampler::sample);
        return new Trace(observable, traceId);
    }

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans. The new trace is {@link
     * Trace#isObservable observable} iff the given flag is true, or, iff {@code isObservable} is absent, if the {@link
     * #setSampler configured sampler} returns true.
     */
    public static void initTrace(Optional<Boolean> isObservable, String traceId) {
        setTrace(createTrace(isObservable, traceId));
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty.
     */
    public static OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        Trace current = getOrCreateCurrentTrace();
        Preconditions.checkState(current.isEmpty(),
                "Cannot start a span with explicit parent if the current thread's trace is non-empty");
        Preconditions.checkArgument(parentSpanId != null && !parentSpanId.isEmpty(),
                "parentTraceId must be non-empty: %s", parentSpanId);
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
        if (prevState.isPresent()) {
            spanBuilder.parentSpanId(prevState.get().getSpanId());
        }

        OpenSpan span = spanBuilder.build();
        trace.push(span);
        return span;
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
     */
    public static Optional<Span> completeSpan() {
        return completeSpan(Collections.emptyMap());
    }

    /**
     * Like {@link #completeSpan()}, but adds {@code metadata} to the current span being completed.
     */
    public static Optional<Span> completeSpan(Map<String, String> metadata) {
        Trace trace = currentTrace.get();
        if (trace == null) {
            return Optional.empty();
        }
        Optional<Span> maybeSpan = popCurrentSpan()
                .map(openSpan -> toSpan(openSpan, metadata, trace.getTraceId()));

        // Notify subscribers iff trace is observable
        if (maybeSpan.isPresent() && trace.isObservable()) {
            notifyObservers(maybeSpan.get());
        }

        return maybeSpan;
    }

    private static void notifyObservers(Span span) {
        for (SpanObserver observer : observersList) {
            observer.consume(span);
        }
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
        observersList = ImmutableList.copyOf(observers.values());
    }

    /** Sets the sampler (for all threads). */
    public static void setSampler(TraceSampler sampler) {
        Tracer.sampler = sampler;
    }

    /** Returns the globally unique identifier for this thread's trace. */
    public static String getTraceId() {
        return Preconditions.checkNotNull(currentTrace.get(), "There is no root span").getTraceId();
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

        // Give SLF4J appenders access to the trace id
        MDC.put(Tracers.TRACE_ID_KEY, trace.getTraceId());
    }

    private static Trace getOrCreateCurrentTrace() {
        Trace trace = currentTrace.get();
        if (trace == null) {
            trace = createTrace(Optional.empty(), Tracers.randomId());
            setTrace(trace);
        }
        return trace;
    }

    private static void clearCurrentTrace() {
        currentTrace.remove();
        MDC.remove(Tracers.TRACE_ID_KEY);
    }
}
