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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
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
    private static Trace createTrace(Observability observability, String traceId) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        boolean observable = shouldObserve(observability);
        return Trace.of(observable, traceId);
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

    static TraceMetadata getTraceMetadata() {
        Trace trace = checkNotNull(currentTrace.get(), "Unable to getTraceMetadata when there is trace in progress");

        if (isTraceObservable()) {
            OpenSpan openSpan = trace.top()
                    .orElseThrow(() -> new SafeRuntimeException("Trace with no spans in progress"));
            return TraceMetadata.builder()
                    .spanId(openSpan.getSpanId())
                    .parentSpanId(openSpan.getParentSpanId())
                    .originatingSpanId(trace.getOriginatingSpanId())
                    .traceId(trace.getTraceId())
                    .build();
        } else {
            // In the unsampled case, the Trace.Unsampled class doesn't actually store a spanId/parentSpanId
            // stack, so we just make one up (just in time). This matches the behaviour of Tracer#startSpan.

            // 🌶🌶🌶 this is a bit funky because calling getTraceMetadata multiple times will return different spanIds
            return TraceMetadata.builder()
                    .spanId(Tracers.randomId())
                    .parentSpanId(Optional.of(Tracers.randomId()))
                    .originatingSpanId(trace.getOriginatingSpanId())
                    .traceId(trace.getTraceId())
                    .build();
        }
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #initTrace(Observability, String)}
     */
    @Deprecated
    public static void initTrace(Optional<Boolean> isObservable, String traceId) {
        Observability observability = isObservable
                .map(observable -> observable ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE)
                .orElse(Observability.UNDECIDED);

        setTrace(createTrace(observability, traceId));
    }

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans.
     */
    public static void initTrace(Observability observability, String traceId) {
        setTrace(createTrace(observability, traceId));
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String, String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        return getOrCreateCurrentTrace().startSpan(operation, parentSpanId, type);
    }

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(String operation, SpanType type) {
        return getOrCreateCurrentTrace().startSpan(operation, type);
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(String operation, String parentSpanId, SpanType type) {
        getOrCreateCurrentTrace().fastStartSpan(operation, parentSpanId, type);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(String operation, SpanType type) {
        getOrCreateCurrentTrace().fastStartSpan(operation, type);
    }

    /**
     * Like {@link #startSpan(String)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(String operation) {
        fastStartSpan(operation, SpanType.LOCAL);
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not set or modify tracing thread state.
     * This is an internal utility that should not be called directly outside of {@link DetachedSpan}.
     */
    static DetachedSpan detachInternal(String operation, SpanType type) {
        Trace maybeCurrentTrace = currentTrace.get();
        String traceId = maybeCurrentTrace != null
                ? maybeCurrentTrace.getTraceId() : Tracers.randomId();
        boolean sampled = maybeCurrentTrace != null
                ? maybeCurrentTrace.isObservable() : sampler.sample();
        return sampled
                ? new SampledDetachedSpan(operation, type, traceId, getParentSpanId(maybeCurrentTrace))
                : new UnsampledDetachedSpan(traceId);
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

    private static final class SampledDetachedSpan implements DetachedSpan {

        private final AtomicBoolean completed = new AtomicBoolean();
        private final String traceId;
        private final OpenSpan openSpan;

        SampledDetachedSpan(
                String operation, SpanType type, String traceId, Optional<String> parentSpanId) {
            this.traceId = traceId;
            this.openSpan = OpenSpan.builder()
                    .parentSpanId(parentSpanId)
                    .spanId(Tracers.randomId())
                    .operation(operation)
                    .type(type)
                    .build();
        }

        @Override
        @MustBeClosed
        public CloseableSpan childSpan(String operationName, SpanType type) {
            warnIfCompleted("startSpanOnCurrentThread");
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(true, traceId));
            Tracer.fastStartSpan(operationName, openSpan.getSpanId(), type);
            return TraceRestoringCloseableSpan.of(maybeCurrentTrace);
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            warnIfCompleted("startDetachedSpan");
            return new SampledDetachedSpan(operation, type, traceId, Optional.of(openSpan.getSpanId()));
        }

        @Override
        public void complete() {
            if (completed.compareAndSet(false, true)) {
                Tracer.notifyObservers(toSpan(openSpan, Collections.emptyMap(), traceId));
            }
        }

        @Override
        public String toString() {
            return "SampledDetachedSpan{completed=" + completed
                    + ", traceId='" + traceId + '\'' + ", openSpan=" + openSpan + '}';
        }

        private void warnIfCompleted(String feature) {
            if (completed.get()) {
                log.warn("{} called after span {} completed",
                        SafeArg.of("feature", feature),
                        SafeArg.of("detachedSpan", this));
            }
        }
    }

    private static final class UnsampledDetachedSpan implements DetachedSpan {

        private final String traceId;

        UnsampledDetachedSpan(String traceId) {
            this.traceId = traceId;
        }

        @Override
        public CloseableSpan childSpan(String operationName, SpanType type) {
            Trace maybeCurrentTrace = currentTrace.get();
            setTrace(Trace.of(false, traceId));
            Tracer.fastStartSpan(operationName, type);
            return TraceRestoringCloseableSpan.of(maybeCurrentTrace);
        }

        @Override
        public DetachedSpan childDetachedSpan(String operation, SpanType type) {
            return this;
        }

        @Override
        public void complete() {
            // nop
        }

        @Override
        public String toString() {
            return "UnsampledDetachedSpan{traceId='" + traceId + "'}";
        }
    }

    private static final class TraceRestoringCloseableSpan implements CloseableSpan {

        // Complete the current span.
        private static final CloseableSpan DEFAULT_TOKEN = Tracer::fastCompleteSpan;

        private final Trace original;

        static CloseableSpan of(@Nullable Trace original) {
            return original == null ? DEFAULT_TOKEN : new TraceRestoringCloseableSpan(original);
        }

        TraceRestoringCloseableSpan(Trace original) {
            this.original = Preconditions.checkNotNull(original, "Expected an original trace instance");
        }

        @Override
        public void close() {
            DEFAULT_TOKEN.close();
            Tracer.setTrace(original);
        }
    }

    /** Discards the current span without emitting it. */
    static void fastDiscardSpan() {
        Trace trace = currentTrace.get();
        checkNotNull(trace, "Expected current trace to exist");
        checkState(!trace.isEmpty(), "Expected span to exist before discarding");
        popCurrentSpan(trace);
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
            Optional<OpenSpan> span = popCurrentSpan(trace);
            if (trace.isObservable()) {
                completeSpanAndNotifyObservers(span, metadata, trace.getTraceId());
            }
        }
    }

    private static void completeSpanAndNotifyObservers(
            Optional<OpenSpan> openSpan, Map<String, String> metadata, String traceId) {
        if (openSpan.isPresent()) {
            Tracer.notifyObservers(toSpan(openSpan.get(), metadata, traceId));
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
        Optional<Span> maybeSpan = popCurrentSpan(trace)
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

    private static Optional<OpenSpan> popCurrentSpan(Trace trace) {
        Optional<OpenSpan> span = trace.pop();
        if (trace.isEmpty()) {
            clearCurrentTrace();
        }
        return span;
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
            trace = createTrace(Observability.UNDECIDED, Tracers.randomId());
            setTrace(trace);
        }
        return trace;
    }

    @VisibleForTesting
    static void clearCurrentTrace() {
        currentTrace.remove();
        MDC.remove(Tracers.TRACE_ID_KEY);
        MDC.remove(Tracers.TRACE_SAMPLED_KEY);
    }
}
