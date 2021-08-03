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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CheckReturnValue;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import io.opentelemetry.api.trace.SpanContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.MDC;

/**
 * The singleton entry point for handling Zipkin-style traces and spans. Provides functionality for starting and
 * completing spans, and for subscribing observers to span completion events.
 *
 * <p>This class is thread-safe.
 */
public final class Tracer {

    private static final SafeLogger log = SafeLoggerFactory.get(Tracer.class);

    // Only access in a class-synchronized fashion
    private static final Map<String, SpanObserver> observers = new HashMap<>();
    // we want iterating through tracers to be very fast, and it's faster to pre-define observer execution
    // when our observers are modified.
    private static volatile Consumer<Span> compositeObserver = _span -> {};
    // Thread-safe since stateless
    private static volatile TraceSampler sampler = RandomSampler.create(0.0005f);

    private Tracer() {}

    public static Optional<TraceMetadata> maybeGetTraceMetadata() {
        // OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
        // openTelemetry.getTracer("palantir-tracing-java");
        io.opentelemetry.api.trace.Span currentSpan = io.opentelemetry.api.trace.Span.current();
        if (currentSpan == io.opentelemetry.api.trace.Span.getInvalid()) {
            return Optional.empty();
        }

        SpanContext spanContext = currentSpan.getSpanContext();
        return Optional.of(TraceMetadata.builder()
                .traceId(spanContext.getTraceId())
                // b3 takes the odd decision to propagate parentSpanId, but OpenTelemetry doesn't.
                // In theory, perhaps this could result in a span starting on one node, making an RPC and ending on
                // another node. In practise, conjure undertow never reads this X-B3-ParentSpanId header and we just
                // create a new span on the server side, parented to the value of X-B3-SpanId.
                // https://github.com/openzipkin/b3-propagation/blob/master/RATIONALE.md
                // https://github.com/openzipkin/b3-propagation#why-is-parentspanid-propagated.
                .parentSpanId(Optional.empty())
                .spanId(spanContext.getSpanId())
                .requestId(Optional.empty()) // TODO(dfox): wire this through
                .originatingSpanId(Optional.empty()) // TODO(dfox): wire this through somehows
                .build());
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, String, SpanType)}
     */
    @Deprecated
    public static void initTrace(Optional<Boolean> isObservable, String traceId) {}

    /**
     * Initializes the current thread's trace, erasing any previously accrued open spans.
     *
     * @deprecated Use {@link #initTraceWithSpan(Observability, String, String, SpanType)}
     */
    @Deprecated
    public static void initTrace(Observability observability, String traceId) {}

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability, String traceId, @Safe String operation, String parentSpanId, SpanType type) {}

    /**
     * Initializes the current thread's trace with a root span, erasing any previously accrued open spans.
     * The root span must eventually be completed using {@link #fastCompleteSpan()} or {@link #completeSpan()}.
     */
    public static void initTraceWithSpan(
            Observability observability, String traceId, @Safe String operation, SpanType type) {}

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty. If the return value is not used, prefer {@link Tracer#fastStartSpan(String,
     * String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation, String parentSpanId, SpanType type) {}

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}. If the
     * return value is not used, prefer {@link Tracer#fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation, SpanType type) {}

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation) {}

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, String parentSpanId, SpanType type) {}

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, SpanType type) {}

    /**
     * Like {@link #startSpan(String)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation) {}

    /**
     * Completes the current span (if it exists) and notifies all {@link #observers subscribers} about the completed
     * span.
     *
     * <p>Does not construct the Span object if no subscriber will see it.
     */
    public static void fastCompleteSpan() {}

    /**
     * Like {@link #fastCompleteSpan()}, but adds {@code metadata} to the current span being completed.
     */
    public static void fastCompleteSpan(@Safe Map<String, String> metadata) {}

    public static <T> void fastCompleteSpan(TagTranslator<? super T> tag, T state) {}

    /**
     * Completes and returns the current span (if it exists) and notifies all {@link #observers subscribers} about the
     * completed span. If the return value is not used, prefer {@link Tracer#fastCompleteSpan()}.
     */
    @CheckReturnValue
    public static Optional<Span> completeSpan() {}

    /**
     * Like {@link #completeSpan()}, but adds {@code metadata} to the current span being completed.
     * If the return value is not used, prefer {@link Tracer#fastCompleteSpan(Map)}.
     *
     * @deprecated Use {@link #fastCompleteSpan()}
     */
    @CheckReturnValue
    @Deprecated
    public static Optional<Span> completeSpan(@Safe Map<String, String> metadata) {}

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
    public static String getTraceId() {}

    /**
     * Clears the current trace id and returns it if present.
     */
    static Optional<Trace> getAndClearTraceIfPresent() {}

    /**
     * Clears the current trace id and returns (a copy of) it.
     */
    public static Trace getAndClearTrace() {}

    /**
     * True iff the spans of this thread's trace are to be observed by {@link SpanObserver span obververs} upon
     * {@link Tracer#completeSpan span completion}.
     */
    public static boolean isTraceObservable() {}

    /**
     * Returns true if there is an active trace which is not observable. This is equivalent to the result of
     * {@code Tracer.hasTraceId() && !Tracer.isTraceObservable()}.
     * This check is used frequently in hot paths to avoid unnecessary overhead in unsampled traces.
     */
    public static boolean hasUnobservableTrace() {}

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
