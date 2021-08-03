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

import com.google.errorprone.annotations.CheckReturnValue;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.immutables.value.Value;
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
    private static volatile TraceSampler palantirSampler = RandomSampler.create(0.0005f);

    // TODO(dfox): delete this and just ban the fastStartSpan/fastCompleteSpan?
    private static final ThreadLocal<Deque<Thingy>> threadLocalScopes = ThreadLocal.withInitial(ArrayDeque::new);

    @Value.Immutable
    interface Thingy {
        io.opentelemetry.api.trace.Span span();

        Scope scope();
    }

    static final class MdcContextStorage implements ContextStorage {
        private final ContextStorage delegate;

        MdcContextStorage(ContextStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Scope attach(Context toAttach) {

            // we save these values to ensure we can restore MDCs afterwards
            String mdcTraceIdBefore = MDC.get(Tracers.TRACE_ID_KEY);
            String mdcSampledBefore = MDC.get(Tracers.TRACE_SAMPLED_KEY);

            copySpanInfoToMdc(io.opentelemetry.api.trace.Span.fromContextOrNull(toAttach));

            // in practise, we expect the delegate here to always be a ThreadLocalContextStorage, so the effect of
            // calling this 'attach' is that the context is now accessible using static methods until scope.close is
            // called.
            Scope threadLocalResetScope = delegate.attach(toAttach);
            return new Scope() {
                @Override
                public void close() {
                    threadLocalResetScope.close();
                    updateMdc(Tracers.TRACE_ID_KEY, mdcTraceIdBefore);
                    updateMdc(Tracers.TRACE_SAMPLED_KEY, mdcSampledBefore);
                }
            };
        }

        @Nullable
        @Override
        public Context current() {
            return delegate.current();
        }

        private static void copySpanInfoToMdc(@Nullable io.opentelemetry.api.trace.Span span) {
            if (span == null) {
                MDC.remove(Tracers.TRACE_ID_KEY);
                MDC.remove(Tracers.TRACE_SAMPLED_KEY);
                return;
            }
            SpanContext spanContext = span.getSpanContext();

            MDC.put(Tracers.TRACE_ID_KEY, spanContext.getTraceId());
            updateMdc(Tracers.TRACE_SAMPLED_KEY, spanContext.isSampled() ? "1" : null);
        }

        private static void updateMdc(String key, String value) {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        }
    }

    static io.opentelemetry.api.trace.Tracer getSpanBuilder() {

        // TODO(dfox): need to initialize this _way_ earlier.
        ContextStorage.addWrapper(MdcContextStorage::new);

        // TODO(dfox): should we get this from an OpenTelemetry instance?? perhaps GlobalOpenTelemetry?
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(new SpanProcessor() {
                    @Override
                    public void onStart(Context _parentContext, ReadWriteSpan _span) {}

                    @Override
                    public boolean isStartRequired() {
                        return false;
                    }

                    @Override
                    public void onEnd(ReadableSpan span) {
                        compositeObserver.accept(Translation.fromOpenTelemetry(span));
                    }

                    @Override
                    public boolean isEndRequired() {
                        return true;
                    }
                })
                .setSampler(new Sampler() {
                    @Override
                    public SamplingResult shouldSample(
                            Context _parentContext,
                            String _traceId,
                            String _name,
                            SpanKind _spanKind,
                            Attributes _attributes,
                            List<LinkData> _parentLinks) {
                        return palantirSampler.sample()
                                ? SamplingResult.create(SamplingDecision.RECORD_AND_SAMPLE)
                                : SamplingResult.create(SamplingDecision.DROP);
                    }

                    @Override
                    public String getDescription() {
                        return "palantir-tracing-java-sampler";
                    }

                    @Override
                    public String toString() {
                        return getDescription();
                    }
                })
                .build();
        return tracerProvider.get("palantir-tracing-java");
    }

    private Tracer() {}

    public static Optional<TraceMetadata> maybeGetTraceMetadata() {
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
    public static OpenSpan startSpan(@Safe String operation, String parentSpanId, SpanType type) {
        throw new UnsupportedOperationException();
    }

    /**
     * Like {@link #startSpan(String)}, but opens a span of the explicitly given {@link SpanType span type}. If the
     * return value is not used, prefer {@link Tracer#fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation, SpanType type) {
        throw new UnsupportedOperationException();
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link Tracer#fastStartSpan(String)}}.
     */
    @CheckReturnValue
    public static OpenSpan startSpan(@Safe String operation) {
        throw new UnsupportedOperationException();
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, String parentSpanId, SpanType type) {
        throw new UnsupportedOperationException();
    }

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation, SpanType type) {
        io.opentelemetry.api.trace.Span span = getSpanBuilder()
                .spanBuilder(operation)
                .setSpanKind(Translation.toOpenTelemetry(type))
                .startSpan();

        Scope scope = span.makeCurrent();

        // this is gross... can we just do without???
        threadLocalScopes
                .get()
                .push(ImmutableThingy.builder().span(span).scope(scope).build());
    }

    /**
     * Like {@link #startSpan(String)}, but does not return an {@link OpenSpan}.
     */
    public static void fastStartSpan(@Safe String operation) {
        fastStartSpan(operation, SpanType.LOCAL);
    }

    /**
     * Completes the current span (if it exists) and notifies all {@link #observers subscribers} about the completed
     * span.
     *
     * <p>Does not construct the Span object if no subscriber will see it.
     */
    public static void fastCompleteSpan() {
        Thingy thingy = threadLocalScopes.get().pollFirst();
        if (thingy == null) {
            return;
        }
        thingy.scope().close();
        thingy.span().end();
    }

    /**
     * Like {@link #fastCompleteSpan()}, but adds {@code metadata} to the current span being completed.
     */
    public static void fastCompleteSpan(@Safe Map<String, String> metadata) {
        Thingy thingy = threadLocalScopes.get().pollFirst();
        if (thingy == null) {
            return;
        }
        metadata.forEach(thingy.span()::setAttribute);
        thingy.scope().close();
        thingy.span().end();
    }

    public static <T> void fastCompleteSpan(TagTranslator<? super T> tagTranslator, T data) {
        Thingy thingy = threadLocalScopes.get().pollFirst();
        if (thingy == null) {
            return;
        }

        setSpanAttributes(thingy.span(), tagTranslator, data);

        thingy.scope().close();
        thingy.span().end();
    }

    static <T> void setSpanAttributes(
            io.opentelemetry.api.trace.Span span, TagTranslator<? super T> tagTranslator, T data) {
        if (!tagTranslator.isEmpty(data) && span.isRecording()) {
            tagTranslator.translate(OpenTelemetryTagAdapter.INSTANCE, span, data);
        }
    }

    private enum OpenTelemetryTagAdapter implements TagTranslator.TagAdapter<io.opentelemetry.api.trace.Span> {
        INSTANCE;

        @Override
        public void tag(io.opentelemetry.api.trace.Span target, String key, String value) {
            target.setAttribute(key, value);
        }

        @Override
        public void tag(io.opentelemetry.api.trace.Span target, Map<String, String> tags) {
            tags.forEach(target::setAttribute);
        }
    }

    /**
     * Completes and returns the current span (if it exists) and notifies all {@link #observers subscribers} about the
     * completed span. If the return value is not used, prefer {@link Tracer#fastCompleteSpan()}.
     */
    @CheckReturnValue
    public static Optional<Span> completeSpan() {
        throw new UnsupportedOperationException(
                "OpenTelemetry does not make it easy to directly access the resultant span after completion");
    }

    /**
     * Like {@link #completeSpan()}, but adds {@code metadata} to the current span being completed.
     * If the return value is not used, prefer {@link Tracer#fastCompleteSpan(Map)}.
     *
     * @deprecated Use {@link #fastCompleteSpan()}
     */
    @CheckReturnValue
    @Deprecated
    public static Optional<Span> completeSpan(@Safe Map<String, String> _metadata) {
        throw new UnsupportedOperationException(
                "OpenTelemetry does not make it easy to directly access the resultant span after completion");
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
        palantirSampler = sampler;
    }

    /**
     * Returns true if there is an active trace on this thread.
     */
    public static boolean hasTraceId() {
        return io.opentelemetry.api.trace.Span.fromContextOrNull(Context.current()) != null;
    }

    /**
     * Returns the globally unique identifier for this thread's trace.
     */
    public static String getTraceId() {
        return io.opentelemetry.api.trace.Span.current().getSpanContext().getTraceId();
    }

    /**
     * Clears the current trace id and returns (a copy of) it.
     */
    public static Trace getAndClearTrace() {
        throw new UnsupportedOperationException("Only a couple of users of this - don't think we want to support it");
    }

    /**
     * True iff the spans of this thread's trace are to be observed by {@link SpanObserver span obververs} upon
     * {@link Tracer#completeSpan span completion}.
     */
    public static boolean isTraceObservable() {
        return io.opentelemetry.api.trace.Span.current().getSpanContext().isSampled();
    }

    /**
     * Returns true if there is an active trace which is not observable. This is equivalent to the result of
     * {@code Tracer.hasTraceId() && !Tracer.isTraceObservable()}.
     * This check is used frequently in hot paths to avoid unnecessary overhead in unsampled traces.
     */
    public static boolean hasUnobservableTrace() {
        io.opentelemetry.api.trace.Span maybeSpan =
                io.opentelemetry.api.trace.Span.fromContextOrNull(Context.current());
        return maybeSpan != null && !maybeSpan.getSpanContext().isSampled();
    }
}
