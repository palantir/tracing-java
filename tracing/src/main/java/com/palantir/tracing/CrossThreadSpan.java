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

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;

/**
 * For low-level library code only.
 */
@ThreadSafe
public final class CrossThreadSpan {

    private final String traceId;
    private boolean observable;
    private final AtomicReference<OpenSpan> openSpan;

    private CrossThreadSpan(String traceId, boolean observable, AtomicReference<OpenSpan> openSpan) {
        this.traceId = traceId;
        this.observable = observable;
        this.openSpan = openSpan;
    }

    // TODO(dfox): move these static factories to Tracers.java ??
    // intentionally not using AutoCloseable and '@MustBeClosed' because try-with-resources isn't convenient across
    // threads
    @CheckReturnValue
    public static CrossThreadSpan startSpan(String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    @CheckReturnValue
    public static CrossThreadSpan startSpan(String operation, SpanType spanType) {

        Trace trace = Tracer.getOrCreateCurrentTrace();
        // Trace trace = Tracer.hasTraceId()
        //         ? Tracer.getOrCreateCurrentTrace()
        //         : Tracer.createTrace(Observability.UNDECIDED, Tracers.randomId());

        // TODO don't make this if we don't need it
        Optional<String> parentSpanId = trace.top().map(OpenSpan::getSpanId);
        OpenSpan openSpan = OpenSpan.of(
                operation,
                Tracers.randomId(),
                spanType,
                parentSpanId,
                trace.getOriginatingSpanId());

        return new CrossThreadSpan(trace.getTraceId(), trace.isObservable(), new AtomicReference<>(openSpan));
    }

    /** Destructive operation - once you call this, you can't use it anymore. */
    public void complete() {
        OpenSpan openSpan1 = openSpan.getAndSet(null);
        Preconditions.checkState(openSpan1 != null, "CrossThreadSpan may only be completed once");

        // end the span
        Span span = Tracer.toSpan(openSpan1, Collections.emptyMap(), traceId);

        Tracer.notifyObservers(span);
    }

    /** Destructive operation - once you call this, you can't use it anymore. */
    @MustBeClosed
    public CloseableTracer completeAndStartSpan(String nextOperationName, SpanType spanType) {
        complete();
        return threadLocalSibling(nextOperationName, spanType);
    }

    @MustBeClosed
    public CloseableTracer threadLocalSibling(String nextOperationName, SpanType spanType) {
        Tracer.clearCurrentTrace();

        // a fresh copy doesn't have any of the old spans
        Trace rehydratedTrace = Trace.of(observable, traceId);
        Tracer.setTrace(rehydratedTrace);

        return CloseableTracer.startSpan(nextOperationName, spanType);
    }

    @MustBeClosed
    public CloseableTracer threadLocalChild(String nextOperationName, SpanType spanType) {
        Tracer.clearCurrentTrace();

        Trace rehydratedTrace = Trace.of(observable, traceId);
        // TODO(dfox): this is broken - we threadLocalSpans on a new threaed are not connected to the parent spanid
        OpenSpan openSpan1 = openSpan.get();
        Preconditions.checkState(openSpan1 != null, "Can't create threadLocalChild after complete has been called");
        rehydratedTrace.push(openSpan1); // 🌶🌶🌶 someone could now call Tracer.fastCompleteSpan and emit this guy from
        // multiple threads!!!
        Tracer.setTrace(rehydratedTrace);

        return CloseableTracer.startSpan(nextOperationName, spanType);
    }

    @CheckReturnValue
    public CrossThreadSpan crossThreadChild(String operation, SpanType spanType) {
        OpenSpan openSpan1 = openSpan.get();
        Preconditions.checkState(openSpan1 != null, "Can't create crossThreadChild after complete has been called");

        OpenSpan openSpan = OpenSpan.of(
                operation,
                Tracers.randomId(),
                spanType,
                Optional.of(openSpan1.getSpanId()),
                openSpan1.getOriginatingSpanId());

        return new CrossThreadSpan(traceId, Tracer.isTraceObservable(), new AtomicReference<>(openSpan));
    }

    @MustBeClosed
    public CloseableTracer completeAndCrossThread(String nextOperationName) {
        return completeAndStartSpan(nextOperationName, SpanType.LOCAL);
    }

    @CheckReturnValue
    public CrossThreadSpan crossThreadChild(String operation) {
        return crossThreadChild(operation, SpanType.LOCAL);
    }

    @MustBeClosed
    public CloseableTracer threadLocalChild(String nextOperationName) {
        return threadLocalChild(nextOperationName, SpanType.LOCAL);
    }
}
