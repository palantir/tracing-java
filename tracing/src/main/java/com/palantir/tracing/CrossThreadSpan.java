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

/** For low-level library code only. */
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
    // TODO don't construct an OpenSpan if we don't need it
    @CheckReturnValue
    public static CrossThreadSpan startSpan(String operation) {
        Trace trace = Tracer.hasTraceId()
                ? Tracer.getOrCreateCurrentTrace()
                : Tracer.createTrace(Observability.UNDECIDED, Tracers.randomId());

        Optional<String> parentSpanId = trace.top().map(OpenSpan::getSpanId);
        OpenSpan openSpan1 = OpenSpan.of(
                operation,
                Tracers.randomId(),
                SpanType.LOCAL,
                parentSpanId,
                trace.getOriginatingSpanId());

        return new CrossThreadSpan(trace.getTraceId(), trace.isObservable(), new AtomicReference<>(openSpan1));
    }

    /** Destructive operation - once you call this, you can't use it anymore. */
    public void terminate() {
        OpenSpan openSpan1 = openSpan.getAndSet(null);
        Preconditions.checkState(openSpan1 != null, "CrossThreadSpan may only be completed once");

        // end the span
        Span span = Tracer.toSpan(openSpan1, Collections.emptyMap(), traceId);

        Tracer.notifyObservers(span);
    }

    /** Destructive - the calling thread's threadlocal trace state won't be restored after this. */
    @MustBeClosed
    public CloseableTracer threadLocalSibling(String nextOperationName) {
        // TODO(dfox): gross - nowhere else lets you make siblings...
        terminate();
        Tracer.clearCurrentTrace();

        // a fresh copy doesn't have any of the old spans
        Trace rehydratedTrace = Trace.of(observable, traceId);
        Tracer.setTrace(rehydratedTrace);

        return CloseableTracer.startSpan(nextOperationName, SpanType.LOCAL);
    }

    @CheckReturnValue
    public CrossThreadSpan crossThreadChild(String operation) {
        OpenSpan openSpan1 = openSpan.get();
        Preconditions.checkState(openSpan1 != null, "Can't create crossThreadChild after terminate has been called");

        OpenSpan openSpan11 = OpenSpan.of(
                operation,
                Tracers.randomId(),
                SpanType.LOCAL,
                Optional.of(openSpan1.getSpanId()),
                openSpan1.getOriginatingSpanId());

        return new CrossThreadSpan(traceId, Tracer.isTraceObservable(), new AtomicReference<>(openSpan11));
    }

    /** This is also destructive because original trace state won't be restored afterwards. */
    @MustBeClosed
    public CloseableTracer threadLocalChild(String nextOperationName) {
        Tracer.clearCurrentTrace();

        OpenSpan openSpan1 = openSpan.get();
        Preconditions.checkState(openSpan1 != null, "Can't create threadLocalChild after terminate has been called");

        Trace rehydratedTrace = Trace.of(observable, traceId);
        // 🌶🌶🌶 someone could now call Tracer.fastCompleteSpan and emit this guy from multiple threads!!!
        rehydratedTrace.push(openSpan1);
        Tracer.setTrace(rehydratedTrace);

        return CloseableTracer.startSpan(nextOperationName, SpanType.LOCAL);
    }
}
