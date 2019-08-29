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
import java.util.Map;
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

    /**
     * Complete the span. This method does not change any thread-local state, so successive operations on this
     * thread may have different traceIds. If you want to finish the span and continue work related to
     * this traceId use {@link #sibling}, or if you want to continue work related to this traceId without completing
     * the span, use {@link #child}.
     */
    public void terminate() {
        OpenSpan inProgress = consumeOpenSpan("CrossThreadSpan may only be terminated once");

        // end the OpenSpan
        Map<String, String> metadata = Collections.emptyMap();
        Span span = Tracer.toSpan(inProgress, metadata, traceId);

        Tracer.notifyObservers(span);
    }

    private OpenSpan consumeOpenSpan(String message) {
        OpenSpan inProgress = openSpan.getAndSet(null);
        Preconditions.checkState(inProgress != null, message);
        return inProgress;
    }

    private OpenSpan accessOpenSpan(String message) {
        OpenSpan inProgress = openSpan.get();
        Preconditions.checkState(inProgress != null, message);
        return inProgress;
    }

    /**
     * Finish the cross-thread span and start a new (thread local span) with the given operation name.
     * NOTE: this is destructive, because any existing threadlocal tracing state is thrown away.
     */
    @MustBeClosed
    public CloseableTracer sibling(String nextOperationName) {
        OpenSpan inProgress = consumeOpenSpan("Can't call sibling if span has already been closed");

        Map<String, String> metadata = Collections.emptyMap();
        Span span = Tracer.toSpan(inProgress, metadata, traceId);
        Tracer.notifyObservers(span);

        // we throw away the existing thread local state and replace it with our view of the world
        Tracer.clearCurrentTrace();
        Tracer.setTrace(Trace.of(observable, traceId));

        return CloseableTracer.startSpan(nextOperationName, inProgress.getParentSpanId(), SpanType.LOCAL);
    }

    /**
     * Start a new span parented to this current in-progress CrossThreadSpan.
     * NOTE: this is destructive, because any existing threadlocal tracing state is thrown away.
     */
    @MustBeClosed
    public CloseableTracer child(String nextOperationName) {
        Tracer.clearCurrentTrace();

        OpenSpan inProgress = accessOpenSpan("Can't create child after terminate has been called");

        Tracer.setTrace(Trace.of(observable, traceId));

        return CloseableTracer.startSpan(nextOperationName, Optional.of(inProgress.getSpanId()), SpanType.LOCAL);
    }

    @CheckReturnValue
    public CrossThreadSpan crossThreadSpan(String operation) {
        OpenSpan parent = openSpan.get();
        Preconditions.checkState(parent != null, "Can't create crossThreadSpan after terminate has been called");

        OpenSpan child = OpenSpan.of(
                operation,
                Tracers.randomId(),
                SpanType.LOCAL,
                Optional.of(parent.getSpanId()),
                parent.getOriginatingSpanId());

        return new CrossThreadSpan(traceId, Tracer.isTraceObservable(), new AtomicReference<>(child));
    }
}
