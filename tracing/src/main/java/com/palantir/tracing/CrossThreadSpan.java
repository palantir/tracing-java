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

    // intentionally not using AutoCloseable and '@MustBeClosed' because try-with-resources isn't convenient across
    // threads
    @CheckReturnValue
    public static CrossThreadSpan startSpan(String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    @CheckReturnValue
    public static CrossThreadSpan startSpan(String operation, SpanType spanType) {
        // copy our current state is
        String traceId = "TODO";
        Optional<String> parentSpanId = Optional.of("TODO");
        Optional<String> originatingSpanId = Optional.of("TODO");

        OpenSpan openSpan = OpenSpan.of(operation, Tracers.randomId(), spanType, parentSpanId, originatingSpanId);

        return new CrossThreadSpan(traceId, Tracer.isTraceObservable(), new AtomicReference<>(openSpan));
    }

    /** Destructive operation - once you call this, you can't use it anymore. */
    @MustBeClosed
    public CloseableTracer completeAndStartSpan(String nextOperationName) {
        complete();

        // nuke the current thread locals - we may or may not be throwing away an existing trace with an
        // unclosed OpenSpan.
        Tracer.clearCurrentTrace();

        // a fresh copy doesn't have any of the old spans
        Trace rehydratedTrace = Trace.of(true, traceId);
        Tracer.setTrace(rehydratedTrace);

        return CloseableTracer.startSpan(nextOperationName);
    }

    /** Destructive operation - once you call this, you can't use it anymore. */
    public void complete() {
        OpenSpan openSpan1 = openSpan.getAndSet(null);
        Preconditions.checkState(openSpan1 != null, "CrossThreadSpan may only be completed once");

        // end the span
        Span span = Tracer.toSpan(openSpan1, Collections.emptyMap(), traceId);

        // emit stuff based on the portable thread state
        Tracer.notifyObservers(span);
    }

    @CheckReturnValue
    public CrossThreadSpan startChildSpan(String operation, SpanType spanType) {
        // copy our current state is

        OpenSpan openSpan1 = openSpan.get();
        Preconditions.checkState(openSpan1 != null, "Can't create startChildSpan after complete has been called");

        OpenSpan openSpan = OpenSpan.of(
                operation, Tracers.randomId(), spanType,
                openSpan1.getParentSpanId(), openSpan1.getOriginatingSpanId());

        return new CrossThreadSpan(traceId, Tracer.isTraceObservable(), new AtomicReference<>(openSpan));
    }
}
