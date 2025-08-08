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

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 */
public final class Trace {

    @Nullable
    private TraceState defaultTraceState;

    private final List<SpanStackEntry> stack = new ArrayList<>();

    Trace() {}

    void push(SpanStackEntry span) {
        TraceState oldTraceState = traceState();

        stack.add(span);

        updateMdc(oldTraceState);
    }

    @Nullable
    EnabledSpan pop() {
        if (stack.isEmpty()) {
            return null;
        }

        TraceState oldTraceState = traceState();

        SpanStackEntry entry = stack.remove(stack.size() - 1);

        resetIfEmpty();
        updateMdc(oldTraceState);

        if (entry instanceof EnabledSpan span) {
            return span;
        }

        return null;
    }

    @Nullable
    EnabledSpan remove(String spanId) {
        for (int i = stack.size() - 1; i >= 0; i--) {
            SpanStackEntry entry = stack.get(i);
            if (entry instanceof EnabledSpan span && span.spanId().equals(spanId)) {
                TraceState oldTraceState = traceState();

                stack.remove(i);

                resetIfEmpty();
                updateMdc(oldTraceState);

                return span;
            }
        }

        return null;
    }

    boolean remove(SpanStackEntry entry) {
        for (int i = stack.size() - 1; i >= 0; i--) {
            if (stack.get(i) == entry) {
                TraceState oldTraceState = traceState();

                stack.remove(i);

                resetIfEmpty();
                updateMdc(oldTraceState);

                return true;
            }
        }

        return false;
    }

    void reset(@Nullable TraceState traceState) {
        TraceState oldTraceState = traceState();

        defaultTraceState = traceState;
        stack.clear();

        updateMdc(oldTraceState);
    }

    // TODO(pkoenig): Do we memoize this?
    @Nullable
    EnabledSpan current() {
        for (int i = stack.size() - 1; i >= 0; i--) {
            SpanStackEntry entry = stack.get(i);
            if (entry instanceof EnabledSpan span) {
                return span;
            }
        }

        return null;
    }

    boolean isEmpty() {
        return stack.isEmpty();
    }

    @Nullable
    TraceState traceState() {
        if (stack.isEmpty()) {
            return defaultTraceState;
        } else {
            return stack.get(stack.size() - 1).traceState();
        }
    }

    private void resetIfEmpty() {
        if (stack.isEmpty()) {
            defaultTraceState = null;
        }
    }

    private void updateMdc(@Nullable TraceState oldTraceState) {
        TraceState newTraceState = traceState();
        if (oldTraceState != newTraceState) {
            if (newTraceState != null) {
                MDC.put(Tracers.TRACE_ID_KEY, newTraceState.traceId());

                if (newTraceState.isObservable()) {
                    MDC.put(Tracers.TRACE_SAMPLED_KEY, "1");
                } else {
                    MDC.remove(Tracers.TRACE_SAMPLED_KEY);
                }

                String requestId = newTraceState.requestId();
                if (requestId == null) {
                    MDC.remove(Tracers.REQUEST_ID_KEY);
                } else {
                    MDC.put(Tracers.REQUEST_ID_KEY, requestId);
                }
            } else {
                MDC.remove(Tracers.TRACE_ID_KEY);
                MDC.remove(Tracers.TRACE_SAMPLED_KEY);
                MDC.remove(Tracers.REQUEST_ID_KEY);
            }
        }
    }

    // TODO(pkoenig): Remove
    static Trace of(TraceState _traceState) {
        return new Trace();
    }
}
