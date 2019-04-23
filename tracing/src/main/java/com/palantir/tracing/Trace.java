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

import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanObserver;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 */
public final class Trace {

    private final Deque<OpenSpan> stack;
    private final boolean isObservable;
    private final String traceId;
    @Nullable
    private final String parentSpanId;

    private Trace(ArrayDeque<OpenSpan> stack, boolean isObservable, String traceId, String parentSpanId) {
        checkArgument(!traceId.isEmpty(), "traceId must be non-empty");

        this.stack = stack;
        this.isObservable = isObservable;
        this.traceId = traceId;
        this.parentSpanId = parentSpanId;
    }

    Trace(boolean isObservable, String traceId, String parentSpanId) {
        this(new ArrayDeque<>(), isObservable, traceId, parentSpanId);
    }

    Trace(boolean isObservable, String traceId) {
        this(isObservable, traceId, null);
    }

    void push(OpenSpan span) {
        stack.push(span);
    }

    Optional<String> topSpanId() {
        return stack.isEmpty()
                ? Optional.ofNullable(parentSpanId)
                : Optional.of(stack.peekFirst().getSpanId());
    }

    Optional<OpenSpan> top() {
        return stack.isEmpty() ? Optional.empty() : Optional.of(stack.peekFirst());
    }

    Optional<OpenSpan> pop() {
        return stack.isEmpty() ? Optional.empty() : Optional.of(stack.pop());
    }

    boolean isEmpty() {
        return stack.isEmpty();
    }

    /**
     * True iff the spans of this trace are to be observed by {@link SpanObserver span obververs} upon {@link
     * Tracer#completeSpan span completion}.
     */
    boolean isObservable() {
        return isObservable;
    }

    /**
     * The globally unique non-empty identifier for this call trace.
     */
    String getTraceId() {
        return traceId;
    }

    /** Returns a copy of this Trace which can be independently mutated. */
    Trace deepCopy() {
        return new Trace(new ArrayDeque<>(stack), isObservable, traceId, parentSpanId);
    }

    @Override
    public String toString() {
        return "Trace{stack=" + stack + ", isObservable=" + isObservable + ", traceId='" + traceId + "'}";
    }
}
