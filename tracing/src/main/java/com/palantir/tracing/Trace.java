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

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 */
public final class Trace {

    private final Deque<OpenSpan> stack;
    private final boolean isObservable;
    private final String traceId;

    private Trace(Deque<OpenSpan> stack, boolean isObservable, String traceId) {
        checkArgument(!traceId.isEmpty(), "traceId must be non-empty");

        this.stack = stack;
        this.isObservable = isObservable;
        this.traceId = traceId;
    }

    static Trace create(boolean isObservable, String traceId) {
        Deque<OpenSpan> deque = isObservable ? new ArrayDeque<>() : ImmutableEmptyDeque.instance();
        return new Trace(deque, isObservable, traceId);
    }

    void push(OpenSpan span) {
        if (isObservable) {
            stack.push(span);
        }
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
        Deque<OpenSpan> deque = isObservable ? new ArrayDeque<>(stack) : ImmutableEmptyDeque.instance();
        return new Trace(deque, isObservable, traceId);
    }

    @Override
    public String toString() {
        return "Trace{stack=" + stack + ", isObservable=" + isObservable + ", traceId='" + traceId + "'}";
    }
}
