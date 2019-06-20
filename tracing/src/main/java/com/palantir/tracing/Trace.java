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
import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.base.Strings;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 */
public abstract class Trace {

    private final String traceId;

    private Trace(String traceId) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        this.traceId = traceId;
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only allowed
     * when the current trace is empty.
     */
    final OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        checkState(isEmpty(), "Cannot start a span with explicit parent if the current thread's trace is non-empty");
        checkArgument(!Strings.isNullOrEmpty(parentSpanId), "parentSpanId must be non-empty");
        OpenSpan span = OpenSpan.of(operation, Tracers.randomId(), type, Optional.of(parentSpanId));
        push(span);
        return span;
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link #fastStartSpan(String, SpanType)}}.
     */
    final OpenSpan startSpan(String operation, SpanType type) {
        Optional<OpenSpan> prevState = top();
        final OpenSpan span;
        // Avoid lambda allocation in hot paths
        if (prevState.isPresent()) {
            span = OpenSpan.of(operation, Tracers.randomId(), type, Optional.of(prevState.get().getSpanId()));
        } else {
            span = OpenSpan.of(operation, Tracers.randomId(), type, Optional.empty());
        }

        push(span);
        return span;
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    abstract void fastStartSpan(String operation, String parentSpanId, SpanType type);

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    abstract void fastStartSpan(String operation, SpanType type);

    abstract void push(OpenSpan span);

    abstract Optional<OpenSpan> top();

    abstract Optional<OpenSpan> pop();

    abstract boolean isEmpty();

    /**
     * True iff the spans of this trace are to be observed by {@link SpanObserver span obververs} upon {@link
     * Tracer#completeSpan span completion}.
     */
    abstract boolean isObservable();

    /**
     * The globally unique non-empty identifier for this call trace.
     */
    final String getTraceId() {
        return traceId;
    }

    /** Returns a copy of this Trace which can be independently mutated. */
    abstract Trace deepCopy();

    static Trace of(boolean isObservable, String traceId) {
        return isObservable ? new Sampled(traceId) : new Unsampled(traceId);
    }

    private static final class Sampled extends Trace {

        private final Deque<OpenSpan> stack;

        private Sampled(ArrayDeque<OpenSpan> stack, String traceId) {
            super(traceId);
            this.stack = stack;
        }

        private Sampled(String traceId) {
            this(new ArrayDeque<>(), traceId);
        }

        @Override
        void fastStartSpan(String operation, String parentSpanId, SpanType type) {
            startSpan(operation, parentSpanId, type);
        }

        @Override
        void fastStartSpan(String operation, SpanType type) {
            startSpan(operation, type);
        }

        @Override
        void push(OpenSpan span) {
            stack.push(span);
        }

        @Override
        Optional<OpenSpan> top() {
            return stack.isEmpty() ? Optional.empty() : Optional.of(stack.peekFirst());
        }

        @Override
        Optional<OpenSpan> pop() {
            return stack.isEmpty() ? Optional.empty() : Optional.of(stack.pop());
        }

        @Override
        boolean isEmpty() {
            return stack.isEmpty();
        }

        @Override
        boolean isObservable() {
            return true;
        }

        @Override
        Trace deepCopy() {
            return new Sampled(new ArrayDeque<>(stack), getTraceId());
        }

        @Override
        public String toString() {
            return "Trace{stack=" + stack + ", isObservable=true, traceId='" + getTraceId() + "'}";
        }
    }

    private static final class Unsampled extends Trace {
        private int depth;

        private Unsampled(int depth, String traceId) {
            super(traceId);
            this.depth = depth;
            validateDepth();
        }

        private Unsampled(String traceId) {
            this(0, traceId);
        }

        @Override
        void fastStartSpan(String operation, String parentSpanId, SpanType type) {
            fastStartSpan(operation, type);
        }

        @Override
        void fastStartSpan(String operation, SpanType type) {
            depth++;
        }

        @Override
        void push(OpenSpan span) {
            depth++;
        }

        @Override
        Optional<OpenSpan> top() {
            return Optional.empty();
        }

        @Override
        Optional<OpenSpan> pop() {
            validateDepth();
            if (depth > 0) {
                depth--;
            }
            return Optional.empty();
        }

        @Override
        boolean isEmpty() {
            validateDepth();
            return depth <= 0;
        }

        @Override
        boolean isObservable() {
            return false;
        }

        @Override
        Trace deepCopy() {
            return new Unsampled(depth, getTraceId());
        }

        private void validateDepth() {
            if (depth < 0) {
                throw new SafeIllegalStateException("Unexpected negative depth", SafeArg.of("depth", depth));
            }
        }

        @Override
        public String toString() {
            return "Trace{depth=" + depth + ", isObservable=false, traceId='" + getTraceId() + "'}";
        }
    }
}
