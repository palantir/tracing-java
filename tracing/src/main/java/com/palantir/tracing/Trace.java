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

import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.errorprone.annotations.CheckReturnValue;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.v2.api.OpenSpan;
import com.palantir.tracing.v2.api.Span;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 *
 * <p>There are two implementations of {@link Trace}: {@link Sampled} and {@link Unsampled}. A {@link Sampled sampled
 * trace} records each span in order to record tracing data, however in most scenarios most traces will be
 * {@link Unsampled}, which avoids creation of span objects, random span ID generation, clock reads, etc.
 */
public abstract class Trace {

    private final TraceState traceState;

    private Trace(TraceState traceState) {
        checkNotNull(traceState, "Trace state must not be null");
        this.traceState = traceState;
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation and parent span. Only
     * allowed
     * when the current trace is empty. If the return value is not used, prefer {@link #fastStartSpan(String,
     * String,
     * SpanType)}}.
     */
    @CheckReturnValue
    final com.palantir.tracing.api.OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        return startSpan(operation, Optional.of(parentSpanId), type);
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation. If the return value is
     * not
     * used, prefer {@link #fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    final com.palantir.tracing.api.OpenSpan startSpan(String operation, SpanType type) {
        return startSpan(operation, top().map(EnabledOpenSpan::spanId), type);
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation. If the return value is
     * not
     * used, prefer {@link #fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    final com.palantir.tracing.api.OpenSpan startSpan(String operation, Optional<String> parentSpanId, SpanType type) {
        EnabledOpenSpan span = new EnabledSpan(Tracers.randomId(), parentSpanId, operation, type);
        push(span);
        return span;
    }

    abstract void fastStartSpan(String operation, String parentSpanId, SpanType type);

    abstract void fastStartSpan(String operation, SpanType type);

    abstract Span createSpan(String operation, SpanType type);

    abstract EnabledOpenSpan push(EnabledSpan span);

    @Nullable
    abstract EnabledOpenSpan top();

    @Nullable
    abstract EnabledOpenSpan pop();

    /**
     * True iff the spans of this trace are to be observed by {@link SpanObserver span obververs} upon
     * {@link Tracer#completeSpan span completion}.
     */
    abstract boolean isObservable();

    /** The state of the trace which is stored for each created trace. */
    final TraceState getTraceState() {
        return this.traceState;
    }

    /** Returns a copy of this Trace which can be independently mutated. */
    abstract Trace deepCopy();

    static Trace of(boolean isObservable, TraceState traceState) {
        return isObservable ? new Sampled(traceState) : new Unsampled(traceState);
    }

    private static final class Sampled extends Trace {

        private final Deque<EnabledOpenSpan> stack;

        private Sampled(Deque<EnabledOpenSpan> stack, TraceState traceState) {
            super(traceState);
            this.stack = stack;
        }

        private Sampled(TraceState traceState) {
            this(new ArrayDeque<>(), traceState);
        }

        @Override
        @SuppressWarnings("ResultOfMethodCallIgnored") // Sampled traces cannot optimize this path
        void fastStartSpan(String operation, String parentSpanId, SpanType type) {
            startSpan(operation, parentSpanId, type);
        }

        @Override
        @SuppressWarnings("ResultOfMethodCallIgnored") // Sampled traces cannot optimize this path
        void fastStartSpan(String operation, SpanType type) {
            startSpan(operation, type);
        }

        @Override
        Span createSpan(String operation, SpanType type) {
            return null;
        }

        @Override
        EnabledOpenSpan push(EnabledSpan span) {
            return null;
        }

        @Override
        EnabledOpenSpan top() {
            return stack.peekFirst();
        }

        @Override
        EnabledOpenSpan pop() {
            return stack.pollFirst();
        }

        @Override
        boolean isObservable() {
            return true;
        }

        @Override
        Trace deepCopy() {
            return new Sampled(new ArrayDeque<>(stack), getTraceState());
        }

        @Override
        public String toString() {
            return "Trace{stack=" + stack + ", isObservable=true, state=" + getTraceState() + "}";
        }
    }

    private static final class Unsampled extends Trace {

        private Unsampled(TraceState traceState) {
            super(traceState);
        }

        @Override
        void fastStartSpan(String _operation, String _parentSpanId, SpanType _type) {}

        @Override
        void fastStartSpan(String _operation, SpanType _type) {}

        @Override
        Span createSpan(String operation, SpanType type) {
            return DisabledSpan.INSTANCE;
        }

        @Override
        EnabledOpenSpan push(EnabledSpan span) {
            throw new UnsupportedOperationException();
            return null;
        }

        @Override
        Span createSpan(String operation) {
            return DisabledSpan.INSTANCE;
        }

        @Override
        protected void push(OpenSpan _span) {}

        @Override
        EnabledOpenSpan top() {
            return null;
        }

        @Override
        EnabledOpenSpan pop() {
            return null;
        }

        @Override
        boolean isObservable() {
            return false;
        }

        @Override
        Trace deepCopy() {
            return new Unsampled(getTraceState());
        }

        @Override
        public String toString() {
            return "Trace{isObservable=false, traceState=" + getTraceState() + "}";
        }
    }
}
