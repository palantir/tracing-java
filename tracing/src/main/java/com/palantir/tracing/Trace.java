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
import com.google.errorprone.annotations.CheckReturnValue;
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
 *
 * There are two implementations of {@link Trace}: {@link Sampled} and {@link Unsampled}.
 * A {@link Sampled sampled trace} records each span in order to record tracing data, however in most scenarios
 * most traces will be {@link Unsampled}, which avoids creation of span objects, random span ID generation,
 * clock reads, etc. Instead, the {@link Unsampled unsampled} implementation tracks the number of 'active' spans
 * on the current thread so it can provide correct {@link Trace#isEmpty()} values allowing the {@link Tracer}
 * utility to reset thread state after the emulated root span has been completed.
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
     * If the return value is not used, prefer {@link #fastStartSpan(String, String, SpanType)}}.
     */
    @CheckReturnValue
    final OpenSpan startSpan(String operation, String parentSpanId, SpanType type) {
        checkState(isEmpty(), "Cannot start a span with explicit parent if the current thread's trace is non-empty");
        checkArgument(!Strings.isNullOrEmpty(parentSpanId), "parentSpanId must be non-empty");
        OpenSpan span = OpenSpan.of(
                operation,
                Tracers.randomId(),
                type,
                Optional.of(parentSpanId),
                orElse(getOriginatingSpanId(), Optional.of(parentSpanId)));
        push(span);
        return span;
    }

    /**
     * Opens a new span for this thread's call trace, labeled with the provided operation.
     * If the return value is not used, prefer {@link #fastStartSpan(String, SpanType)}}.
     */
    @CheckReturnValue
    final OpenSpan startSpan(String operation, SpanType type) {
        Optional<OpenSpan> prevState = top();
        final OpenSpan span;
        // Avoid lambda allocation in hot paths
        if (prevState.isPresent()) {
            span = OpenSpan.of(
                    operation,
                    Tracers.randomId(),
                    type,
                    Optional.of(prevState.get().getSpanId()),
                    orElse(getOriginatingSpanId(), prevState.get().getParentSpanId()));
        } else {
            span = OpenSpan.of(
                    operation,
                    Tracers.randomId(),
                    type,
                    Optional.empty(),
                    getOriginatingSpanId());
        }

        push(span);
        return span;
    }

    private static <T> Optional<T> orElse(Optional<T> left, Optional<T> right) {
        if (left.isPresent()) {
            return left;
        }
        return right;
    }

    /**
     * Like {@link #startSpan(String, String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    abstract void fastStartSpan(String operation, String parentSpanId, SpanType type);

    /**
     * Like {@link #startSpan(String, SpanType)}, but does not return an {@link OpenSpan}.
     */
    abstract void fastStartSpan(String operation, SpanType type);

    protected abstract void push(OpenSpan span);

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

    /**
     * The globally unique non-empty identifier for this call trace within a service (or another locality context).
     *
     * While {@link #getTraceId()} is expected to be propagated across RPC calls, the top span id distinguishes
     * between two concurrent RPC calls made to a service with the same traceid.
     */
    abstract Optional<String> getTopSpanId();

    abstract Optional<String> getOriginatingSpanId();

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
        protected void push(OpenSpan span) {
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
        Optional<String> getTopSpanId() {
            if (stack.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(stack.peekLast().getSpanId());
        }

        @Override
        Optional<String> getOriginatingSpanId() {
            if (stack.isEmpty()) {
                return Optional.empty();
            }
            return stack.peekLast().getParentSpanId();
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
        /**
         * Tracks the size that a {@link Sampled} trace {@link Sampled#stack} would have <i>if</i> this was sampled.
         * This allows thread trace state to be cleared when all "started" spans have been "removed".
         */
        private int numberOfSpans;
        private Optional<String> originatingSpanId;
        private Optional<String> topSpanId;

        private Unsampled(
                int numberOfSpans,
                String traceId,
                Optional<String> originatingSpanId,
                Optional<String> topSpanId) {
            super(traceId);
            this.numberOfSpans = numberOfSpans;
            this.originatingSpanId = originatingSpanId;
            this.topSpanId = topSpanId;
            validateNumberOfSpans();
        }

        private Unsampled(String traceId) {
            this(0, traceId, Optional.empty(), Optional.empty());
        }

        @Override
        void fastStartSpan(String _operation, String parentSpanId, SpanType _type) {
            if (numberOfSpans == 0) {
                originatingSpanId = Optional.of(parentSpanId);
                topSpanId = Optional.of(Tracers.randomId());
            }
            numberOfSpans++;
        }

        @Override
        void fastStartSpan(String _operation, SpanType _type) {
            if (numberOfSpans == 0) {
                topSpanId = Optional.of(Tracers.randomId());
            }
            numberOfSpans++;
        }

        @Override
        protected void push(OpenSpan span) {
            if (numberOfSpans == 0) {
                originatingSpanId = span.getParentSpanId();
                topSpanId = Optional.of(span.getSpanId());
            }
            numberOfSpans++;
        }

        @Override
        Optional<OpenSpan> top() {
            return Optional.empty();
        }

        @Override
        Optional<OpenSpan> pop() {
            validateNumberOfSpans();
            if (numberOfSpans > 0) {
                numberOfSpans--;
            }
            if (numberOfSpans == 0) {
                originatingSpanId = Optional.empty();
                topSpanId = Optional.empty();
            }
            return Optional.empty();
        }

        @Override
        boolean isEmpty() {
            validateNumberOfSpans();
            return numberOfSpans <= 0;
        }

        @Override
        boolean isObservable() {
            return false;
        }

        @Override
        Optional<String> getTopSpanId() {
            return topSpanId;
        }

        @Override
        Optional<String> getOriginatingSpanId() {
            return originatingSpanId;
        }

        @Override
        Trace deepCopy() {
            return new Unsampled(numberOfSpans, getTraceId(), getOriginatingSpanId(), getTopSpanId());
        }

        /** Internal validation, this should never fail because {@link #pop()} only decrements positive values. */
        private void validateNumberOfSpans() {
            if (numberOfSpans < 0) {
                throw new SafeIllegalStateException("Unexpected negative numberOfSpans",
                        SafeArg.of("numberOfSpans", numberOfSpans));
            }
        }

        @Override
        public String toString() {
            return "Trace{numberOfSpans=" + numberOfSpans + ", isObservable=false, traceId='" + getTraceId() + "'}";
        }
    }
}
