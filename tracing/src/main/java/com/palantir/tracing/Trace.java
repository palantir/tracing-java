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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanType;
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
 * {@link Unsampled}, which avoids creation of span objects, random span ID generation, clock reads, etc. Instead, the
 * {@link Unsampled unsampled} implementation tracks the number of 'active' spans on the current thread so it can
 * provide correct {@link Trace#isEmpty()} values allowing the {@link Tracer} utility to reset thread state after the
 * emulated root span has been completed.
 */
public abstract class Trace {

    private final TraceState traceState;

    private Trace(TraceState traceState) {
        checkNotNull(traceState, "Trace state must not be null");
        this.traceState = traceState;
    }

    abstract void fastStartSpan(String operation, Optional<String> parentSpanId, SpanType type);

    @Nullable
    abstract OpenSpan fastCompleteSpan();

    abstract void push(OpenSpan span);

    @Nullable
    abstract OpenSpan current();

    abstract boolean isEmpty();

    /** The state of the trace which is stored for each created trace. */
    final TraceState traceState() {
        return traceState;
    }

    static Trace of(TraceState traceState) {
        return traceState.isObservable() ? new Sampled(traceState) : new Unsampled(traceState);
    }

    private static final class Sampled extends Trace {

        private final Deque<OpenSpan> stack;

        private Sampled(ArrayDeque<OpenSpan> stack, TraceState traceState) {
            super(traceState);
            this.stack = stack;
        }

        private Sampled(TraceState traceState) {
            this(new ArrayDeque<>(), traceState);
        }

        @Override
        void fastStartSpan(String operation, Optional<String> parentSpanId, SpanType type) {
            OpenSpan span = OpenSpan.of(operation, Tracers.randomId(), type, parentSpanId);
            push(span);
        }

        @Override
        @Nullable
        OpenSpan fastCompleteSpan() {
            return stack.pollFirst();
        }

        @Override
        protected void push(OpenSpan span) {
            stack.push(span);
        }

        @Override
        @Nullable
        OpenSpan current() {
            return stack.peekFirst();
        }

        @Override
        boolean isEmpty() {
            return stack.isEmpty();
        }

        @Override
        public String toString() {
            return "Trace{stack=" + stack + ", isObservable=true, state=" + traceState() + "}";
        }
    }

    private static final class Unsampled extends Trace {
        /**
         * Tracks the size that a {@link Sampled} trace {@link Sampled#stack} would have <i>if</i> this was sampled.
         * This allows thread trace state to be cleared when all "started" spans have been "removed".
         */
        private int numberOfSpans;

        private Unsampled(int numberOfSpans, TraceState traceState) {
            super(traceState);
            this.numberOfSpans = numberOfSpans;
            validateNumberOfSpans();
        }

        private Unsampled(TraceState traceState) {
            this(0, traceState);
        }

        @Override
        void fastStartSpan(String _operation, Optional<String> _parentSpanId, SpanType _type) {
            numberOfSpans++;
        }

        @Override
        @Nullable
        OpenSpan fastCompleteSpan() {
            validateNumberOfSpans();
            if (numberOfSpans > 0) {
                numberOfSpans--;
            }
            return null;
        }

        @Override
        protected void push(OpenSpan _span) {
            numberOfSpans++;
        }

        @Override
        @Nullable
        OpenSpan current() {
            return null;
        }

        @Override
        boolean isEmpty() {
            validateNumberOfSpans();
            return numberOfSpans <= 0;
        }

        /** Internal validation, this should never fail because {@link #fastCompleteSpan()} only decrements positive values. */
        private void validateNumberOfSpans() {
            if (numberOfSpans < 0) {
                throw new SafeIllegalStateException(
                        "Unexpected negative numberOfSpans", SafeArg.of("numberOfSpans", numberOfSpans));
            }
        }

        @Override
        public String toString() {
            return "Trace{numberOfSpans=" + numberOfSpans + ", isObservable=false, traceState=" + traceState() + "}";
        }
    }
}
