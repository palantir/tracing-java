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

package com.palantir.tracing2;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Encodes a concrete, observed {@link Span}.
 * <p>
 * The methods on this class enable creation of child and sibling Spans that are part of the same overall Trace as
 * this Span. Sibling spans are a short-cut method for closing this Span and starting another Span with the same parent.
 * On completion/close, this implementation emits a {@link CompletedSpan} using {@link Spans#notify(CompletedSpan)}.
 * <p>
 * Users may choose to explicitly pass around {@link Span} instances to manually connect children and siblings to
 * traces, and to complete traces. A common usage pattern is to place the Span within a try-with-resources block:
 * <pre>
 *     Trace trace = Traces.newTrace(Id.randomId(), true);
 *     try (Span rootSpan = trace.rootSpan("operationName")) {
 *          // traced work for root
 *          try (Span childSpan = rootSpan.child("childOperation")) {
 *              // traced work for child
 *          }
 *          // work also in the root span
 *     }
 *     // untraced work
 * </pre>
 * <p>
 * Users may also take advantage of implicit trace propagation using {@link Spans#forCurrentTrace(String)}. This
 * class always updates local thread state to match the most recently created span or the parent of the most
 * recently completed span. It's not possible to obtain the "current" implicit span, only to derive a child span, and
 * so generally speaking it's not possible to close implicitly passed parent spans.
 * <p>
 * Users should use implicit spans similar to explicit spans:
 * <pre>
 *     try (Span ignored = Spans.forCurrentTrace("operationName")) {
 *         // traced work
 *     }
 * </pre>
 * <p>
 * Implicit spans are especially valuable when tracing may traverse many levels of existing code, and likely should
 * be preferred in scenarios where passing the explicit Span around would create heavy code churn or add noise to
 * code structure.
 * <p>
 * Because this class always interacts with thread state, one may intermix implicit and explicit spans as desired.
 * For instance:
 * <pre>
 *     Trace trace = Traces.newTrace(Id.randomId(), true);
 *     try (Span rootSpan = trace.rootSpan("operationName")) {
 *         executor.submit(() -> {
 *             try (Span childSpan = rootSpan.child("childOperation")) {
 *                 cpuIntensiveTask();
 *             }
 *         });
 *     }
 *
 *     // for some implementation of
 *     void cpuIntensiveTask() {
 *         try (Span step1 = Spans.forCurrentTrace("cpuIntensiveTask.step1")) {
 *             // do step 1
 *         }
 *         try (Span step1 = Spans.forCurrentTrace("cpuIntensiveTask.step2")) {
 *             // do step 1
 *         }
 *     }
 * </pre>
 * <p>
 * Users should generally use {@link Tracing}'s utility methods to transit thread boundaries.
 */
final class DefaultSpan implements Span {
    private static Clock clock = Clock.systemUTC();

    private final Trace trace;
    private final Optional<Span> parent;
    private final String opName;
    private final Instant start;
    private final String spanId;

    /** Create a new {@link DefaultSpan} and set the current thread state. */
    static Span create(Trace trace, Optional<Span> parent, String opName) {
        Span span = new DefaultSpan(trace, parent, opName);
        Spans.setThreadSpan(span);
        return span;
    }

    private DefaultSpan(Trace trace, Optional<Span> parent, String opName) {
        this.trace = trace;
        this.parent = parent;
        this.opName = opName;
        this.start = clock.instant();
        this.spanId = Ids.randomId();
    }

    @Override
    public Span sibling(String op) {
        close();
        return create(trace, parent, op);
    }

    @Override
    public Span child(String op) {
        return create(trace, Optional.of(this), op);
    }

    @Override
    public String spanId() {
        return spanId;
    }

    @Override
    public void close() {
        Spans.notify(ImmutableCompletedSpan.builder()
                .traceId(trace.traceId())
                .parentId(parent.map(Span::spanId))
                .spanId(spanId)
                .opName(opName)
                .start(start)
                .duration(Duration.between(start, clock.instant()))
                .build());

        Spans.setThreadSpan(parent.orElse(null));
    }

    @VisibleForTesting
    static void setClock(Clock otherClock) {
        DefaultSpan.clock = otherClock;
    }
}
