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

final class DefaultSpan implements Span {
    private static Clock clock = Clock.systemUTC();

    private final Trace trace;
    private final Optional<Span> parent;
    private final String opName;
    private final Instant start;
    private final String spanId;

    DefaultSpan(Trace trace, Optional<Span> parent, String opName) {
        this.trace = trace;
        this.parent = parent;
        this.opName = opName;
        this.start = clock.instant();
        this.spanId = Spans.newId();

        Spans.setThreadSpan(this);
    }

    @Override
    public Span sibling(String op) {
        close();
        return new DefaultSpan(trace, parent, op);
    }

    @Override
    public Span child(String op) {
        return new DefaultSpan(trace, Optional.of(this), op);
    }

    public String spanId() {
        return spanId;
    }

    @Override
    public void close() {
        Spans.notify(ImmutableCompletedSpan.builder()
                .traceId(trace.getTraceId())
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
