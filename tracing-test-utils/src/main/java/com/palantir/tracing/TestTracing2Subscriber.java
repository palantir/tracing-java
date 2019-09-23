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

import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing2.CompletedSpan;
import java.time.Instant;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

final class TestTracing2Subscriber implements Consumer<CompletedSpan> {

    // Spans can be emitted from different threads, so using a concurrent datastructure rather than an ArrayList.
    private final Queue<Span> allSpans = new ConcurrentLinkedQueue();

    @Override
    public void accept(CompletedSpan span) {
        allSpans.add(Span.builder()
                .traceId(span.traceId())
                .parentSpanId(span.parentId())
                .spanId(span.spanId())
                .operation(span.opName())
                .type(SpanType.LOCAL)
                .startTimeMicroSeconds(getNowInMicroSeconds(span.start()))
                .durationNanoSeconds(span.duration().getNano())
                .build());
    }

    public Collection<Span> getAllSpans() {
        return allSpans;
    }


    private static long getNowInMicroSeconds(Instant now) {
        return (1000000 * now.getEpochSecond()) + (now.getNano() / 1000);
    }

}
