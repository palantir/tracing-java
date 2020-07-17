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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.SpanType;
import java.util.Optional;
import org.junit.Test;

public final class TraceTest {
    private static final String ORIGINATING_SPAN_ID = "originating span id";

    @Test
    public void constructTrace_emptyTraceId() {
        assertThatThrownBy(() -> Trace.of(false, "", Optional.empty())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToString() {
        Trace trace = Trace.of(true, "traceId", Optional.empty());
        OpenSpan span = trace.startSpan("operation", SpanType.LOCAL);
        assertThat(trace.toString())
                .isEqualTo("Trace{stack=[" + span + "], isObservable=true, traceId='traceId'}")
                .contains(span.getOperation())
                .contains(span.getSpanId());
    }

    @Test
    public void testOriginatingTraceId_slow_sampled() {
        Trace trace = Trace.of(true, "traceId", Optional.empty());
        OpenSpan originating = trace.startSpan("1", ORIGINATING_SPAN_ID, SpanType.LOCAL);
        assertThat(originating.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
        OpenSpan span = trace.startSpan("2", SpanType.LOCAL);
        assertThat(span.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
    }

    @Test
    public void testOriginatingTraceId_fast_sampled() {
        Trace trace = Trace.of(true, "traceId", Optional.empty());
        trace.fastStartSpan("1", ORIGINATING_SPAN_ID, SpanType.LOCAL);
        OpenSpan span = trace.startSpan("2", SpanType.LOCAL);
        assertThat(span.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
    }

    @Test
    public void testOriginatingTraceId_slow_unsampled() {
        Trace trace = Trace.of(false, "traceId", Optional.empty());
        OpenSpan originating = trace.startSpan("1", ORIGINATING_SPAN_ID, SpanType.LOCAL);
        assertThat(originating.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
        OpenSpan span = trace.startSpan("2", SpanType.LOCAL);
        assertThat(span.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
    }

    @Test
    public void testOriginatingTraceId_fast_unsampled() {
        Trace trace = Trace.of(false, "traceId", Optional.empty());
        trace.fastStartSpan("1", ORIGINATING_SPAN_ID, SpanType.LOCAL);
        OpenSpan span = trace.startSpan("2", SpanType.LOCAL);
        assertThat(span.getOriginatingSpanId()).contains(ORIGINATING_SPAN_ID);
    }
}
