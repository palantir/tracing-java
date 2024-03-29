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

    @Test
    public void constructTrace_emptyTraceId() {
        assertThatThrownBy(() -> Trace.of(false, TraceState.of("", Optional.empty(), Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToString() {
        Trace trace = Trace.of(true, TraceState.of("traceId", Optional.empty(), Optional.empty()));
        OpenSpan span = trace.startSpan("operation", SpanType.LOCAL);
        assertThat(trace.toString())
                .isEqualTo("Trace{"
                        + "stack=[" + span + "], "
                        + "isObservable=true, "
                        + "state=TraceState{traceId='traceId', requestId='null', forUserAgent='null'}}")
                .contains(span.getOperation())
                .contains(span.getSpanId());
    }

    @Test
    public void testToString_doesNotContainTraceLocals() {
        Trace trace = Trace.of(true, TraceState.of("traceId", Optional.empty(), Optional.empty()));

        TraceLocal<String> traceLocal = TraceLocal.of();
        trace.getTraceState().getOrCreateTraceLocals().put(traceLocal, "secret-value");

        assertThat(trace.toString()).doesNotContain("secret");
    }
}
