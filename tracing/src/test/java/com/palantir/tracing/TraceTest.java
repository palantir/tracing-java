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
import org.junit.Test;

public final class TraceTest {

    @Test
    public void constructTrace_emptyTraceId() {
        assertThatThrownBy(() -> new Trace(false, ""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testToString() {
        Trace trace = new Trace(true, "traceId");
        trace.push(OpenSpan.builder()
                .type(SpanType.LOCAL)
                .spanId("spanId")
                .operation("operation")
                .startClockNanoSeconds(0L)
                .startTimeMicroSeconds(0L)
                .build());
        assertThat(trace.toString()).isEqualTo("Trace{stack=[OpenSpan{operation=operation, startTimeMicroSeconds=0, "
                + "startClockNanoSeconds=0, spanId=spanId, type=LOCAL}], isObservable=true, traceId='traceId'}");
    }

    @Test
    public void noop() {
        Trace traceId = Trace.create(false, "traceId");
        assertThat(traceId.getTraceId()).isEqualTo("noop");
        assertThat(traceId.isObservable()).isFalse();
        assertThat(traceId.isEmpty()).isTrue();
        assertThat(traceId.pop()).isNotPresent();
        assertThat(traceId.top()).isNotPresent();
        assertThat(traceId.deepCopy()).isSameAs(traceId);

        traceId.push(OpenSpan.builder().spanId("spanId").operation("operation").type(SpanType.LOCAL).build());
        assertThat(traceId.getTraceId()).isEqualTo("noop");
        assertThat(traceId.isObservable()).isFalse();
        assertThat(traceId.isEmpty()).isTrue();
        assertThat(traceId.pop()).isNotPresent();
        assertThat(traceId.top()).isNotPresent();
        assertThat(traceId.deepCopy()).isSameAs(traceId);

        assertThat(traceId.toString()).isEqualTo("Trace{stack=[], isObservable=false, traceId='noop'}");

        assertThat(traceId).isSameAs(Trace.create(false, "traceId2"));
    }

}
