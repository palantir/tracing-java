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

package com.palantir.tracing.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import java.io.ByteArrayOutputStream;
import javax.ws.rs.core.StreamingOutput;
import org.junit.Test;

public final class JaxRsTracersTest {

    @Test
    public void testWrappingStreamingOutput_streamingOutputTraceIsIsolated() throws Exception {
        Tracer.startSpan("outside");
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            Tracer.startSpan("inside"); // never completed
        });
        streamingOutput.write(new ByteArrayOutputStream());
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingStreamingOutput_traceStateIsCapturedAtConstructionTime() throws Exception {
        String beforeSpanId = Tracer.startSpan("before-construction").getSpanId();
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            Span span = Tracer.completeSpan().get();
            assertThat(span.getOperation()).isEqualTo("streaming-output");
            assertThat(span.getParentSpanId()).contains(beforeSpanId);
        });
        Tracer.startSpan("after-construction");
        streamingOutput.write(new ByteArrayOutputStream());
    }
}
