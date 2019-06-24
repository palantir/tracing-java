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

import com.palantir.tracing.AlwaysSampler;
import com.palantir.tracing.Tracer;
import java.io.ByteArrayOutputStream;
import javax.ws.rs.core.StreamingOutput;
import org.junit.Test;

public final class JaxRsTracersTest {

    @Test
    public void testWrappingStreamingOutput_streamingOutputTraceIsIsolated_sampled() throws Exception {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.getAndClearTrace();

        Tracer.fastStartSpan("outside");
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            Tracer.fastStartSpan("inside"); // never completed
        });
        streamingOutput.write(new ByteArrayOutputStream());
        assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("outside");
    }

    @Test
    public void testWrappingStreamingOutput_streamingOutputTraceIsIsolated_unsampled() throws Exception {
        Tracer.setSampler(() -> false);
        Tracer.getAndClearTrace();

        Tracer.fastStartSpan("outside");
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            Tracer.fastStartSpan("inside"); // never completed
        });
        streamingOutput.write(new ByteArrayOutputStream());
        assertThat(Tracer.hasTraceId()).isTrue();
        Tracer.fastCompleteSpan();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testWrappingStreamingOutput_traceStateIsCapturedAtConstructionTime_sampled() throws Exception {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.getAndClearTrace();

        Tracer.fastStartSpan("before-construction");
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            assertThat(Tracer.completeSpan().get().getOperation()).isEqualTo("streaming-output");
        });
        Tracer.fastStartSpan("after-construction");
        streamingOutput.write(new ByteArrayOutputStream());
    }

    @Test
    public void testWrappingStreamingOutput_traceStateIsCapturedAtConstructionTime_unsampled() throws Exception {
        Tracer.setSampler(() -> false);
        Tracer.getAndClearTrace();

        Tracer.fastStartSpan("before-construction");
        StreamingOutput streamingOutput = JaxRsTracers.wrap(os -> {
            assertThat(Tracer.hasTraceId()).isTrue();
            Tracer.fastCompleteSpan();
            assertThat(Tracer.hasTraceId()).isFalse();
        });
        Tracer.fastStartSpan("after-construction");
        streamingOutput.write(new ByteArrayOutputStream());
    }
}
