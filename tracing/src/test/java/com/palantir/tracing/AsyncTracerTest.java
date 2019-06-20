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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class AsyncTracerTest {

    @Before
    public void before() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @Test
    public void doesNotLeakEnqueueSpan() {
        Tracer.initTrace(Observability.UNDECIDED, "defaultTraceId");
        Trace originalTrace = getTrace();
        AsyncTracer deferredTracer = new AsyncTracer();
        assertThat(originalTrace.top()).isEmpty();

        deferredTracer.withTrace(() -> {
            Trace traceCopy = Tracer.copyTrace().get();
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("async-run"));
            return null;
        });
    }

    @Test
    public void completesBothDeferredSpans() {
        Tracer.initTrace(Observability.SAMPLE, "defaultTraceId");
        Tracer.fastStartSpan("defaultSpan");
        AsyncTracer asyncTracer = new AsyncTracer();
        List<String> observedSpans = Lists.newArrayList();
        Tracer.subscribe(
                AsyncTracerTest.class.getName(),
                span -> observedSpans.add(span.getOperation()));

        asyncTracer.withTrace(() -> null);
        Tracer.unsubscribe(AsyncTracerTest.class.getName());
        assertThat(observedSpans).containsExactly("async-enqueue", "async-run");
    }

    @Test
    public void preservesState() {
        Tracer.initTrace(Observability.UNDECIDED, "defaultTraceId");
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");
        Tracer.fastStartSpan("baz");
        Trace originalTrace = getTrace();
        AsyncTracer asyncTracer = new AsyncTracer();

        asyncTracer.withTrace(() -> {
            Trace traceCopy = Tracer.copyTrace().get();
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("async-run"));
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("baz"));
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("bar"));
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("foo"));
            return null;
        });

        assertThat(originalTrace.pop())
                .isPresent()
                .hasValueSatisfying(span -> span.getSpanId().equals("baz"));
        assertThat(originalTrace.pop())
                .isPresent()
                .hasValueSatisfying(span -> span.getSpanId().equals("bar"));
        assertThat(originalTrace.pop())
                .isPresent()
                .hasValueSatisfying(span -> span.getSpanId().equals("foo"));
    }

    /**
     * Get reference to the current trace.
     */
    private Trace getTrace() {
        Trace originalTrace = Tracer.getAndClearTrace();
        Tracer.setTrace(originalTrace);
        return originalTrace;
    }
}
