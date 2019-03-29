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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class DeferredTracerTest {

    @Test
    public void testIsSerializable() throws IOException, ClassNotFoundException {
        Tracer.initTrace(Optional.empty(), "defaultTraceId");
        Tracer.startSpan("defaultOperation");

        DeferredTracer deferredTracer = new DeferredTracer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(deferredTracer);
        }

        Tracer.initTrace(Optional.empty(), "someOtherTraceId");

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream objectInputStream = new ObjectInputStream(bais)) {
            DeferredTracer deserialized = (DeferredTracer) objectInputStream.readObject();

            String trace = deserialized.withTrace(() -> Tracer.getTraceId());
            assertThat(trace).isEqualTo("defaultTraceId");
        }
    }

    @Test
    public void doesNotLeakSpans() {
        Tracer.initTrace(Optional.empty(), "defaultTraceId");
        Trace originalTrace = Tracer.getAndClearTrace();
        Tracer.setTrace(originalTrace);
        DeferredTracer deferredTracer = new DeferredTracer();
        assertThat(originalTrace.top()).isEmpty();

        deferredTracer.withTrace(() -> {
            Trace traceCopy = Tracer.copyTrace().get();
            assertThat(traceCopy.pop())
                    .isPresent()
                    .hasValueSatisfying(span -> span.getSpanId().equals("deferred-run"));
            return null;
        });
    }

    @Test
    public void completesBothDeferredSpans() {
        Tracer.initTrace(Optional.empty(), "defaultTraceId");
        DeferredTracer deferredTracer = new DeferredTracer();
        List<String> observedSpans = Lists.newArrayList();
        Tracer.subscribe(
                TracerTest.class.getName(),
                span -> observedSpans.add(span.getOperation()));

        deferredTracer.withTrace(() -> null);
        Tracer.unsubscribe(TracerTest.class.getName());
        assertThat(observedSpans).containsExactly("deferred-enqueue", "deferred-run");
    }
}
