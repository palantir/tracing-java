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

import com.palantir.tracing.api.SpanType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

public class DeferredTracerTest {

    @Test
    public void testIsSerializable() throws IOException, ClassNotFoundException {
        Tracer.initTraceWithSpan(Observability.UNDECIDED, "defaultTraceId", "span", SpanType.LOCAL);

        DeferredTracer deferredTracer = new DeferredTracer("operation");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(deferredTracer);
        }

        Tracer.initTraceWithSpan(Observability.UNDECIDED, "someOtherTraceId", "span", SpanType.LOCAL);

        DeferredTracer deserialized;

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream objectInputStream = new ObjectInputStream(bais)) {
            deserialized = (DeferredTracer) objectInputStream.readObject();
        }

        assertThat(Tracer.getTraceId()).isEqualTo("someOtherTraceId");

        try (CloseableTracer tracer = deserialized.startSpan()) {
            assertThat(Tracer.getTraceId()).isEqualTo("defaultTraceId");
        }

        assertThat(Tracer.getTraceId()).isEqualTo("someOtherTraceId");
    }
}
