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

import com.google.common.collect.ImmutableMap;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class CloseableTracerTest {

    @Before
    public void before() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.getAndClearTrace();
    }

    @Test
    public void startsAndClosesSpan() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("foo")) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.LOCAL);
        }
        assertThat(Tracer.getAndClearTrace().top()).isEmpty();
    }

    @Test
    public void startsAndClosesSpanWithType() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("foo", SpanType.CLIENT_OUTGOING)) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        }
        assertThat(Tracer.getAndClearTrace().top()).isEmpty();
    }

    @Test
    public void supportsMetadata_map() {
        SpanObserver observer = Mockito.mock(SpanObserver.class);
        String name = "CloseableTracerTest#supportsMetadata_map";
        Tracer.subscribe(name, observer);
        try {
            try (CloseableTracer tracer = CloseableTracer.startSpan("foo", ImmutableMap.of("key", "value"))) {
                OpenSpan openSpan = Tracer.copyTrace().get().top().get();
                assertThat(openSpan.getOperation()).isEqualTo("foo");
                assertThat(openSpan.type()).isEqualTo(SpanType.LOCAL);
            }
            assertThat(Tracer.getAndClearTrace().top()).isEmpty();
            ArgumentCaptor<Span> captor = ArgumentCaptor.forClass(Span.class);
            Mockito.verify(observer).consume(captor.capture());
            Span emitted = captor.getValue();
            assertThat(emitted.getOperation()).isEqualTo("foo");
            assertThat(emitted.getMetadata()).containsEntry("key", "value");
        } finally {
            Tracer.unsubscribe(name);
        }
    }

    @Test
    public void supportsMetadata_translator() {
        SpanObserver observer = Mockito.mock(SpanObserver.class);
        String name = "CloseableTracerTest#supportsMetadata_translator";
        Tracer.subscribe(name, observer);
        try {
            try (CloseableTracer tracer = CloseableTracer.startSpan(
                    "foo",
                    new TagTranslator<String>() {
                        @Override
                        public <T> void translate(TagAdapter<T> adapter, T target, String data) {
                            adapter.tag(target, "key", data);
                        }
                    },
                    "value")) {
                OpenSpan openSpan = Tracer.copyTrace().get().top().get();
                assertThat(openSpan.getOperation()).isEqualTo("foo");
                assertThat(openSpan.type()).isEqualTo(SpanType.LOCAL);
            }
            assertThat(Tracer.getAndClearTrace().top()).isEmpty();
            ArgumentCaptor<Span> captor = ArgumentCaptor.forClass(Span.class);
            Mockito.verify(observer).consume(captor.capture());
            Span emitted = captor.getValue();
            assertThat(emitted.getOperation()).isEqualTo("foo");
            assertThat(emitted.getMetadata()).containsEntry("key", "value");
        } finally {
            Tracer.unsubscribe(name);
        }
    }
}
