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
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class CloseableTracerTest {

    @Mock
    private SpanObserver spanObserver;
    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Before
    public void before() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.getAndClearTrace();
        Tracer.subscribe("spanObserver", spanObserver);
    }

    @Test
    public void startsAndClosesSpan() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("foo")) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.LOCAL);
        }

        verify(spanObserver).consume(spanCaptor.capture());

        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("foo");
        assertThat(span.type()).isEqualTo(SpanType.LOCAL);
        assertThat(span.getMetadata()).isEmpty();
    }

    @Test
    public void startsAndClosesSpanWithMetadata() {
        Map<String, String> metadata = ImmutableMap.of("key", "value");

        try (CloseableTracer tracer = CloseableTracer.startSpan("foo", metadata)) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.LOCAL);
        }

        verify(spanObserver).consume(spanCaptor.capture());

        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("foo");
        assertThat(span.type()).isEqualTo(SpanType.LOCAL);
        assertThat(span.getMetadata()).isEqualTo(metadata);
    }

    @Test
    public void startsAndClosesSpanWithType() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("foo", SpanType.CLIENT_OUTGOING)) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        }

        verify(spanObserver).consume(spanCaptor.capture());

        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("foo");
        assertThat(span.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        assertThat(span.getMetadata()).isEmpty();
    }

    @Test
    public void startsAndClosesSpanWithTypeAndMetadata() {
        Map<String, String> metadata = ImmutableMap.of("key", "value");

        try (CloseableTracer tracer = CloseableTracer.startSpan("foo", SpanType.CLIENT_OUTGOING, metadata)) {
            OpenSpan openSpan = Tracer.copyTrace().get().top().get();
            assertThat(openSpan.getOperation()).isEqualTo("foo");
            assertThat(openSpan.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        }

        verify(spanObserver).consume(spanCaptor.capture());

        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("foo");
        assertThat(span.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
        assertThat(span.getMetadata()).isEqualTo(metadata);
    }
}
