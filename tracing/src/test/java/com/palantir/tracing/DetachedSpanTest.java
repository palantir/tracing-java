/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.tracing.api.Span;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

public final class DetachedSpanTest {

    @Before
    public void before() {
        MDC.clear();
        Tracer.clearCurrentTrace();
    }

    @After
    public void after() {
        // Clear out the old trace from each test
        Tracer.clearCurrentTrace();
    }

    @Test
    public void testAttach_sampled() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        String subscription = UUID.randomUUID().toString();
        List<Span> spans = new CopyOnWriteArrayList<>();
        Tracer.subscribe(subscription, spans::add);
        try {
            DetachedSpan span = DetachedSpan.start("root");
            assertThat(Tracer.hasTraceId()).isFalse();
            try (CloseableSpan ignored = span.attach()) {
                assertThat(Tracer.hasTraceId()).as("Expected a traceId").isTrue();
                String traceId = Tracer.getTraceId();
                Tracer.fastStartSpan("one");
                assertThat(Tracer.getTraceId()).isEqualTo(traceId);
                Tracer.fastCompleteSpan();

                // Completing the first nested span should leave tracing state in-tact
                assertThat(Tracer.getTraceId()).isEqualTo(traceId);

                Tracer.fastStartSpan("two");
                assertThat(Tracer.getTraceId()).isEqualTo(traceId);
                Tracer.fastCompleteSpan();
            }
            assertThat(Tracer.hasTraceId())
                    .as("Completing the detached span clears thread state")
                    .isFalse();
            span.complete();
            assertThat(spans).hasSize(3);
            Span oneSpan = spans.get(0);
            Span twoSpan = spans.get(1);
            Span rootSpan = spans.get(2);
            assertThat(rootSpan.getParentSpanId()).isEmpty();
            assertThat(rootSpan.getOperation()).isEqualTo("root");
            assertThat(rootSpan.getTraceId()).isEqualTo(oneSpan.getTraceId()).isEqualTo(twoSpan.getTraceId());
            assertThat(oneSpan.getOperation()).isEqualTo("one");
            assertThat(oneSpan.getParentSpanId()).hasValue(rootSpan.getSpanId());
            assertThat(twoSpan.getOperation()).isEqualTo("two");
            assertThat(twoSpan.getParentSpanId()).hasValue(rootSpan.getSpanId());
        } finally {
            Tracer.unsubscribe(subscription);
        }
    }

    @Test
    public void testAttachThreadState_sampled() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        testAttachThreadState();
    }

    @Test
    public void testAttachThreadState_unsampled() {
        Tracer.setSampler(NeverSampler.INSTANCE);
        testAttachThreadState();
    }

    private void testAttachThreadState() {
        DetachedSpan span = DetachedSpan.start("root");
        assertThat(Tracer.hasTraceId()).isFalse();
        try (CloseableSpan ignored = span.attach()) {
            assertThat(Tracer.hasTraceId()).as("Expected a traceId").isTrue();
            String traceId = Tracer.getTraceId();
            Tracer.fastStartSpan("one");
            assertThat(Tracer.getTraceId()).isEqualTo(traceId);
            Tracer.fastCompleteSpan();

            // Completing the first nested span should leave tracing state in-tact
            assertThat(Tracer.getTraceId()).isEqualTo(traceId);

            Tracer.fastStartSpan("two");
            assertThat(Tracer.getTraceId()).isEqualTo(traceId);
            Tracer.fastCompleteSpan();
        }
        assertThat(Tracer.hasTraceId())
                .as("Completing the detached span clears thread state")
                .isFalse();
    }

    @Test
    public void testAttachRestoresPreviousTrace_sampled() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        testAttachRestoresPreviousTrace();
    }

    @Test
    public void testAttachRestoresPreviousTrace_unsampled() {
        Tracer.setSampler(NeverSampler.INSTANCE);
        testAttachRestoresPreviousTrace();
    }

    private void testAttachRestoresPreviousTrace() {
        // This detached span has a unique identifier which differs from the trace
        // initialized by 'otherTrace'
        DetachedSpan span = DetachedSpan.start("root");

        Tracer.fastStartSpan("otherTrace");
        String otherTraceId = Tracer.getTraceId();
        // begin
        try (CloseableSpan ignored = span.attach()) {
            String detachedSpanTraceId = Tracer.getTraceId();
            assertThat(detachedSpanTraceId).isNotEqualTo(otherTraceId);
        }
        assertThat(Tracer.getTraceId())
                .as("The previous thread state should be restored")
                .isEqualTo(otherTraceId);
        Tracer.fastCompleteSpan();
        assertThat(Tracer.hasTraceId()).isFalse();
    }
}
