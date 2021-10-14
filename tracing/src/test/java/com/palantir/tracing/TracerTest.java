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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.MDC;

@RunWith(MockitoJUnitRunner.class)
public final class TracerTest {

    @Mock
    private SpanObserver observer1;

    @Mock
    private SpanObserver observer2;

    @Mock
    private TraceSampler sampler;

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @After
    public void before() {
        Tracer.getAndClearTraceIfPresent();
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @After
    public void after() {
        Tracer.initTraceWithSpan(Observability.SAMPLE, Tracers.randomId(), "op", SpanType.LOCAL);
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.unsubscribe("0");
        Tracer.unsubscribe("1");
        Tracer.unsubscribe("2");
        Tracer.getAndClearTrace();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored") // testing that exceptions are thrown
    public void testIdsMustBeNonNullAndNotEmpty() throws Exception {
        assertThatLoggableExceptionThrownBy(
                        () -> Tracer.initTraceWithSpan(Observability.UNDECIDED, null, "op", SpanType.LOCAL))
                .hasLogMessage("traceId must be non-empty")
                .hasExactlyArgs();

        assertThatLoggableExceptionThrownBy(
                        () -> Tracer.initTraceWithSpan(Observability.UNDECIDED, "", "op", SpanType.LOCAL))
                .hasLogMessage("traceId must be non-empty")
                .hasExactlyArgs();

        assertThatLoggableExceptionThrownBy(() -> Tracer.startSpan("op", null, null))
                .hasLogMessage("parentSpanId must be non-empty")
                .hasExactlyArgs();

        assertThatLoggableExceptionThrownBy(() -> Tracer.startSpan("op", "", null))
                .hasLogMessage("parentSpanId must be non-empty")
                .hasExactlyArgs();

        assertThatLoggableExceptionThrownBy(() -> Tracer.fastStartSpan("op", null, null))
                .hasLogMessage("parentSpanId must be non-empty")
                .hasExactlyArgs();

        assertThatLoggableExceptionThrownBy(() -> Tracer.fastStartSpan("op", "", null))
                .hasLogMessage("parentSpanId must be non-empty")
                .hasExactlyArgs();
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception {
        // no error when completing span without a registered subscriber
        startAndCompleteSpan();

        Tracer.subscribe("1", observer1);
        Tracer.subscribe("2", observer2);
        Span span = startAndCompleteSpan();
        verify(observer1).consume(span);
        verify(observer2).consume(span);
        verifyNoMoreInteractions(observer1, observer2);

        assertThat(Tracer.unsubscribe("1")).isEqualTo(observer1);
        span = startAndCompleteSpan();
        verify(observer2).consume(span);
        verifyNoMoreInteractions(observer1, observer2);

        assertThat(Tracer.unsubscribe("2")).isEqualTo(observer2);
        startAndCompleteSpan();
        verifyNoMoreInteractions(observer1, observer2);
    }

    @Test
    public void testCanSubscribeWithDuplicatesNames() throws Exception {
        Tracer.subscribe("1", observer1);
        assertThat(Tracer.subscribe("1", observer1)).isEqualTo(observer1);
        assertThat(Tracer.subscribe("1", observer2)).isEqualTo(observer1);
        assertThat(Tracer.subscribe("2", observer1)).isNull();
    }

    @Test
    public void testDoesNotNotifyObserversWhenCompletingNonexistingSpan() throws Exception {
        Tracer.subscribe("1", observer1);
        Tracer.subscribe("2", observer2);
        Tracer.fastCompleteSpan(); // no active span.
        verifyNoMoreInteractions(observer1, observer2);
    }

    @Test
    public void testObserversAreInvokedOnObservableTracesOnly() throws Exception {
        Tracer.subscribe("1", observer1);

        Tracer.setTrace(Trace.of(true, CommonTraceState.of(Tracers.randomId(), Optional.empty())));
        Span span = startAndCompleteSpan();
        verify(observer1).consume(span);
        span = startAndCompleteSpan();
        verify(observer1).consume(span);
        verifyNoMoreInteractions(observer1);

        Tracer.setTrace(Trace.of(false, CommonTraceState.of(Tracers.randomId(), Optional.empty())));
        startAndFastCompleteSpan(); // not sampled, see above
        verifyNoMoreInteractions(observer1);
    }

    @Test
    public void testCountsSpansWhenTraceIsNotObservable() throws Exception {
        String traceId = Tracers.randomId();
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull();
        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
        Tracer.setTrace(Trace.of(false, CommonTraceState.of(traceId, Optional.empty())));
        // Unsampled trace should still apply thread state
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(traceId);
        assertThat(Tracer.hasTraceId()).isTrue();
        assertThat(Tracer.getTraceId()).isEqualTo(traceId);
        assertThat(Tracer.hasUnobservableTrace()).isTrue();
        Tracer.fastStartSpan("foo");
        Tracer.fastStartSpan("bar");

        Tracer.fastCompleteSpan();
        // Unsampled trace should still apply thread state
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(traceId);
        assertThat(Tracer.hasTraceId()).isTrue();
        assertThat(Tracer.getTraceId()).isEqualTo(traceId);

        // Complete the root span, which should clear thread state
        Tracer.fastCompleteSpan();
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull();
        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
    }

    @Test
    public void testInitTraceCallsSampler() throws Exception {
        Tracer.setSampler(sampler);
        when(sampler.sample()).thenReturn(true, false);
        Tracer.subscribe("1", observer1);

        Span span = startAndCompleteSpan();
        verify(sampler).sample();
        verify(observer1).consume(span);
        verifyNoMoreInteractions(observer1, sampler);

        Mockito.reset(observer1, sampler);
        startAndFastCompleteSpan(); // not sampled, see above
        verify(sampler).sample();
        verifyNoMoreInteractions(observer1, sampler);
    }

    @Test
    public void testTraceCopyIsIndependent() throws Exception {
        Tracer.fastStartSpan("span");
        try {
            Trace trace = Tracer.copyTrace().get();
            trace.fastStartSpan("fop", SpanType.LOCAL);
        } finally {
            Tracer.fastCompleteSpan();
        }
        assertThat(Tracer.completeSpan()).isNotPresent();
    }

    @Test
    public void testSetTraceSetsCurrentTraceAndMdcTraceIdKey() throws Exception {
        Tracer.fastStartSpan("operation");
        Tracer.setTrace(Trace.of(true, CommonTraceState.of("newTraceId", Optional.empty())));
        assertThat(Tracer.getTraceId()).isEqualTo("newTraceId");
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo("newTraceId");
        assertThat(Tracer.completeSpan()).isEmpty();
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull();
    }

    @Test
    public void testSetTraceSetsMdcTraceSampledKeyWhenObserved() {
        Tracer.setTrace(Trace.of(true, CommonTraceState.of("observedTraceId", Optional.empty())));
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isEqualTo("1");
        assertThat(Tracer.completeSpan()).isEmpty();
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
    }

    @Test
    public void testSetTraceMissingMdcTraceSampledKeyWhenNotObserved() {
        Tracer.setTrace(Trace.of(false, CommonTraceState.of("notObservedTraceId", Optional.empty())));
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
        assertThat(Tracer.completeSpan()).isEmpty();
        assertThat(MDC.get(Tracers.TRACE_SAMPLED_KEY)).isNull();
    }

    @Test
    public void testCompletedSpanHasCorrectSpanType() throws Exception {
        for (SpanType type : SpanType.values()) {
            Tracer.fastStartSpan("1", type);
            assertThat(Tracer.completeSpan().get().type()).isEqualTo(type);
        }

        // Default is LOCAL
        Tracer.fastStartSpan("1");
        assertThat(Tracer.completeSpan().get().type()).isEqualTo(SpanType.LOCAL);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCompleteSpanWithMetadataIncludesMetadata() {
        Map<String, String> metadata = ImmutableMap.of(
                "key1", "value1",
                "key2", "value2");
        Tracer.fastStartSpan("operation");
        Optional<Span> maybeSpan = Tracer.completeSpan(metadata);
        assertThat(maybeSpan).isPresent();
        assertThat(maybeSpan.get().getMetadata()).isEqualTo(metadata);
    }

    @Test
    public void testMetadataNullIgnored() {
        Tracer.subscribe("1", observer1);
        try {
            Tracer.fastStartSpan("operation");
            Tracer.fastCompleteSpan(
                    new TagTranslator<String>() {
                        @Override
                        public <T> void translate(TagAdapter<T> adapter, T target, String _data) {
                            adapter.tag(target, "foo", null);
                            adapter.tag(target, null, "bar");
                            adapter.tag(target, "baz", "bang");
                        }
                    },
                    "str");
        } finally {
            Tracer.unsubscribe("1");
        }

        verify(observer1).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("operation");
        assertThat(span.getMetadata()).isEqualTo(ImmutableMap.of("baz", "bang"));
    }

    @Test
    public void testCompleteSpanWithoutMetadataHasNoMetadata() {
        assertThat(startAndCompleteSpan().getMetadata()).isEmpty();
    }

    @Test
    public void testFastCompleteSpan() {
        Tracer.subscribe("1", observer1);
        String operation = "operation";
        Tracer.fastStartSpan(operation);
        Tracer.fastCompleteSpan();
        verify(observer1).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testFastCompleteSpanWithMetadata() {
        Tracer.subscribe("1", observer1);
        Map<String, String> metadata = ImmutableMap.of("key", "value");
        String operation = "operation";
        Tracer.fastStartSpan("operation");
        Tracer.fastCompleteSpan(metadata);
        verify(observer1).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation);
        assertThat(spanCaptor.getValue().getMetadata()).isEqualTo(metadata);
    }

    @Test
    public void testObserversThrow() {
        Tracer.subscribe("0", _span -> {
            throw new IllegalStateException("0");
        });
        Tracer.subscribe("1", observer1);
        Tracer.subscribe("2", _span -> {
            throw new IllegalStateException("2");
        });
        String operation = "operation";
        Tracer.fastStartSpan(operation);
        Tracer.fastCompleteSpan();
        verify(observer1).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation);
    }

    @Test
    public void testGetAndClearTraceIfPresent() {
        Trace trace = Trace.of(true, CommonTraceState.of("newTraceId", Optional.empty()));
        Tracer.setTrace(trace);

        Optional<Trace> nonEmptyTrace = Tracer.getAndClearTraceIfPresent();
        assertThat(nonEmptyTrace).hasValue(trace);
        assertThat(Tracer.hasTraceId()).isFalse();

        Optional<Trace> emptyTrace = Tracer.getAndClearTraceIfPresent();
        assertThat(emptyTrace).isEmpty();
    }

    @Test
    public void testClearAndGetTraceClearsMdc() {
        Tracer.fastStartSpan("test");
        try {
            String startTrace = Tracer.getTraceId();
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo(startTrace);

            Trace oldTrace = Tracer.getAndClearTrace();
            assertThat(oldTrace.getCommonTraceState().getTraceId()).isEqualTo(startTrace);
            assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull(); // after clearing, it's empty
        } finally {
            Tracer.fastCompleteSpan();
        }
    }

    @Test
    public void testCompleteRootSpanCompletesTrace() {
        Set<String> traceIds = Sets.newHashSet();
        SpanObserver traceIdCaptor = span -> traceIds.add(span.getTraceId());
        Tracer.subscribe("traceIds", traceIdCaptor);
        try {
            // Only one root span is allowed, the second span
            // is expected to generate a second traceId
            startAndCompleteSpan();
            startAndCompleteSpan();
        } finally {
            Tracer.unsubscribe("traceIds");
        }
        assertThat(traceIds).hasSize(2);
    }

    @Test
    public void testHasTraceId() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.fastStartSpan("testSpan");
        try {
            assertThat(Tracer.hasTraceId()).isTrue();
        } finally {
            Tracer.fastCompleteSpan();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testHasUnobservableTrace_observableTrace() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
        Tracer.fastStartSpan("test");
        try {
            assertThat(Tracer.hasTraceId()).isTrue();
            assertThat(Tracer.hasUnobservableTrace()).isFalse();
        } finally {
            Tracer.fastCompleteSpan();
        }
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
    }

    @Test
    public void testHasUnobservableTrace_unobservableTrace() {
        Tracer.setSampler(NeverSampler.INSTANCE);
        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
        Tracer.fastStartSpan("test");
        try {
            assertThat(Tracer.hasTraceId()).isTrue();
            assertThat(Tracer.hasUnobservableTrace()).isTrue();
        } finally {
            Tracer.fastCompleteSpan();
        }
        assertThat(Tracer.hasUnobservableTrace()).isFalse();
    }

    @Test
    public void testSimpleDetachedTrace() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        String operation = "operation";
        DetachedSpan detached = DetachedSpan.start(operation);
        try {
            assertThat(Tracer.hasTraceId())
                    .describedAs("Detached spans should not set thread state")
                    .isFalse();
        } finally {
            detached.complete();
        }
        verify(observer1).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation);
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetachedTraceAppliedToThreadState() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        String operation1 = "operation";
        String operation2 = "attached";
        DetachedSpan detached = DetachedSpan.start(operation1);
        try {
            assertThat(Tracer.hasTraceId()).isFalse();
            try (CloseableSpan ignored = detached.childSpan(operation2)) {
                assertThat(Tracer.hasTraceId()).isTrue();
                assertThat(Tracer.maybeGetTraceMetadata().get().getRequestId()).isEmpty();
            }
            verify(observer1).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation2);
            assertThat(Tracer.hasTraceId()).isFalse();
        } finally {
            detached.complete();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetachedTraceAppliedToThreadState_requestId() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        String operation1 = "operation";
        String operation2 = "attached";
        DetachedSpan detached = DetachedSpan.start(operation1, SpanType.SERVER_INCOMING);
        String requestId = InternalTracers.getRequestId(detached).get();
        try {
            assertThat(Tracer.hasTraceId()).isFalse();
            try (CloseableSpan ignored = detached.childSpan(operation2)) {
                assertThat(Tracer.hasTraceId()).isTrue();
                assertThat(Tracer.maybeGetTraceMetadata().get().getRequestId()).hasValue(requestId);
            }
            verify(observer1).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo(operation2);
            assertThat(Tracer.hasTraceId()).isFalse();
        } finally {
            detached.complete();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetached_metadata_map() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        DetachedSpan detached = DetachedSpan.start("operation");
        detached.complete(ImmutableMap.of("foo", "bar"));
        Tracer.unsubscribe("1");
        verify(observer1).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getMetadata()).containsEntry("foo", "bar");
    }

    @Test
    public void testDetached_metadata_translator() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        DetachedSpan detached = DetachedSpan.start("operation");
        detached.complete(
                new TagTranslator<String>() {
                    @Override
                    public <T> void translate(TagAdapter<T> adapter, T target, String data) {
                        adapter.tag(target, "foo", data);
                    }
                },
                "bar");
        Tracer.unsubscribe("1");
        verify(observer1).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getMetadata()).containsEntry("foo", "bar");
    }

    @Test
    public void testDetachedTraceAppliedToThreadState_metadata_map() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        String operation1 = "operation";
        String operation2 = "attached";
        DetachedSpan detached = DetachedSpan.start(operation1);
        try {
            assertThat(Tracer.hasTraceId()).isFalse();
            try (CloseableSpan ignored = detached.childSpan(operation2, ImmutableMap.of("foo", "bar"))) {
                assertThat(Tracer.hasTraceId()).isTrue();
            }
            verify(observer1).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.getOperation()).isEqualTo(operation2);
            assertThat(Tracer.hasTraceId()).isFalse();
            assertThat(span.getMetadata()).containsEntry("foo", "bar");
        } finally {
            detached.complete();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetachedTraceAppliedToThreadState_metadata_translator() {
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.subscribe("1", observer1);
        String operation1 = "operation";
        String operation2 = "attached";
        DetachedSpan detached = DetachedSpan.start(operation1);
        try {
            assertThat(Tracer.hasTraceId()).isFalse();
            try (CloseableSpan ignored = detached.childSpan(
                    operation2,
                    new TagTranslator<String>() {
                        @Override
                        public <T> void translate(TagAdapter<T> adapter, T target, String data) {
                            adapter.tag(target, "foo", data);
                        }
                    },
                    "bar")) {
                assertThat(Tracer.hasTraceId()).isTrue();
            }
            verify(observer1).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.getOperation()).isEqualTo(operation2);
            assertThat(Tracer.hasTraceId()).isFalse();
            assertThat(span.getMetadata()).containsEntry("foo", "bar");
        } finally {
            detached.complete();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetachedTraceRestoresTrace() {
        assertThat(Tracer.hasTraceId()).isFalse();
        DetachedSpan detached = DetachedSpan.start("detached");
        Tracer.fastStartSpan("standard");
        String standardTraceId = Tracer.getTraceId();
        try (CloseableSpan ignored = detached.childSpan("operation")) {
            assertThat(Tracer.getTraceId())
                    .describedAs("The detached span should have a different trace")
                    .isNotEqualTo(standardTraceId);
        }
        assertThat(Tracer.getTraceId())
                .describedAs("The detached span restores the original trace")
                .isEqualTo(standardTraceId);
        Tracer.fastCompleteSpan();
        detached.complete();
        assertThat(Tracer.hasTraceId()).isFalse();
    }

    @Test
    public void testDetachedTraceRestoresOriginalTrace() {
        Tracer.setSampler(sampler);
        Tracer.subscribe("1", observer1);
        when(sampler.sample()).thenReturn(true);
        assertThat(Tracer.hasTraceId()).isFalse();
        try (CloseableTracer outer = CloseableTracer.startSpan("root")) {
            assertThat(Tracer.hasTraceId()).isTrue();
            DetachedSpan.start("detached").complete();
            assertThat(Tracer.hasTraceId()).isTrue();
        }
        assertThat(Tracer.hasTraceId()).isFalse();
        verify(sampler).sample();
        verify(observer1, times(2)).consume(any(Span.class));
        verifyNoMoreInteractions(observer1, sampler);
        Tracer.unsubscribe("1");
    }

    @Test
    public void testDetachedTraceBuildsUponExistingTrace() {
        Tracer.setSampler(sampler);
        when(sampler.sample()).thenReturn(true);
        Tracer.subscribe("1", observer1);
        assertThat(Tracer.hasTraceId()).isFalse();
        // Standard span starts first, so the detached tracer should build from the current threads state.
        Tracer.fastStartSpan("standard");
        DetachedSpan detached = DetachedSpan.start("detached");
        String standardTraceId = Tracer.getTraceId();
        try (CloseableSpan ignored = detached.childSpan("operation")) {
            assertThat(Tracer.getTraceId()).isEqualTo(standardTraceId);
        }
        assertThat(Tracer.getTraceId()).isEqualTo(standardTraceId);
        Tracer.fastCompleteSpan();
        assertThat(Tracer.hasTraceId()).isFalse();
        detached.complete();
        assertThat(Tracer.hasTraceId()).isFalse();
        verify(sampler, times(1)).sample();
        ArgumentCaptor<Span> captor = ArgumentCaptor.forClass(Span.class);
        verify(observer1, times(3)).consume(captor.capture());
        List<Span> spans = captor.getAllValues();
        Span standardSpan = spans.get(1);
        Span operationSpan = spans.get(0);
        Span detachedSpan = spans.get(2);
        assertThat(standardSpan.getOperation()).isEqualTo("standard");
        assertThat(standardSpan.getParentSpanId()).isEmpty();
        assertThat(operationSpan.getOperation()).isEqualTo("operation");
        assertThat(operationSpan.getParentSpanId()).hasValue(detachedSpan.getSpanId());
        assertThat(detachedSpan.getOperation()).isEqualTo("detached");
        assertThat(detachedSpan.getParentSpanId()).hasValue(standardSpan.getSpanId());
        Tracer.unsubscribe("1");
    }

    @Test
    public void testNewDetachedTrace() {
        try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
            String currentTraceId = Tracer.getTraceId();
            DetachedSpan span =
                    DetachedSpan.start(Observability.SAMPLE, "12345", Optional.empty(), "op", SpanType.LOCAL);
            try (CloseableSpan ignored2 = span.completeAndStartChild("foo")) {
                assertThat(Tracer.getTraceId()).isEqualTo("12345");
            }
            assertThat(Tracer.getTraceId())
                    .as("Current thread state should not be modified")
                    .isEqualTo(currentTraceId);
        }
    }

    @Test
    public void testNewDetachedTrace_doesNotModifyCurrentState() {
        try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
            String currentTraceId = Tracer.getTraceId();
            DetachedSpan span =
                    DetachedSpan.start(Observability.SAMPLE, "12345", Optional.empty(), "op", SpanType.LOCAL);
            assertThat(Tracer.getTraceId())
                    .as("Current thread state should not be modified")
                    .isEqualTo(currentTraceId);
            span.complete();
        }
    }

    private static void startAndFastCompleteSpan() {
        Tracer.fastStartSpan("operation");
        Tracer.fastCompleteSpan();
    }

    private static Span startAndCompleteSpan() {
        Tracer.fastStartSpan("operation");
        return Tracer.completeSpan().get();
    }
}
