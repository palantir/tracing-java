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

package com.palantir.tracing.okhttp3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import java.io.IOException;
import okhttp3.Interceptor;
import okhttp3.Request;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("deprecation")
public final class OkhttpTraceInterceptorTest {

    @Mock
    private Interceptor.Chain chain;

    @Mock
    private SpanObserver observer;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Before
    public void before() {
        Request request = new Request.Builder().url("http://localhost").build();
        when(chain.request()).thenReturn(request);
        Tracer.subscribe("", observer);
    }

    @After
    public void after() {
        Tracer.initTrace(Observability.SAMPLE, Tracers.randomId());
        Tracer.unsubscribe("");
    }

    @Test
    public void testPopulatesNewTrace_whenNoTraceIsPresentInGlobalState() throws IOException {
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(chain).request();
        verify(chain).proceed(requestCaptor.capture());
        verifyNoMoreInteractions(chain);

        Request intercepted = requestCaptor.getValue();
        assertThat(intercepted.headers(TraceHttpHeaders.SPAN_ID)).hasSize(1);
        assertThat(intercepted.headers(TraceHttpHeaders.TRACE_ID)).hasSize(1);
        assertThat(intercepted.headers(TraceHttpHeaders.ORIGINATING_SPAN_ID)).isEmpty();
        assertThat(intercepted.headers(TraceHttpHeaders.PARENT_SPAN_ID)).isEmpty();
    }

    @Test
    public void testPopulatesNewTrace_whenParentTraceIsPresent() throws IOException {
        String originatingSpanId = "originating Span";
        OpenSpan parentState = Tracer.startSpan("operation", originatingSpanId, SpanType.SERVER_INCOMING);
        String traceId = Tracer.getTraceId();
        try {
            OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        } finally {
            Tracer.fastCompleteSpan();
        }

        verify(chain).request();
        verify(chain).proceed(requestCaptor.capture());
        verifyNoMoreInteractions(chain);

        Request intercepted = requestCaptor.getValue();
        assertThat(intercepted.headers(TraceHttpHeaders.SPAN_ID)).isNotNull();
        assertThat(intercepted.headers(TraceHttpHeaders.SPAN_ID)).doesNotContain(parentState.getSpanId());
        assertThat(intercepted.headers(TraceHttpHeaders.TRACE_ID)).containsOnly(traceId);
        assertThat(intercepted.headers(TraceHttpHeaders.PARENT_SPAN_ID)).containsOnly(parentState.getSpanId());
        assertThat(intercepted.headers(TraceHttpHeaders.ORIGINATING_SPAN_ID)).containsOnly(originatingSpanId);
    }

    @Test
    public void testAddsIsSampledHeader_whenTraceIsObservable() throws IOException {
        Tracer.initTrace(Observability.SAMPLE, Tracers.randomId());
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(chain).proceed(requestCaptor.capture());
        assertThat(requestCaptor.getValue().headers(TraceHttpHeaders.IS_SAMPLED))
                .containsOnly("1");
    }

    @Test
    public void testHeaders_whenTraceIsNotObservable() throws IOException {
        Tracer.initTrace(Observability.DO_NOT_SAMPLE, Tracers.randomId());
        String traceId = Tracer.getTraceId();
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(chain).proceed(requestCaptor.capture());
        Request intercepted = requestCaptor.getValue();
        assertThat(intercepted.headers(TraceHttpHeaders.SPAN_ID)).hasSize(1);
        assertThat(intercepted.headers(TraceHttpHeaders.TRACE_ID)).containsOnly(traceId);
        assertThat(intercepted.headers(TraceHttpHeaders.IS_SAMPLED)).containsOnly("0");
    }

    @Test
    public void testPopsSpan() throws IOException {
        OpenSpan before = Tracer.startSpan("");
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        assertThat(Tracer.startSpan("").getParentSpanId().get()).isEqualTo(before.getSpanId());
    }

    @Test
    public void testPopsSpanEvenWhenChainFails() throws IOException {
        OpenSpan before = Tracer.startSpan("op");
        when(chain.proceed(any(Request.class))).thenThrow(new IllegalStateException());
        try {
            OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        } catch (IllegalStateException e) {
            /* expected */
        }
        assertThat(Tracer.startSpan("").getParentSpanId().get()).isEqualTo(before.getSpanId());
    }

    @Test
    public void testCompletesSpan() throws Exception {
        OpenSpan outerSpan = Tracer.startSpan("outer");
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(observer).consume(spanCaptor.capture());
        Span okhttpSpan = spanCaptor.getValue();
        assertThat(okhttpSpan).isNotEqualTo(outerSpan);
    }

    @Test
    public void testSpanHasClientType() throws Exception {
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(observer).consume(spanCaptor.capture());
        Span okhttpSpan = spanCaptor.getValue();
        assertThat(okhttpSpan.type()).isEqualTo(SpanType.CLIENT_OUTGOING);
    }

    @Test
    public void testHeaders_noMultiValue() throws IOException {
        Request request = new Request.Builder()
                .url("http://localhost")
                .header(TraceHttpHeaders.SPAN_ID, "existingSpan")
                .header(TraceHttpHeaders.TRACE_ID, "existingTraceId")
                .header(TraceHttpHeaders.IS_SAMPLED, "existingSampled")
                .build();
        when(chain.request()).thenReturn(request);

        Tracer.initTrace(Observability.SAMPLE, Tracers.randomId());
        OkhttpTraceInterceptor.INSTANCE.intercept(chain);
        verify(chain).proceed(requestCaptor.capture());

        assertThat(requestCaptor.getValue().headers(TraceHttpHeaders.SPAN_ID)).hasSize(1);
        assertThat(requestCaptor.getValue().headers(TraceHttpHeaders.TRACE_ID)).hasSize(1);
        assertThat(requestCaptor.getValue().headers(TraceHttpHeaders.IS_SAMPLED))
                .containsOnly("1");
    }
}
