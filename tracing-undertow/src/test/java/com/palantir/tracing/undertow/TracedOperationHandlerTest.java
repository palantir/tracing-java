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

package com.palantir.tracing.undertow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.InternalTracers;
import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.MDC;

@RunWith(MockitoJUnitRunner.class)
public class TracedOperationHandlerTest {

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Mock
    private SpanObserver observer;

    @Mock
    private TraceSampler traceSampler;

    @Mock
    private HttpHandler delegate;

    private HttpServerExchange exchange = HttpServerExchanges.createStub();
    private String traceId;

    private TracedOperationHandler handler;

    @Before
    public void before() {
        Tracer.subscribe("TEST_OBSERVER", observer);
        Tracer.setSampler(traceSampler);

        MDC.clear();

        exchange.setRequestMethod(HttpString.tryFromString("GET"));
        when(traceSampler.sample()).thenReturn(true);

        traceId = Tracers.randomId();
        handler = new TracedOperationHandler(delegate, "GET /foo");
    }

    @After
    public void after() {
        Tracer.unsubscribe("TEST_OBSERVER");
    }

    @Test
    public void whenNoTraceIsInHeader_generatesNewTrace() throws Exception {
        handler.handleRequest(exchange);
        verify(observer).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();

        assertThat(Tracer.hasTraceId()).isFalse();
        assertThat(span.getOperation()).isEqualTo("Undertow: GET /foo");
        HeaderMap responseHeaders = exchange.getResponseHeaders();
        assertThat(responseHeaders.get(TraceHttpHeaders.SPAN_ID)).isNull();
        assertThat(responseHeaders.get(HttpString.tryFromString(TraceHttpHeaders.TRACE_ID)))
                .containsExactly(span.getTraceId());
    }

    @Test
    public void whenTraceIsInHeader_usesGivenTraceId() throws Exception {
        setRequestTraceId(traceId);
        handler.handleRequest(exchange);
        assertThat(exchange.getResponseHeaders().getFirst(TraceHttpHeaders.TRACE_ID))
                .isEqualTo(traceId);
    }

    @Test
    public void whenParentSpanIsGiven_usesParentSpan() throws Exception {
        setRequestTraceId(traceId);
        String parentSpanId = Tracers.randomId();
        setRequestSpanId(parentSpanId);

        handler.handleRequest(exchange);
        // Since we're not running a full request, the completion handler cannot execute normally.
        exchange.getAttachment(TracingAttachments.REQUEST_SPAN).complete();
        verify(observer, times(2)).consume(spanCaptor.capture());
        List<Span> spans = spanCaptor.getAllValues();
        assertThat(spans).hasSize(2);
        Span span = spans.get(1);
        assertThat(spans.get(0).getParentSpanId()).hasValue(span.getSpanId());
        assertThat(span.getParentSpanId()).hasValue(parentSpanId);
        assertThat(span.getSpanId()).isNotEqualTo(parentSpanId);
    }

    @Test
    public void whenOnlyUserAgentIsProvided_setsItAsForUserAgent() throws Exception {
        setRequestTraceId(traceId);
        setUserAgent("userAgent");
        handler.handleRequest(exchange);

        DetachedSpan detachedSpan = exchange.getAttachment(TracingAttachments.REQUEST_SPAN);
        assertThat(InternalTracers.getForUserAgent(detachedSpan)).contains("userAgent");
    }

    @Test
    public void whenFetchUserAgentIsProvided_setsItAsForUserAgent() throws Exception {
        setRequestTraceId(traceId);
        setUserAgent("userAgent");
        setFetchUserAgent("fetchUserAgent");
        handler.handleRequest(exchange);

        DetachedSpan detachedSpan = exchange.getAttachment(TracingAttachments.REQUEST_SPAN);
        assertThat(InternalTracers.getForUserAgent(detachedSpan)).contains("fetchUserAgent");
    }

    @Test
    public void whenForUserAgentIsProvided_propagateItFurther() throws Exception {
        setRequestTraceId(traceId);
        setUserAgent("userAgent");
        setFetchUserAgent("fetchUserAgent");
        setForUserAgent("forUserAgent");
        handler.handleRequest(exchange);

        DetachedSpan detachedSpan = exchange.getAttachment(TracingAttachments.REQUEST_SPAN);
        assertThat(InternalTracers.getForUserAgent(detachedSpan)).contains("forUserAgent");
    }

    @Test
    public void whenTraceIsAlreadySampled_doesNotCallSampler() throws Exception {
        exchange.getRequestHeaders().put(HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED), "1");
        handler.handleRequest(exchange);

        exchange.getRequestHeaders().put(HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED), "0");
        handler.handleRequest(exchange);

        setRequestTraceId(traceId);
        handler.handleRequest(exchange);

        verify(traceSampler, never()).sample();
    }

    @Test
    public void whenTraceIsAlreadySampled_setsAttachment() throws Exception {
        exchange.getRequestHeaders().put(HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED), "1");
        handler.handleRequest(exchange);

        assertThat(exchange.getAttachment(TracingAttachments.IS_SAMPLED)).isTrue();
    }

    @Test
    public void whenTraceIsAlreadyNotSampled_setsAttachment() throws Exception {
        exchange.getRequestHeaders().put(HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED), "0");
        handler.handleRequest(exchange);

        assertThat(exchange.getAttachment(TracingAttachments.IS_SAMPLED)).isFalse();
    }

    @Test
    public void whenSamplingDecisionHasNotBeenMade_callsSampler() throws Exception {
        handler.handleRequest(exchange);
        verify(traceSampler).sample();
    }

    @Test
    public void setsDetachedTrace() throws Exception {
        handler.handleRequest(exchange);
        assertThat(TracingAttachments.requestTrace(exchange)).isNotNull();
        assertThat(exchange.getAttachment(TracingAttachments.REQUEST_DETACHED_TRACE))
                .isNotNull();
    }

    @Test
    public void completesSpanEvenIfDelegateThrows() throws Exception {
        doThrow(new RuntimeException()).when(delegate).handleRequest(any());
        try {
            handler.handleRequest(exchange);
        } catch (RuntimeException e) {
            // expected
        }
        assertThat(Tracer.completeSpan()).isEmpty();
    }

    @Test
    public void populatesSlf4jMdc() throws Exception {
        setRequestTraceId(traceId);
        AtomicReference<String> mdcTraceValue = new AtomicReference<>();
        new TracedOperationHandler(_exc -> mdcTraceValue.set(MDC.get(Tracers.TRACE_ID_KEY)), "GET /traced")
                .handleRequest(exchange);
        assertThat(mdcTraceValue).hasValue(traceId);
        // Value should be cleared when the handler returns
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isNull();
    }

    private void setRequestTraceId(String theTraceId) {
        setHeader(TraceHttpHeaders.TRACE_ID, theTraceId);
    }

    private void setRequestSpanId(String spanId) {
        setHeader(TraceHttpHeaders.SPAN_ID, spanId);
    }

    private void setUserAgent(String userAgent) {
        setHeader(Headers.USER_AGENT, userAgent);
    }

    private void setFetchUserAgent(String fetchUserAgent) {
        setHeader(UndertowTracing.FETCH_USER_AGENT, fetchUserAgent);
    }

    private void setForUserAgent(String forUserAgent) {
        setHeader(TraceHttpHeaders.FOR_USER_AGENT, forUserAgent);
    }

    private void setHeader(String headerName, String headerValue) {
        setHeader(HttpString.tryFromString(headerName), headerValue);
    }

    private void setHeader(HttpString headerName, String headerValue) {
        exchange.getRequestHeaders().put(headerName, headerValue);
    }
}
