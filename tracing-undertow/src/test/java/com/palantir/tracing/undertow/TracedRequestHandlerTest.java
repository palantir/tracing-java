/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.util.HttpString;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TracedRequestHandlerTest {

    private static final AtomicInteger portSelector = new AtomicInteger(4439);

    private int port;
    private Undertow server;

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Mock
    private SpanObserver observer;

    @Mock
    private TraceSampler traceSampler;

    private CountDownLatch traceReportedLatch;

    @Before
    public void before() {
        Tracer.subscribe("TracedRequestHandlerTest", observer);
        Tracer.setSampler(traceSampler);

        traceReportedLatch = new CountDownLatch(1);
        port = portSelector.incrementAndGet();
        HttpHandler nextHandler = new TracedRequestHandler(exchange -> exchange.getResponseHeaders()
                .put(HttpString.tryFromString("requestId"), exchange.getAttachment(TracingAttachments.REQUEST_ID)));
        server = Undertow.builder()
                .addHttpListener(port, null)
                .setHandler(exchange -> {
                    exchange.addExchangeCompleteListener((_exc, nextListener) -> {
                        traceReportedLatch.countDown();
                        nextListener.proceed();
                    });
                    nextHandler.handleRequest(exchange);
                })
                .build();
        server.start();
    }

    @After
    public void after() {
        Tracer.unsubscribe("TracedRequestHandlerTest");
        server.stop();
    }

    private HttpURLConnection connection() throws IOException {
        return (HttpURLConnection) new URL("http://localhost:" + port).openConnection();
    }

    @Test
    public void testRequestTracing_sampled() throws Exception {
        HttpURLConnection con = connection();
        con.setRequestProperty(TraceHttpHeaders.IS_SAMPLED, "1");
        con.setRequestProperty(TraceHttpHeaders.TRACE_ID, "1234");
        assertThat(con.getResponseCode()).isEqualTo(200);
        assertThat(con.getHeaderField(TraceHttpHeaders.TRACE_ID)).isEqualTo("1234");
        assertThat(con.getHeaderField("requestId")).isNotEmpty();
        assertThat(traceReportedLatch.await(5, TimeUnit.SECONDS)).isTrue();
        verifyNoMoreInteractions(traceSampler);
        verify(observer).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("Undertow Request");
        assertThat(span.getTraceId()).isEqualTo("1234");
        assertThat(span.getMetadata()).containsEntry("status", Integer.toString(con.getResponseCode()));
    }

    @Test
    public void testRequestTracing_unsampled() throws Exception {
        HttpURLConnection con = connection();
        con.setRequestProperty(TraceHttpHeaders.IS_SAMPLED, "0");
        con.setRequestProperty(TraceHttpHeaders.TRACE_ID, "1234");
        assertThat(con.getResponseCode()).isEqualTo(200);
        assertThat(con.getHeaderField(TraceHttpHeaders.TRACE_ID)).isEqualTo("1234");
        assertThat(traceReportedLatch.await(5, TimeUnit.SECONDS)).isTrue();
        verifyNoMoreInteractions(traceSampler);
        verifyNoMoreInteractions(observer);
    }

    @Test
    public void testRequestTracing_sampledWithoutTraceId() throws Exception {
        HttpURLConnection con = connection();
        con.setRequestProperty(TraceHttpHeaders.IS_SAMPLED, "1");
        assertThat(con.getResponseCode()).isEqualTo(200);
        String reportedTraceId = con.getHeaderField(TraceHttpHeaders.TRACE_ID);
        assertThat(traceReportedLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(reportedTraceId).isNotEmpty();
        verifyNoMoreInteractions(traceSampler);
        verify(observer).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("Undertow Request");
        assertThat(span.getTraceId()).isEqualTo(reportedTraceId);
        assertThat(span.getMetadata()).containsEntry("status", Integer.toString(con.getResponseCode()));
    }

    @Test
    public void testRequestTracing_noTracingInformation() throws Exception {
        HttpURLConnection con = connection();
        when(traceSampler.sample()).thenReturn(true);
        assertThat(con.getResponseCode()).isEqualTo(200);
        String reportedTraceId = con.getHeaderField(TraceHttpHeaders.TRACE_ID);
        assertThat(traceReportedLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(reportedTraceId).isNotEmpty();
        verify(traceSampler).sample();
        verifyNoMoreInteractions(traceSampler);
        verify(observer).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.getOperation()).isEqualTo("Undertow Request");
        assertThat(span.getTraceId()).isEqualTo(reportedTraceId);
        assertThat(span.getMetadata()).containsEntry("status", Integer.toString(con.getResponseCode()));
    }
}
