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

package com.palantir.tracing.jersey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.tracing.InternalTracers;
import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import com.palantir.tracing.api.TraceTags;
import com.palantir.undertest.UndertowServerExtension;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public final class TraceEnrichingFilterTest {

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Mock
    private SpanObserver observer;

    @Mock
    private ContainerRequestContext request;

    @Mock
    private TraceSampler traceSampler;

    @BeforeEach
    void before() {
        Tracer.subscribe("", observer);
        Tracer.setSampler(traceSampler);

        MDC.clear();

        when(request.getMethod()).thenReturn("GET");
        when(traceSampler.sample()).thenReturn(true);
    }

    @AfterEach
    void after() {
        Tracer.unsubscribe("");
    }

    @Test
    public void testTraceState_withHeaderUsesTraceId() {
        HttpGet get = new HttpGet("/trace");
        get.addHeader(TraceHttpHeaders.TRACE_ID, "traceId");
        get.addHeader(TraceHttpHeaders.SPAN_ID, "spanId");
        undertow.runRequest(get, response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID).getValue())
                    .isEqualTo("traceId");
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace");
        });
    }

    @Test
    public void testTraceState_respectsMethod() {
        HttpPost post = new HttpPost("/trace");
        post.addHeader(TraceHttpHeaders.TRACE_ID, "traceId");
        post.addHeader(TraceHttpHeaders.SPAN_ID, "spanId");
        post.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));
        undertow.runRequest(post, response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID).getValue())
                    .isEqualTo("traceId");
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: POST /trace");
        });
    }

    @Test
    public void testTraceState_doesNotIncludePathParams() {
        HttpGet get = new HttpGet("/trace/no");
        get.addHeader(TraceHttpHeaders.TRACE_ID, "traceId");
        get.addHeader(TraceHttpHeaders.SPAN_ID, "spanId");
        undertow.runRequest(get, response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID).getValue())
                    .isEqualTo("traceId");
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace/{param}");
        });
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeaders() {
        undertow.runRequest(new HttpGet("/trace"), response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.getOperation()).isEqualTo("Jersey: GET /trace");
            assertThat(span.getMetadata())
                    .containsEntry(TraceTags.HTTP_STATUS_CODE, Integer.toString(response.getCode()));
        });
    }

    @Test
    public void testTraceState_setsResponseStatus() {
        undertow.runRequest(new HttpPost("/trace"), response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.getOperation()).isEqualTo("Jersey: POST /trace");
            assertThat(span.getMetadata())
                    .containsEntry(TraceTags.HTTP_STATUS_CODE, Integer.toString(response.getCode()))
                    .containsEntry(TraceTags.HTTP_URL_PATH_TEMPLATE, "/trace")
                    .containsEntry(TraceTags.HTTP_METHOD, "POST")
                    .containsKey(TraceTags.HTTP_REQUEST_ID);
        });
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenFailing() {
        undertow.runRequest(new HttpGet("/failing-trace"), response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.getOperation()).isEqualTo("Jersey: GET /failing-trace");
            assertThat(span.getMetadata())
                    .containsEntry(TraceTags.HTTP_STATUS_CODE, Integer.toString(response.getCode()));
        });
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenStreaming() {
        undertow.runRequest(new HttpGet("/streaming-trace"), response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /streaming-trace");
        });
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenFailingToStream() {
        undertow.runRequest(new HttpGet("/failing-streaming-trace"), response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /failing-streaming-trace");
        });
    }

    @Test
    public void testTraceState_withSamplingHeaderWithoutTraceIdDoesNotUseTraceSampler() {
        HttpGet notSampled = new HttpGet("/trace");
        notSampled.setHeader(TraceHttpHeaders.IS_SAMPLED, "0");

        HttpGet sampled = new HttpGet("/trace");
        sampled.setHeader(TraceHttpHeaders.IS_SAMPLED, "1");
        undertow.runRequest(notSampled, _response -> {
            verify(traceSampler, never()).sample();
        });

        undertow.runRequest(sampled, _response -> {
            verify(traceSampler, never()).sample();
        });

        undertow.runRequest(new HttpGet("/trace"), _response -> {
            verify(traceSampler, times(1)).sample();
        });
    }

    @Test
    public void testTraceState_withEmptyTraceIdGeneratesValidTraceResponseHeaders() {
        HttpGet get = new HttpGet("/trace");
        get.addHeader(TraceHttpHeaders.TRACE_ID, "");
        undertow.runRequest(get, response -> {
            assertThat(response.getFirstHeader(TraceHttpHeaders.TRACE_ID)).isNotNull();
            assertThat(response.getFirstHeader(TraceHttpHeaders.SPAN_ID)).isNull();
            verify(observer).consume(spanCaptor.capture());
            assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace");
        });
    }

    @Test
    public void testFilter_setsMdcIfTraceIdHeaderIsPresent() throws Exception {
        when(request.getHeaderString(TraceHttpHeaders.TRACE_ID)).thenReturn("traceId");
        TraceEnrichingFilter.INSTANCE.filter(request);
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo("traceId");
        verify(request).setProperty(TraceEnrichingFilter.TRACE_ID_PROPERTY_NAME, "traceId");
        // Note: this will be set to a random value; we want to check whether the value is being set
        verify(request).setProperty(eq(TraceEnrichingFilter.SAMPLED_PROPERTY_NAME), anyBoolean());
        verify(request).setProperty(eq(TraceEnrichingFilter.REQUEST_ID_PROPERTY_NAME), anyString());
    }

    @Test
    public void testFilter_setsUserAgentAsForUserAgent() throws Exception {
        when(request.getHeaderString(TraceHttpHeaders.TRACE_ID)).thenReturn("traceId");
        when(request.getHeaderString(HttpHeaders.USER_AGENT)).thenReturn("userAgent");
        TraceEnrichingFilter.INSTANCE.filter(request);

        assertThat(InternalTracers.getForUserAgent()).contains("userAgent");
    }

    @Test
    public void testFilter_setsFetchUserAgentAsForUserAgent() throws Exception {
        when(request.getHeaderString(TraceHttpHeaders.TRACE_ID)).thenReturn("traceId");
        when(request.getHeaderString(TraceEnrichingFilter.FETCH_USER_AGENT_HEADER))
                .thenReturn("fetchUserAgent");
        TraceEnrichingFilter.INSTANCE.filter(request);

        assertThat(InternalTracers.getForUserAgent()).contains("fetchUserAgent");
    }

    @Test
    public void testFilter_propagatesProvidedForUserAgent() throws Exception {
        when(request.getHeaderString(TraceHttpHeaders.TRACE_ID)).thenReturn("traceId");
        when(request.getHeaderString(TraceHttpHeaders.FOR_USER_AGENT)).thenReturn("forUserAgent");
        TraceEnrichingFilter.INSTANCE.filter(request);

        assertThat(InternalTracers.getForUserAgent()).contains("forUserAgent");
    }

    @Test
    public void testFilter_createsReceiveAndSendEvents() throws Exception {
        HttpGet get = new HttpGet("/trace");
        get.addHeader(TraceHttpHeaders.TRACE_ID, "");
        undertow.runRequest(get, _ignore -> {
            verify(observer).consume(spanCaptor.capture());
            Span span = spanCaptor.getValue();
            assertThat(span.type()).isEqualTo(SpanType.SERVER_INCOMING);
        });
    }

    @Test
    public void testFilter_setsMdcIfTraceIdHeaderIsNotPresent() throws Exception {
        TraceEnrichingFilter.INSTANCE.filter(request);
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).hasSize(16);
        verify(request).setProperty(eq(TraceEnrichingFilter.TRACE_ID_PROPERTY_NAME), anyString());
        verify(request).setProperty(eq(TraceEnrichingFilter.REQUEST_ID_PROPERTY_NAME), anyString());
    }

    @RegisterExtension
    public static final UndertowServerExtension undertow =
            UndertowServerExtension.create().jersey(new TraceEnrichingFilter()).jersey(new TracingTestResource());

    public static final class TracingTestResource implements TracingTestService {
        @Override
        public void getTraceOperation() {}

        @Override
        public void postTraceOperation() {}

        @Override
        public void getTraceWithPathParam() {}

        @Override
        public void getFailingTraceOperation() {
            throw new RuntimeException();
        }

        @Override
        public StreamingOutput getFailingStreamingTraceOperation() {
            return _os -> {
                throw new RuntimeException();
            };
        }

        @Override
        public StreamingOutput getStreamingTraceOperation() {
            return _os -> {};
        }
    }

    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public interface TracingTestService {
        @GET
        @Path("/trace")
        void getTraceOperation();

        @POST
        @Path("/trace")
        void postTraceOperation();

        @GET
        @Path("/trace/{param}")
        void getTraceWithPathParam();

        @GET
        @Path("/failing-trace")
        void getFailingTraceOperation();

        @GET
        @Path("/failing-streaming-trace")
        @Produces(MediaType.APPLICATION_OCTET_STREAM)
        StreamingOutput getFailingStreamingTraceOperation();

        @GET
        @Path("/streaming-trace")
        @Produces(MediaType.APPLICATION_OCTET_STREAM)
        StreamingOutput getStreamingTraceOperation();
    }
}
