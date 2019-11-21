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

import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.junit.DropwizardAppRule;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;

public final class TraceEnrichingFilterTest {

    @ClassRule
    public static final DropwizardAppRule<Configuration> APP =
            new DropwizardAppRule<>(TracingTestServer.class, "src/test/resources/test-server.yml");

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    @Mock
    private SpanObserver observer;
    @Mock
    private ContainerRequestContext request;
    @Mock
    private UriInfo uriInfo;
    @Mock
    private TraceSampler traceSampler;

    private WebTarget target;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        String endpointUri = "http://localhost:" + APP.getLocalPort();
        JerseyClientBuilder builder = new JerseyClientBuilder();
        Client client = builder.build();
        target = client.target(endpointUri);
        Tracer.subscribe("", observer);
        Tracer.setSampler(traceSampler);

        MDC.clear();

        when(request.getMethod()).thenReturn("GET");
        when(uriInfo.getPath()).thenReturn("/foo");
        when(request.getUriInfo()).thenReturn(uriInfo);
        when(traceSampler.sample()).thenReturn(true);
    }

    @After
    public void after() {
        Tracer.unsubscribe("");
    }

    @Test
    public void testTraceState_withHeaderUsesTraceId() {
        Response response = target.path("/trace").request()
                .header(TraceHttpHeaders.TRACE_ID, "traceId")
                .header(TraceHttpHeaders.PARENT_SPAN_ID, "parentSpanId")
                .header(TraceHttpHeaders.SPAN_ID, "spanId")
                .get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isEqualTo("traceId");
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace");
    }

    @Test
    public void testTraceState_withHeaderUsesTraceIdWithDifferentLocalIds() {
        Response response = target.path("/top-span").request()
                .header(TraceHttpHeaders.TRACE_ID, "traceId")
                .header(TraceHttpHeaders.PARENT_SPAN_ID, "parentSpanId")
                .header(TraceHttpHeaders.SPAN_ID, "spanId")
                .get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isEqualTo("traceId");
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /top-span");

        String firstLocalTrace = response.readEntity(String.class);
        assertThat(firstLocalTrace).isNotEmpty();

        // make the query again
        Response otherResponse = target.path("/top-span").request()
                .header(TraceHttpHeaders.TRACE_ID, "traceId")
                .header(TraceHttpHeaders.PARENT_SPAN_ID, "parentSpanId")
                .header(TraceHttpHeaders.SPAN_ID, "spanId")
                .get();
        assertThat(otherResponse.getHeaderString(TraceHttpHeaders.TRACE_ID)).isEqualTo("traceId");
        assertThat(otherResponse.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(otherResponse.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        String otherLocalTrace = otherResponse.readEntity(String.class);
        assertThat(otherLocalTrace).isNotEmpty();
        assertThat(otherLocalTrace).isNotEqualTo(firstLocalTrace);
    }

    @Test
    public void testTraceState_respectsMethod() {
        Response response = target.path("/trace").request()
                .header(TraceHttpHeaders.TRACE_ID, "traceId")
                .header(TraceHttpHeaders.PARENT_SPAN_ID, "parentSpanId")
                .header(TraceHttpHeaders.SPAN_ID, "spanId")
                .post(Entity.json(""));
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isEqualTo("traceId");
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: POST /trace");
    }

    @Test
    public void testTraceState_doesNotIncludePathParams() {
        Response response = target.path("/trace/no").request()
                .header(TraceHttpHeaders.TRACE_ID, "traceId")
                .header(TraceHttpHeaders.PARENT_SPAN_ID, "parentSpanId")
                .header(TraceHttpHeaders.SPAN_ID, "spanId")
                .get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isEqualTo("traceId");
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace/{param}");
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeaders() {
        Response response = target.path("/trace").request().get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isNotNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace");
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenFailing() {
        Response response = target.path("/failing-trace").request().get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isNotNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /failing-trace");
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenStreaming() {
        Response response = target.path("/streaming-trace").request().get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isNotNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /streaming-trace");
    }

    @Test
    public void testTraceState_withoutRequestHeadersGeneratesValidTraceResponseHeadersWhenFailingToStream() {
        Response response = target.path("/failing-streaming-trace").request().get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isNotNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /failing-streaming-trace");
    }

    @Test
    public void testTraceState_withSamplingHeaderWithoutTraceIdDoesNotUseTraceSampler() {
        target.path("/trace").request()
                .header(TraceHttpHeaders.IS_SAMPLED, "0")
                .get();
        verify(traceSampler, never()).sample();

        target.path("/trace").request()
                .header(TraceHttpHeaders.IS_SAMPLED, "1")
                .get();
        verify(traceSampler, never()).sample();

        target.path("/trace").request()
                .get();
        verify(traceSampler, times(1)).sample();
    }

    @Test
    public void testTraceState_withEmptyTraceIdGeneratesValidTraceResponseHeaders() {
        Response response = target.path("/trace").request().header(TraceHttpHeaders.TRACE_ID, "").get();
        assertThat(response.getHeaderString(TraceHttpHeaders.TRACE_ID)).isNotNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.PARENT_SPAN_ID)).isNull();
        assertThat(response.getHeaderString(TraceHttpHeaders.SPAN_ID)).isNull();
        verify(observer).consume(spanCaptor.capture());
        assertThat(spanCaptor.getValue().getOperation()).isEqualTo("Jersey: GET /trace");
    }

    @Test
    public void testFilter_setsMdcIfTraceIdHeaderIsPresent() throws Exception {
        when(request.getHeaderString(TraceHttpHeaders.TRACE_ID)).thenReturn("traceId");
        TraceEnrichingFilter.INSTANCE.filter(request);
        assertThat(MDC.get(Tracers.TRACE_ID_KEY)).isEqualTo("traceId");
        verify(request).setProperty(TraceEnrichingFilter.TRACE_ID_PROPERTY_NAME, "traceId");
        // the top span id is randomly generated
        verify(request).setProperty(eq(TraceEnrichingFilter.TOP_SPAN_ID_PROPERTY_NAME), anyString());
        // Note: this will be set to a random value; we want to check whether the value is being set
        verify(request).setProperty(eq(TraceEnrichingFilter.SAMPLED_PROPERTY_NAME), anyBoolean());
    }

    @Test
    public void testFilter_createsReceiveAndSendEvents() throws Exception {
        target.path("/trace").request().header(TraceHttpHeaders.TRACE_ID, "").get();
        verify(observer).consume(spanCaptor.capture());
        Span span = spanCaptor.getValue();
        assertThat(span.type()).isEqualTo(SpanType.SERVER_INCOMING);
    }

    @Test
    public void testFilter_setsMdcIfTraceIdHeaderIsNotePresent() throws Exception {
        TraceEnrichingFilter.INSTANCE.filter(request);
        assertThat(MDC.get(Tracers.TRACE_ID_KEY).length()).isEqualTo(16);
        verify(request).setProperty(eq(TraceEnrichingFilter.TRACE_ID_PROPERTY_NAME), anyString());
    }

    public static class TracingTestServer extends Application<Configuration> {
        @Override
        public final void run(Configuration _value, final Environment env) throws Exception {
            env.jersey().register(new TraceEnrichingFilter());
            env.jersey().register(new TracingTestResource());
        }
    }

    public static final class TracingTestResource implements TracingTestService {
        @Override
        public void getTraceOperation() {
            throw new RuntimeException("FAIL");
        }

        @Override
        public void postTraceOperation() {}

        @Override
        public void getTraceWithPathParam() {}

        @Override
        public void getFailingTraceOperation() {
            throw new RuntimeException();
        }

        @Override
        public String getOutermostSpanId() {
            return Tracer.getOutermostSpanId().get();
        }

        @Override
        public StreamingOutput getFailingStreamingTraceOperation() {
            return os -> {
                throw new RuntimeException();
            };
        }

        @Override
        public StreamingOutput getStreamingTraceOperation() {
            return os -> {
            };
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
        @Path("/top-span")
        String getOutermostSpanId();

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
