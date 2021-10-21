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

import com.google.common.base.Strings;
import com.palantir.tracing.Observability;
import com.palantir.tracing.TagTranslator;
import com.palantir.tracing.TraceMetadata;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import com.palantir.tracing.api.TraceTags;
import java.io.IOException;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.model.Resource;

// Default is `Priorities.USER` == 5000. This filter needs to execute earlier to ensure traces are ready to use.
@Priority(500)
@Provider
public final class TraceEnrichingFilter implements ContainerRequestFilter, ContainerResponseFilter {
    public static final TraceEnrichingFilter INSTANCE = new TraceEnrichingFilter();

    /**
     * This is the name of the trace id property we set on {@link ContainerRequestContext}.
     */
    public static final String TRACE_ID_PROPERTY_NAME = "com.palantir.tracing.traceId";

    public static final String REQUEST_ID_PROPERTY_NAME = "com.palantir.tracing.requestId";

    public static final String SAMPLED_PROPERTY_NAME = "com.palantir.tracing.sampled";

    @Context
    @SuppressWarnings("NullAway") // instantiated using by Jersey using reflection
    private ExtendedUriInfo uriInfo;

    // Handles incoming request
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String path = getPathTemplate();

        String operation = "Jersey: " + requestContext.getMethod() + " " + path;
        // The following strings are all nullable
        String traceId = requestContext.getHeaderString(TraceHttpHeaders.TRACE_ID);
        String spanId = requestContext.getHeaderString(TraceHttpHeaders.SPAN_ID);
        Optional<String> forUserAgent =
                Optional.ofNullable(requestContext.getHeaderString(TraceHttpHeaders.FOR_USER_AGENT));

        // Set up thread-local span that inherits state from HTTP headers
        if (Strings.isNullOrEmpty(traceId)) {
            // HTTP request did not indicate a trace; initialize trace state and create a span.
            Tracer.initTraceWithSpan(
                    getObservabilityFromHeader(requestContext),
                    Tracers.randomId(),
                    forUserAgent,
                    operation,
                    SpanType.SERVER_INCOMING);
        } else if (spanId == null) {
            Tracer.initTraceWithSpan(
                    getObservabilityFromHeader(requestContext),
                    traceId,
                    forUserAgent,
                    operation,
                    SpanType.SERVER_INCOMING);
        } else {
            // caller's span is this span's parent.
            Tracer.initTraceWithSpan(
                    getObservabilityFromHeader(requestContext),
                    traceId,
                    forUserAgent,
                    operation,
                    spanId,
                    SpanType.SERVER_INCOMING);
        }

        // Give asynchronous downstream handlers access to the trace id
        requestContext.setProperty(TRACE_ID_PROPERTY_NAME, Tracer.getTraceId());
        requestContext.setProperty(SAMPLED_PROPERTY_NAME, Tracer.isTraceObservable());
        Tracer.maybeGetTraceMetadata()
                .flatMap(TraceMetadata::getRequestId)
                .ifPresent(requestId -> requestContext.setProperty(REQUEST_ID_PROPERTY_NAME, requestId));
    }

    // Handles outgoing response
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        MultivaluedMap<String, Object> headers = responseContext.getHeaders();
        if (Tracer.hasTraceId()) {
            String traceId = Tracer.getTraceId();
            Tracer.fastCompleteSpan(FunctionalTagTranslator.INSTANCE, sink -> {
                sink.accept(TraceTags.HTTP_STATUS_CODE, Integer.toString(responseContext.getStatus()));
                sink.accept(TraceTags.HTTP_URL_PATH_TEMPLATE, getPathTemplate());
                sink.accept(TraceTags.HTTP_METHOD, requestContext.getMethod());
                Object requestId = requestContext.getProperty(REQUEST_ID_PROPERTY_NAME);
                if (requestId instanceof String) {
                    sink.accept(TraceTags.HTTP_REQUEST_ID, (String) requestId);
                }
            });
            headers.putSingle(TraceHttpHeaders.TRACE_ID, traceId);
        } else {
            // When the filter is called twice (e.g. an exception is thrown in a streaming call),
            // the current trace will be empty. To allow clients to still get the trace ID corresponding to
            // the failure, we retrieve it from the requestContext.
            Optional.ofNullable(requestContext.getProperty(TRACE_ID_PROPERTY_NAME))
                    .ifPresent(s -> headers.putSingle(TraceHttpHeaders.TRACE_ID, s));
        }
    }

    // Force sample iff the context contains a "1" X-B3-Sampled header, force not sample if the header contains another
    // non-empty value, or undecided if there is no such header or the header is empty.
    private static Observability getObservabilityFromHeader(ContainerRequestContext context) {
        String header = context.getHeaderString(TraceHttpHeaders.IS_SAMPLED);
        if (Strings.isNullOrEmpty(header)) {
            return Observability.UNDECIDED;
        } else {
            return "1".equals(header) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE;
        }
    }

    private String getPathTemplate() {
        return Optional.ofNullable(uriInfo)
                .map(ExtendedUriInfo::getMatchedModelResource)
                .map(Resource::getPath)
                .orElse("(unknown)");
    }

    private enum FunctionalTagTranslator implements TagTranslator<Consumer<BiConsumer<String, String>>> {
        INSTANCE;

        @Override
        public <T> void translate(TagAdapter<T> adapter, T target, Consumer<BiConsumer<String, String>> data) {
            data.accept((key, value) -> adapter.tag(target, key, value));
        }
    }
}
