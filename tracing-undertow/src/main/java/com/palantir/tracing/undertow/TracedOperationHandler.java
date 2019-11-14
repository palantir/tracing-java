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

import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;

/**
 * Extracts Zipkin-style trace information from the given HTTP request and sets up a corresponding
 * {@link com.palantir.tracing.Trace} and {@link com.palantir.tracing.api.Span} for delegating to the configured
 * {@link #delegate} handler. See <a href="https://github.com/openzipkin/b3-propagation">b3-propagation</a>.
 *
 * Note that this handler must be registered after routing, each instance is used for exactly one operation name.
 * This {@link HttpHandler handler} traces the execution of the {@link TracedOperationHandler#delegate} handlers
 * {@link HttpHandler#handleRequest(HttpServerExchange)}, but does not apply tracing to any asynchronous operations
 * that handler may register.
 */
public final class TracedOperationHandler implements HttpHandler {
    /**
     * Attachment to check whether the current request is being traced.
     */
    public static final AttachmentKey<Boolean> IS_SAMPLED_ATTACHMENT = AttachmentKey.create(Boolean.class);
    public static final AttachmentKey<String> REQUEST_ID_ATTACHMENT = AttachmentKey.create(String.class);

    private static final HttpString TRACE_ID = HttpString.tryFromString(TraceHttpHeaders.TRACE_ID);
    private static final HttpString SPAN_ID = HttpString.tryFromString(TraceHttpHeaders.SPAN_ID);
    private static final HttpString IS_SAMPLED = HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED);

    private final String operation;
    private final HttpHandler delegate;

    public TracedOperationHandler(HttpHandler delegate, String operation) {
        this.delegate = checkNotNull(delegate, "A delegate HttpHandler is required");
        this.operation = "Undertow: " + checkNotNull(operation, "Operation name is required");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        HeaderMap headers = exchange.getRequestHeaders();
        // TODO(rfink): Log/warn if we find multiple headers?
        String traceIdHeader = headers.getFirst(TRACE_ID); // nullable
        String traceId;
        // XXX: again, nasty abstraction leak
        String requestId;

        // Set up thread-local span that inherits state from HTTP headers
        if (Strings.isNullOrEmpty(traceIdHeader)) {
            traceId = initializeNewTrace(headers);
            requestId = Tracer.startSpan(operation, SpanType.SERVER_INCOMING).getSpanId();
        } else {
            traceId = traceIdHeader;
            Tracer.initTrace(getObservabilityFromHeader(headers), traceIdHeader);
            String spanId = headers.getFirst(SPAN_ID); // nullable
            if (spanId == null) {
                requestId = Tracer.startSpan(operation, SpanType.SERVER_INCOMING).getSpanId();
            } else {
                requestId = Tracer.startSpan(operation, spanId, SpanType.SERVER_INCOMING).getSpanId();
            }
        }

        // Populate response before calling delegate since delegate might commit the response.
        exchange.getResponseHeaders().put(TRACE_ID, traceId);
        exchange.putAttachment(IS_SAMPLED_ATTACHMENT, Tracer.isTraceObservable());
        exchange.putAttachment(REQUEST_ID_ATTACHMENT, requestId);
        try {
            delegate.handleRequest(exchange);
        } finally {
            Tracer.fastCompleteSpan();
        }
    }

    // Force sample iff the context contains a "1" X-B3-Sampled header, force not sample if the header contains another
    // non-empty value, or undecided if there is no such header or the header is empty.
    private static Observability getObservabilityFromHeader(HeaderMap headers) {
        String header = headers.getFirst(IS_SAMPLED);
        if (Strings.isNullOrEmpty(header)) {
            return Observability.UNDECIDED;
        } else {
            return "1".equals(header) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE;
        }
    }

    /** Initializes trace state for a request without tracing headers. */
    private String initializeNewTrace(HeaderMap headers) {
        // HTTP request did not indicate a trace; initialize trace state and create a span.
        String newTraceId = Tracers.randomId();
        Tracer.initTrace(getObservabilityFromHeader(headers), newTraceId);
        return newTraceId;
    }

    @Override
    public String toString() {
        return "TracedOperationHandler{operation='" + operation + "', delegate=" + delegate + '}';
    }
}
