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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.InternalTracers;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import java.util.Map;
import java.util.Optional;

/**
 * Internal utility functionality shared between {@link TracedOperationHandler} and {@link TracedRequestHandler}.
 * Intentionally package private.
 */
final class UndertowTracing {

    // Tracing header definitions
    private static final HttpString TRACE_ID = HttpString.tryFromString(TraceHttpHeaders.TRACE_ID);
    private static final HttpString SPAN_ID = HttpString.tryFromString(TraceHttpHeaders.SPAN_ID);
    private static final HttpString IS_SAMPLED = HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED);

    // Consider moving this to TracingAttachments and making it public. For now it's well encapsulated
    // here because we expect the two handler implementations to be sufficient.
    /** Detached span object representing the entire request including asynchronous components. */
    @VisibleForTesting
    static final AttachmentKey<DetachedSpan> REQUEST_SPAN = AttachmentKey.create(DetachedSpan.class);

    private static final String OPERATION_NAME = "Undertow Request";

    /** Apply detached tracing state to the provided {@link HttpServerExchange request}. */
    static DetachedSpan getOrInitializeRequestTrace(HttpServerExchange exchange) {
        DetachedSpan detachedSpan = exchange.getAttachment(REQUEST_SPAN);
        if (detachedSpan == null) {
            return initializeRequestTrace(exchange);
        }
        return detachedSpan;
    }

    private static DetachedSpan initializeRequestTrace(HttpServerExchange exchange) {
        HeaderMap requestHeaders = exchange.getRequestHeaders();
        String maybeTraceId = requestHeaders.getFirst(TRACE_ID);
        boolean newTraceId = maybeTraceId == null;
        String traceId = newTraceId ? Tracers.randomId() : maybeTraceId;
        DetachedSpan detachedSpan = detachedSpan(newTraceId, traceId, requestHeaders);
        setExchangeState(exchange, detachedSpan, traceId);
        return detachedSpan;
    }

    private static void setExchangeState(HttpServerExchange exchange, DetachedSpan detachedSpan, String traceId) {
        // Populate response before proceeding since later operations might commit the response.
        exchange.getResponseHeaders().put(TRACE_ID, traceId);
        exchange.putAttachment(TracingAttachments.IS_SAMPLED, InternalTracers.isSampled(detachedSpan));
        Optional<String> requestId = InternalTracers.getRequestId(detachedSpan);
        if (!requestId.isPresent()) {
            throw new SafeIllegalStateException("No requestId is set", SafeArg.of("span", detachedSpan));
        }
        exchange.putAttachment(TracingAttachments.REQUEST_ID, requestId.get());
        exchange.putAttachment(REQUEST_SPAN, detachedSpan);
        exchange.addExchangeCompleteListener(DetachedTraceCompletionListener.INSTANCE);
    }

    private static DetachedSpan detachedSpan(boolean newTrace, String traceId, HeaderMap requestHeaders) {
        return DetachedSpan.start(
                getObservabilityFromHeader(requestHeaders),
                traceId,
                newTrace ? Optional.empty() : Optional.ofNullable(requestHeaders.getFirst(SPAN_ID)),
                OPERATION_NAME,
                SpanType.SERVER_INCOMING);
    }

    private enum DetachedTraceCompletionListener implements ExchangeCompletionListener {
        INSTANCE;

        @Override
        public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
            try {
                DetachedSpan detachedSpan = exchange.getAttachment(REQUEST_SPAN);
                if (detachedSpan != null) {
                    detachedSpan.complete(statusMetadata(exchange.getStatusCode()));
                }
            } finally {
                nextListener.proceed();
            }
        }
    }

    private static final ImmutableMap<String, String> METADATA_200 = ImmutableMap.of("status", "200");
    private static final ImmutableMap<String, String> METADATA_204 = ImmutableMap.of("status", "204");

    private static Map<String, String> statusMetadata(int statusCode) {
        // handle common cases quickly
        switch (statusCode) {
            case 200:
                return METADATA_200;
            case 204:
                return METADATA_204;
        }
        return ImmutableMap.of("status", Integer.toString(statusCode));
    }

    /**
     * Force sample iff the context contains a "1" X-B3-Sampled header, force not sample if the header contains another
     * non-empty value, or undecided if there is no such header or the header is empty.
     */
    private static Observability getObservabilityFromHeader(HeaderMap headers) {
        String header = headers.getFirst(IS_SAMPLED);
        if (Strings.isNullOrEmpty(header)) {
            return Observability.UNDECIDED;
        } else {
            return "1".equals(header) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE;
        }
    }

    private UndertowTracing() {}
}
