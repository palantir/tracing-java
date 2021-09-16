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

import com.google.common.base.Strings;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.InternalTracers;
import com.palantir.tracing.Observability;
import com.palantir.tracing.TagTranslator;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import java.util.Optional;

/**
 * Internal utility functionality shared between {@link TracedRequestHandler}, {@link TracedStateHandler}, and
 * {@link TracedOperationHandler}.
 * Intentionally package private.
 */
final class UndertowTracing {

    private static final SafeLogger log = SafeLoggerFactory.get(UndertowTracing.class);

    // Tracing header definitions
    private static final HttpString TRACE_ID = HttpString.tryFromString(TraceHttpHeaders.TRACE_ID);
    private static final HttpString SPAN_ID = HttpString.tryFromString(TraceHttpHeaders.SPAN_ID);
    private static final HttpString IS_SAMPLED = HttpString.tryFromString(TraceHttpHeaders.IS_SAMPLED);
    private static final HttpString ORIGIN_USER_AGENT = HttpString.tryFromString(TraceHttpHeaders.ORIGIN_USER_AGENT);

    // Consider moving this to TracingAttachments and making it public. For now it's well encapsulated
    // here because we expect the two handler implementations to be sufficient.
    /**
     * Detached span object representing the entire request including asynchronous components.
     */
    static final AttachmentKey<DetachedSpan> REQUEST_SPAN = AttachmentKey.create(DetachedSpan.class);

    private static final AttachmentKey<TagTranslator<? super HttpServerExchange>> TAG_TRANSLATOR_ATTACHMENT_KEY =
            AttachmentKey.create(TagTranslator.class);

    /**
     * Apply detached tracing state to the provided {@link HttpServerExchange request}.
     */
    static DetachedSpan getOrInitializeRequestTrace(
            HttpServerExchange exchange, String operationName, TagTranslator<? super HttpServerExchange> translator) {
        DetachedSpan detachedSpan = exchange.getAttachment(REQUEST_SPAN);
        if (detachedSpan == null) {
            return initializeRequestTrace(exchange, operationName, translator);
        }
        return detachedSpan;
    }

    private static DetachedSpan initializeRequestTrace(
            HttpServerExchange exchange, String operationName, TagTranslator<? super HttpServerExchange> translator) {
        HeaderMap requestHeaders = exchange.getRequestHeaders();
        String maybeTraceId = requestHeaders.getFirst(TRACE_ID);
        boolean newTraceId = maybeTraceId == null;
        String traceId = newTraceId ? Tracers.randomId() : maybeTraceId;
        DetachedSpan detachedSpan = detachedSpan(operationName, newTraceId, traceId, requestHeaders);
        setExchangeState(exchange, detachedSpan, traceId, translator);
        return detachedSpan;
    }

    private static void setExchangeState(
            HttpServerExchange exchange,
            DetachedSpan detachedSpan,
            String traceId,
            TagTranslator<? super HttpServerExchange> translator) {
        // Populate response before proceeding since later operations might commit the response.
        exchange.getResponseHeaders().put(TRACE_ID, traceId);
        boolean isSampled = InternalTracers.isSampled(detachedSpan);
        exchange.putAttachment(TracingAttachments.IS_SAMPLED, isSampled);
        Optional<String> requestId = InternalTracers.getRequestId(detachedSpan);
        if (!requestId.isPresent()) {
            throw new SafeIllegalStateException("No requestId is set", SafeArg.of("span", detachedSpan));
        }
        exchange.putAttachment(TracingAttachments.REQUEST_ID, requestId.get());
        exchange.putAttachment(REQUEST_SPAN, detachedSpan);
        exchange.putAttachment(TAG_TRANSLATOR_ATTACHMENT_KEY, translator);
        exchange.addExchangeCompleteListener(DetachedTraceCompletionListener.INSTANCE);
    }

    private static DetachedSpan detachedSpan(
            String operationName, boolean newTrace, String traceId, HeaderMap requestHeaders) {
        return DetachedSpan.start(
                getObservabilityFromHeader(requestHeaders),
                traceId,
                newTrace ? Optional.empty() : Optional.ofNullable(requestHeaders.getFirst(SPAN_ID)),
                operationName,
                SpanType.SERVER_INCOMING,
                Optional.ofNullable(requestHeaders.getFirst(ORIGIN_USER_AGENT)));
    }

    private enum DetachedTraceCompletionListener implements ExchangeCompletionListener {
        INSTANCE;

        @Override
        public void exchangeEvent(HttpServerExchange exchange, NextListener nextListener) {
            try {
                DetachedSpan detachedSpan = exchange.getAttachment(REQUEST_SPAN);
                if (detachedSpan != null) {
                    detachedSpan.complete(tagTranslator(exchange), exchange);
                }
            } catch (Throwable t) {
                log.error("Failed to complete the request tracing span", t);
            } finally {
                nextListener.proceed();
            }
        }

        static TagTranslator<? super HttpServerExchange> tagTranslator(HttpServerExchange exchange) {
            TagTranslator<? super HttpServerExchange> translator =
                    exchange.getAttachment(TAG_TRANSLATOR_ATTACHMENT_KEY);
            return translator == null ? StatusCodeTagTranslator.INSTANCE : translator;
        }
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
