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

import com.google.common.collect.ImmutableMap;
import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.DetachedSpan;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import java.util.Map;

/**
 * Extracts Zipkin-style trace information from the given HTTP request and sets up a corresponding
 * {@link com.palantir.tracing.Trace} and {@link com.palantir.tracing.api.Span} for delegating to the configured
 * {@link #delegate} handler. See <a href="https://github.com/openzipkin/b3-propagation">b3-propagation</a>.
 *
 * <p>Note that this handler must be registered after routing, each instance is used for exactly one operation name.
 * This {@link HttpHandler handler} traces the execution of the {@link TracedOperationHandler#delegate} handlers
 * {@link HttpHandler#handleRequest(HttpServerExchange)}, but does not apply tracing to any asynchronous operations that
 * handler may register.
 */
public final class TracedOperationHandler implements HttpHandler {
    /**
     * Attachment to check whether the current request is being traced.
     *
     * @deprecated in favor of {@link TracingAttachments#IS_SAMPLED}
     */
    @Deprecated
    public static final AttachmentKey<Boolean> IS_SAMPLED_ATTACHMENT = TracingAttachments.IS_SAMPLED;

    private final String operation;
    private final Map<String, String> metadata;
    private final HttpHandler delegate;

    public TracedOperationHandler(HttpHandler delegate, String operation, Map<String, String> metadata) {
        this.delegate = checkNotNull(delegate, "A delegate HttpHandler is required");
        this.operation = "Undertow: " + checkNotNull(operation, "Operation name is required");
        this.metadata = checkNotNull(metadata, "Metadata map is required");
    }

    public TracedOperationHandler(HttpHandler delegate, String operation) {
        this(delegate, operation, ImmutableMap.of());
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        DetachedSpan detachedSpan = UndertowTracing.getOrInitializeRequestTrace(exchange);
        try (CloseableSpan ignored = detachedSpan.childSpan(operation, metadata)) {
            delegate.handleRequest(exchange);
        }
    }

    @Override
    public String toString() {
        return "TracedOperationHandler{operation='" + operation + "', delegate=" + delegate + '}';
    }
}
