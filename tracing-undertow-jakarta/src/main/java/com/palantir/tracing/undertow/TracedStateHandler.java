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

import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.DetachedSpan;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

/**
 * Handler applies existing tracing state associated with the {@link HttpServerExchange} to the current thread
 * without additional tracing spans. This ensures that logging has the correct {@code traceId} and
 * standard tracing uses are associated with the request span.
 */
public final class TracedStateHandler implements HttpHandler {
    private final HttpHandler delegate;

    public TracedStateHandler(HttpHandler delegate) {
        this.delegate = Preconditions.checkNotNull(delegate, "A delegate HttpHandler is required");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // We expect that TracedRequestHandler has already been invoked, however we create a generic request
        // span in the off-chance it hasn't.
        DetachedSpan detachedSpan = UndertowTracing.getOrInitializeRequestTrace(
                exchange, "Undertow Request", StatusCodeTagTranslator.INSTANCE);
        try (CloseableSpan ignored = detachedSpan.attach()) {
            delegate.handleRequest(exchange);
        }
    }

    @Override
    public String toString() {
        return "TracedStateHandler{" + delegate + '}';
    }
}
