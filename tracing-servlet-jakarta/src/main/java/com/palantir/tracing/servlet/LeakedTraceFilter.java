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

package com.palantir.tracing.servlet;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.Trace;
import com.palantir.tracing.Tracer;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Objects;

/**
 * Guarantees clean {@link Tracer} thread state for incoming requests. This {@link Filter} logs at
 *
 * <pre>DEBUG</pre>
 *
 * level when another a leaked trace is encountered on the pooled server thread, as well as when the operation wrapped
 * by this filter has leaked state.
 */
public final class LeakedTraceFilter implements Filter {
    private static final SafeLogger log = SafeLoggerFactory.get(LeakedTraceFilter.class);

    @Override
    public void init(FilterConfig _value) {
        // nop
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (Tracer.hasTraceId()) {
            Trace leakedTrace = Tracer.getAndClearTrace();
            if (log.isDebugEnabled()) {
                log.debug("Clearing leaked trace trace {}", SafeArg.of("trace", toLoggableValue(leakedTrace)));
            }
        }
        try {
            chain.doFilter(request, response);
        } finally {
            if (Tracer.hasTraceId()) {
                Trace leakedTrace = Tracer.getAndClearTrace();
                if (log.isDebugEnabled()) {
                    log.debug(
                            "This operation has leaked Tracer state. Tracer.startSpan was executed without "
                                    + "Tracer.completeSpan, resulting in both loss of span data and spans using "
                                    + "completion information from incorrect operations. Trace: {}, Path: {}",
                            SafeArg.of("trace", toLoggableValue(leakedTrace)),
                            UnsafeArg.of("path", getPath(request)));
                }
            }
        }
    }

    private static String toLoggableValue(Trace trace) {
        // Use the trace toString value, the Trace object is not meant to be JSON serializable
        return Objects.toString(trace);
    }

    private static String getPath(ServletRequest request) {
        if (request instanceof HttpServletRequest) {
            return ((HttpServletRequest) request).getRequestURI();
        }
        return "Unknown";
    }

    @Override
    public void destroy() {
        // nop
    }
}
