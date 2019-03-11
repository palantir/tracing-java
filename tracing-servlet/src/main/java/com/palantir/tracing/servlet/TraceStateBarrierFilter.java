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
import com.palantir.tracing.Trace;
import com.palantir.tracing.Tracer;
import java.io.IOException;
import java.util.Objects;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guarantees clean {@link Tracer} thread state for incoming requests.
 * This {@link Filter} logs at <pre>DEBUG</pre> level when another a leaked trace is
 * encountered on the pooled server thread, as well as when the operation wrapped by
 * this filter has leaked state.
 */
public final class TraceStateBarrierFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(TraceStateBarrierFilter.class);

    @Override
    public void init(FilterConfig filterConfig) {
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
                    log.debug("This operation has leaked Tracer state. Tracer.startSpan was executed without "
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
