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

package com.palantir.tracing.okhttp3;

import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.TracingHeadersEnrichingFunction;
import com.palantir.tracing.api.SpanType;
import java.io.IOException;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

/**
 * An OkHttp interceptor that adds Zipkin-style trace/span/parent-span headers to the HTTP request.
 *
 * @deprecated prefer {@link com.palantir.tracing.OkhttpTraceInterceptor2}
 */
@Deprecated
public enum OkhttpTraceInterceptor implements Interceptor {
    INSTANCE;

    /** The HTTP header used to communicate API endpoint names internally. Not considered public API. */
    public static final String PATH_TEMPLATE_HEADER = "hr-path-template";

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        String spanName = request.method();
        String httpRemotingPath = request.header(PATH_TEMPLATE_HEADER);
        if (httpRemotingPath != null) {
            spanName = "OkHttp: " + httpRemotingPath;
            request = request.newBuilder().removeHeader(PATH_TEMPLATE_HEADER).build();
        }

        Tracer.fastStartSpan(spanName, SpanType.CLIENT_OUTGOING);
        Request.Builder tracedRequest = request.newBuilder();
        Tracers.addTracingHeaders(tracedRequest, EnrichingFunction.INSTANCE);

        Response response;
        try {
            response = chain.proceed(tracedRequest.build());
        } finally {
            Tracer.fastCompleteSpan();
        }

        return response;
    }

    private enum EnrichingFunction implements TracingHeadersEnrichingFunction<Request.Builder> {
        INSTANCE;

        @Override
        public void addHeader(String headerName, String headerValue, Request.Builder state) {
            state.header(headerName, headerValue);
        }
    }
}
