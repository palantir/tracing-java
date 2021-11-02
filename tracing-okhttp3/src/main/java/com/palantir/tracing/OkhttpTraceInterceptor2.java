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

package com.palantir.tracing;

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

/** An OkHttp interceptor that adds Zipkin-style trace/span/parent-span headers to the HTTP request. */
public final class OkhttpTraceInterceptor2 implements Interceptor {

    private final Function<Request, CloseableSpan> createNetworkCallSpan;

    private OkhttpTraceInterceptor2(Function<Request, CloseableSpan> createNetworkCallSpan) {
        this.createNetworkCallSpan = createNetworkCallSpan;
    }

    /** Provide a function to construct an appropriately parented span for the network call. */
    public static Interceptor create(Function<Request, CloseableSpan> createNetworkCallSpan) {
        return new OkhttpTraceInterceptor2(createNetworkCallSpan);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        try (Closeable span = createNetworkCallSpan.apply(request)) {
            if (!Tracer.hasTraceId()) {
                throw new SafeRuntimeException("Trace with no spans in progress");
            }

            Request.Builder requestBuilder = request.newBuilder();

            Tracers.addTracingHeaders(requestBuilder, EnrichingFunction.INSTANCE);

            return chain.proceed(requestBuilder.build());
        }
    }

    private enum EnrichingFunction implements TracingHeadersEnrichingFunction<Request.Builder> {
        INSTANCE;

        @Override
        public void addHeader(String headerName, String headerValue, Request.Builder state) {
            state.header(headerName, headerValue);
        }
    }
}
