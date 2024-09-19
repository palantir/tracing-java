/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.TraceHttpHeaders;
import io.undertow.Undertow;
import io.undertow.server.handlers.BlockingHandler;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public final class TracedExchangeCompletionListenerTest {

    @Test
    public void testExchangeCompletionListener() throws Exception {
        String expectedTraceId = Tracers.randomId();
        AtomicReference<String> completionListenerTraceId = new AtomicReference<>();
        Undertow undertow = Undertow.builder()
                .setIoThreads(1)
                .setWorkerThreads(2)
                .setHandler(new TracedRequestHandler(
                        new BlockingHandler(exchange -> exchange.addExchangeCompleteListener((_exc, nextListener) -> {
                            completionListenerTraceId.set(Tracer.hasTraceId() ? Tracer.getTraceId() : "none");
                            nextListener.proceed();
                        }))))
                .addHttpListener(0, null)
                .build();
        undertow.start();
        try {
            HttpURLConnection connection =
                    (HttpURLConnection) new URL("http://127.0.0.1:" + getPort(undertow)).openConnection();
            connection.setRequestProperty(TraceHttpHeaders.TRACE_ID, expectedTraceId);
            assertThat(connection.getResponseCode()).isEqualTo(200);
        } finally {
            undertow.stop();
        }
        assertThat(completionListenerTraceId).hasValue(expectedTraceId);
    }

    private static int getPort(Undertow undertow) {
        InetSocketAddress address = (InetSocketAddress)
                Iterables.getOnlyElement(undertow.getListenerInfo()).getAddress();
        return address.getPort();
    }
}
