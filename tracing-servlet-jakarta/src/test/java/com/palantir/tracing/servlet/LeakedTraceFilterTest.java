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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.tracing.AlwaysSampler;
import com.palantir.tracing.Tracer;
import com.palantir.undertest.UndertowServerExtension;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.util.ImmediateInstanceFactory;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class LeakedTraceFilterTest {
    @BeforeEach
    void beforeEach() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @Test
    public void testFilter_noLeaks() throws IOException {
        undertow.runRequest(new HttpGet("/standard"), response -> {
            assertThat(response.getCode()).isBetween(200, 299);
            assertThat(response.getFirstHeader("Servlet-Has-Trace").getValue()).isEqualTo("false");
            assertThat(response.getFirstHeader("Pre-Leak").getValue()).isEqualTo("false");
            assertThat(response.getFirstHeader("Post-Leak").getValue()).isEqualTo("false");
        });
    }

    @Test
    public void testFilter_previousRequestLeaked() throws IOException {
        undertow.runRequest(new HttpGet("/previous-request-leaked"), response -> {
            assertThat(response.getCode()).isBetween(200, 299);
            assertThat(response.getFirstHeader("Pre-Leak").getValue()).isEqualTo("true");
            // But the leaked trace filter fixes thread state prior to allowing our servlet to execute
            assertThat(response.getFirstHeader("Servlet-Has-Trace").getValue()).isEqualTo("false");
            assertThat(response.getFirstHeader("Post-Leak").getValue()).isEqualTo("false");
        });
    }

    @Test
    public void testFilter_aroundLeakyOperation() throws IOException {
        undertow.runRequest(new HttpGet("/leaky"), response -> {
            assertThat(response.getCode()).isBetween(200, 299);
            assertThat(response.getFirstHeader("Pre-Leak").getValue()).isEqualTo("false");
            // Validate the test executed the leaky servlet
            assertThat(response.getFirstHeader("Leaky-Invoked").getValue()).isEqualTo("true");
            // Leaked trace filter must clean the leak before the test filter is invoked.
            assertThat(response.getFirstHeader("Post-Leak").getValue()).isEqualTo("false");
        });
    }

    @RegisterExtension
    public static final UndertowServerExtension undertow = UndertowServerExtension.create()
            .filter(Servlets.filter(
                    "previousRequestLeaked", HttpFilter.class, new ImmediateInstanceFactory<>(new HttpFilter() {
                        @Override
                        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                                throws IOException, ServletException {
                            // Open a span to simulate a thread from another request
                            // leaving bad data without the leaked trace filter applied.
                            Tracer.fastStartSpan("previous request leaked");
                            chain.doFilter(request, response);
                        }
                    })))
            .filterUrlMapping("previousRequestLeaked", "/previous-request-leaked")
            // Register a filter to help us orchestrate test cases
            .filter(Servlets.filter("testFilter", HttpFilter.class, new ImmediateInstanceFactory<>(new HttpFilter() {
                @Override
                public void doFilter(HttpServletRequest req, HttpServletResponse httpResponse, FilterChain chain)
                        throws IOException, ServletException {
                    httpResponse.addHeader("Pre-Leak", Boolean.toString(Tracer.hasTraceId()));
                    try {
                        chain.doFilter(req, httpResponse);
                    } finally {
                        httpResponse.addHeader("Post-Leak", Boolean.toString(Tracer.hasTraceId()));
                    }
                }
            })))
            .filterUrlMapping("testFilter", "/*")
            .filter(Servlets.filter(
                    "leakedTraceFilter",
                    LeakedTraceFilter.class,
                    new ImmediateInstanceFactory<>(new LeakedTraceFilter())))
            .filterUrlMapping("leakedTraceFilter", "/*")
            .servlet(Servlets.servlet(
                            "alwaysLeaks", HttpServlet.class, new ImmediateInstanceFactory<>(new HttpServlet() {
                                @Override
                                protected void service(HttpServletRequest _req, HttpServletResponse resp) {
                                    Tracer.fastStartSpan("leaky");
                                    resp.addHeader("Leaky-Invoked", "true");
                                }
                            }))
                    .addMapping("/leaky"))
            .servlet(Servlets.servlet(
                            "reportingServlet", HttpServlet.class, new ImmediateInstanceFactory<>(new HttpServlet() {
                                @Override
                                protected void service(HttpServletRequest _req, HttpServletResponse resp) {
                                    resp.addHeader("Servlet-Has-Trace", Boolean.toString(Tracer.hasTraceId()));
                                }
                            }))
                    .addMappings("/standard", "/previous-request-leaked"));
}
