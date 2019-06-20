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
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.junit.DropwizardAppRule;
import java.io.IOException;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class LeakedTraceFilterTest {
    @ClassRule
    public static final DropwizardAppRule<Configuration> APP =
            new DropwizardAppRule<>(TracingTestServer.class, "src/test/resources/test-server.yml");

    private WebTarget target;

    @Before
    public void before() {
        String endpointUri = "http://localhost:" + APP.getLocalPort();
        JerseyClientBuilder builder = new JerseyClientBuilder();
        Client client = builder.build();
        target = client.target(endpointUri);
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @Test
    public void testFilter_noLeaks() {
        Response response = target.path("/standard").request().get();
        assertThat(response.getHeaderString("Servlet-Has-Trace")).isEqualTo("false");
        assertThat(response.getHeaderString("Pre-Leak")).isEqualTo("false");
        assertThat(response.getHeaderString("Post-Leak")).isEqualTo("false");
        response.close();
    }

    @Test
    public void testFilter_previousRequestLeaked() {
        Response response = target.path("/previous-request-leaked").request().get();
        // Verify that we detected a leak
        assertThat(response.getHeaderString("Pre-Leak")).isEqualTo("true");
        // But the leaked trace filter fixes thread state prior to allowing our servlet to execute
        assertThat(response.getHeaderString("Servlet-Has-Trace")).isEqualTo("false");
        assertThat(response.getHeaderString("Post-Leak")).isEqualTo("false");
        response.close();
    }

    @Test
    public void testFilter_aroundLeakyOperation() {
        Response response = target.path("/leaky").request().get();
        assertThat(response.getHeaderString("Pre-Leak")).isEqualTo("false");
        // Validate the test executed the leaky servlet
        assertThat(response.getHeaderString("Leaky-Invoked")).isEqualTo("true");
        // Leaked trace filter must clean the leak before the test filter is invoked.
        assertThat(response.getHeaderString("Post-Leak")).isEqualTo("false");
        response.close();
    }

    public static class TracingTestServer extends Application<Configuration> {
        @Override
        public final void run(Configuration config, final Environment env) throws Exception {
            env.servlets().addFilter("previousRequestLeaked", new Filter() {
                @Override
                public void init(FilterConfig filterConfig) { }

                @Override
                public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                        throws IOException, ServletException {
                    // Open a span to simulate a thread from another request
                    // leaving bad data without the leaked trace filter applied.
                    Tracer.fastStartSpan("previous request leaked");
                    chain.doFilter(request, response);
                }

                @Override
                public void destroy() { }
            }).addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true,
                    "/previous-request-leaked");

            // Register a filter to help us orchestrate test cases
            env.servlets().addFilter("testFilter", new Filter() {
                @Override
                public void init(FilterConfig filterConfig) {}

                @Override
                public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                        throws IOException, ServletException {
                    HttpServletResponse httpResponse = (HttpServletResponse) response;
                    httpResponse.addHeader("Pre-Leak", Boolean.toString(Tracer.hasTraceId()));
                    try {
                        chain.doFilter(request, response);
                    } finally {
                        httpResponse.addHeader("Post-Leak", Boolean.toString(Tracer.hasTraceId()));
                    }
                }

                @Override
                public void destroy() {}
            }).addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

            // Register the filter we're testing
            env.servlets().addFilter("leakedTraceFilter", new LeakedTraceFilter())
                    .addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

            env.servlets().addServlet("alwaysLeaks", new HttpServlet() {
                @Override
                protected void service(HttpServletRequest req, HttpServletResponse resp) {
                    Tracer.fastStartSpan("leaky");
                    resp.addHeader("Leaky-Invoked", "true");
                }
            }).addMapping("/leaky");

            env.servlets().addServlet("reportingServlet", new HttpServlet() {
                @Override
                protected void service(HttpServletRequest req, HttpServletResponse resp) {
                    resp.addHeader("Servlet-Has-Trace", Boolean.toString(Tracer.hasTraceId()));
                }
            }).addMapping("/standard", "/previous-request-leaked");
        }
    }
}
