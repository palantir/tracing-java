/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.undertest;

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.servlet.util.ImmediateInstanceFactory;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHost;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@SuppressWarnings("NullAway")
public final class UndertowServerExtension implements BeforeAllCallback, AfterAllCallback {

    private Undertow server;

    private CloseableHttpClient httpClient;

    private List<ServletInfo> servlets = new ArrayList<>();
    private List<FilterInfo> filters = new ArrayList<>();

    // LinkedHashMap preserves order
    private Map<String, String> filterUrlMapping = new LinkedHashMap<>();

    private List<Object> jerseyObjects = new ArrayList<>();

    public static UndertowServerExtension create() {
        return new UndertowServerExtension();
    }

    public UndertowServerExtension servlet(ServletInfo servlet) {
        servlets.add(servlet);
        return this;
    }

    public UndertowServerExtension filter(FilterInfo filter) {
        filters.add(filter);
        return this;
    }

    public UndertowServerExtension filterUrlMapping(String name, String url) {
        Preconditions.checkArgument(
                filterUrlMapping.put(name, url) == null, "name already existed", SafeArg.of("name", name));
        return this;
    }

    public UndertowServerExtension jersey(Object jerseyObject) {
        jerseyObjects.add(jerseyObject);
        return this;
    }

    @Override
    public void beforeAll(ExtensionContext _context) throws ServletException {
        DeploymentInfo servletBuilder = Servlets.deployment()
                .setDeploymentName("test")
                .setContextPath("/")
                .setClassLoader(UndertowServerExtension.class.getClassLoader());

        servletBuilder.addServlets(servlets);
        servletBuilder.addFilters(filters);

        filterUrlMapping.forEach((key, value) -> {
            servletBuilder.addFilterUrlMapping(key, value, DispatcherType.REQUEST);
        });

        if (!jerseyObjects.isEmpty()) {
            ResourceConfig jerseyConfig = new ResourceConfig()
                    .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
                    .property(ServerProperties.WADL_FEATURE_DISABLE, true)
                    .register(new TracingExceptionMapper())
                    .register(new JacksonFeature());
            jerseyObjects.forEach(jerseyConfig::register);

            servletBuilder.addServlet(Servlets.servlet(
                            "jersey",
                            ServletContainer.class,
                            new ImmediateInstanceFactory<>(new ServletContainer(jerseyConfig)))
                    .addMapping("/*"));
        }

        DeploymentManager manager = Servlets.defaultContainer().addDeployment(servletBuilder);
        manager.deploy();

        server = Undertow.builder()
                .addHttpListener(0, "0.0.0.0")
                .setHandler(Handlers.path().addPrefixPath("/", manager.start()))
                .build();
        server.start();

        httpClient = HttpClients.createDefault();
    }

    @Override
    public void afterAll(ExtensionContext _context) throws IOException {
        if (server != null) {
            server.stop();
        }
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @MustBeClosed
    @CheckReturnValue
    public CloseableHttpResponse makeRequest(ClassicHttpRequest request) throws IOException {
        return httpClient.execute(HttpHost.create(URI.create("http://localhost:" + getLocalPort())), request);
    }

    public void runRequest(ClassicHttpRequest request, Consumer<CloseableHttpResponse> handler) {
        try (CloseableHttpResponse response = makeRequest(request)) {
            handler.accept(response);
        } catch (IOException e) {
            throw new SafeRuntimeException("Failed to make http request", e);
        }
    }

    public int getLocalPort() {
        return ((InetSocketAddress) server.getListenerInfo().iterator().next().getAddress()).getPort();
    }
}
