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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ServletInfo;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class UndertowServerExtension implements BeforeAllCallback, AfterAllCallback {

    private Undertow server;

    private List<ServletInfo> servlets = new ArrayList<>();
    private List<FilterInfo> filters = new ArrayList<>();

    private Map<String, String> filterUrlMapping = new HashMap<>();

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

    @Override
    @SuppressWarnings("ProxyNonConstantType")
    public void beforeAll(ExtensionContext _context) throws ServletException {
        DeploymentInfo servletBuilder =
                Servlets.deployment().setClassLoader(UndertowServerExtension.class.getClassLoader());

        servletBuilder.addServlets(servlets);
        servletBuilder.addFilters(filters);

        filterUrlMapping.forEach((key, value) -> {
            servletBuilder.addFilterUrlMapping(key, value, DispatcherType.REQUEST);
        });

        DeploymentManager manager = Servlets.defaultContainer().addDeployment(servletBuilder);
        manager.deploy();

        server = Undertow.builder()
                .addHttpListener(0, "0.0.0.0")
                .setHandler(Handlers.path().addPrefixPath("/", manager.start()))
                .build();
        server.start();
    }

    @Override
    public void afterAll(ExtensionContext _context) {
        if (server != null) {
            server.stop();
        }
    }

    public int getLocalPort() {
        return ((InetSocketAddress) server.getListenerInfo().iterator().next().getAddress()).getPort();
    }
}