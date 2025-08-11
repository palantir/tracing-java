/*
 * (c) Copyright 2025 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.tracing.logger;

import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.logger.api.TraceLogger;
import com.palantir.tracing.logger.spi.TraceLoggerFactorySpi;
import java.util.List;
import java.util.ServiceLoader;

// TODO(pkoenig): SpanType
public final class TraceLoggerFactory {

    private static final TraceLoggerFactorySpi TRACE_LOGGER_FACTORY_SPI = loadSpi();

    private TraceLoggerFactory() {}

    public static TraceLogger get(@Safe Class<?> clazz) {
        return TRACE_LOGGER_FACTORY_SPI.get(clazz.getName());
    }

    public static TraceLogger get(@Safe String name) {
        return TRACE_LOGGER_FACTORY_SPI.get(name);
    }

    private static TraceLoggerFactorySpi loadSpi() {
        List<ServiceLoader.Provider<TraceLoggerFactorySpi>> providers =
                ServiceLoader.load(TraceLoggerFactorySpi.class).stream().toList();

        return switch (providers.size()) {
            case 0 -> {
                throw new SafeRuntimeException("Found no TraceLoggerFactorySpi implementations");
            }
            case 1 -> {
                yield providers.get(0).get();
            }
            default -> {
                throw new SafeRuntimeException(
                        "Found multiple TraceLoggerFactorySpi implementations",
                        SafeArg.of(
                                "types",
                                providers.stream()
                                        .map(ServiceLoader.Provider::type)
                                        .toList()));
            }
        };
    }
}
