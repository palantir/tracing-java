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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

final class Utils {

    public static float percent(long numerator, long denominator) {
        return 100f * numerator / denominator;
    }

    public static String renderDuration(float amount, TimeUnit timeUnit) {
        ImmutableMap<TimeUnit, TimeUnit> largerUnit = ImmutableMap.<TimeUnit, TimeUnit>builder()
                .put(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS)
                .put(TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS)
                .put(TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
                .build();

        ImmutableMap<TimeUnit, String> abbreviation = ImmutableMap.<TimeUnit, String>builder()
                .put(TimeUnit.NANOSECONDS, "ns")
                .put(TimeUnit.MICROSECONDS, "micros")
                .put(TimeUnit.MILLISECONDS, "ms")
                .put(TimeUnit.SECONDS, "s")
                .build();

        TimeUnit bigger = largerUnit.get(timeUnit);
        if (amount >= 1000 && bigger != null) {
            return renderDuration(amount / 1000, bigger);
        }

        return String.format("%.2f %s", amount, abbreviation.get(timeUnit));
    }

    private Utils() {}

    public static Path createOutputFile(Class<?> clazz, String methodName) {
        Path base = Paths.get(Optional.ofNullable(System.getenv("CIRCLE_ARTIFACTS")).orElse("build"));
        Path dir = base.resolve("tracing").resolve(clazz.getSimpleName());
        try {
            Files.createDirectories(dir);
            return dir.resolve(methodName + ".html");
        } catch (IOException e) {
            throw new RuntimeException("Unable to create directory " + dir, e);
        }
    }
}
