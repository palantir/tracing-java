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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.tracing.api.DeserializeSpan;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

final class TestTracingExtension implements BeforeEachCallback, AfterEachCallback {

    private final TestTracingSubscriber subscriber = new TestTracingSubscriber();
    private static final ObjectMapper mapper = ObjectMappers.newServerObjectMapper();

    @Override
    public void beforeEach(ExtensionContext context) {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.subscribe(testName(context), subscriber);

        // TODO(dfox): clear existing tracing??
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Tracer.unsubscribe(testName(context));

        Path file = Paths.get("src/test/resources").resolve(testName(context));

        // match recorded traces against expected file (or create)
        if (!Files.exists(file) || Boolean.valueOf(System.getProperty("recreate", "false"))) {
            serialize(file, subscriber.getAllSpans());
            return;
        }

        List<Span> expected = deserialize(file);
        // TODO filter for just one traceId (??) to figure out concurrency
        Collection<Span> actual = subscriber.getAllSpans();

        if (!compare(expected, actual)) {
            // TODO(dfox): render nicely here
            throw new AssertionError("traces did not match up with expected, use -Drecreate=true to overwrite");
        }
    }

    private boolean compare(List<Span> expected, Collection<Span> actual) {
        // TODO(dfox): ensure structure of the graph is the same (don't mind about real start times / durations)
        return false;
    }

    private static String testName(ExtensionContext context) {
        return context.getRequiredTestClass().getSimpleName() + "/" + context.getRequiredTestMethod().getName();
    }

    private static void serialize(Path file, Collection<Span> allSpans) throws IOException {
        Files.createDirectories(file.getParent());
        try (OutputStream outputStream = Files.newOutputStream(file)) {
            allSpans.forEach(span -> {
                try {
                    byte[] bytes = mapper.writeValueAsBytes(span);
                    outputStream.write(bytes);
                    outputStream.write('\n');
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static List<Span> deserialize(Path file) throws IOException {
        try (Stream<String> lines = Files.lines(file)) {
            return lines.map(line -> {
                try {
                    return DeserializeSpan.deserialize(mapper, line);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(ImmutableList.toImmutableList());
        }
    }
}
