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
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
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

        // TODO filter for just one traceId (??) to figure out concurrency
        SpanAnalyzer.Result expected = SpanAnalyzer.analyze(deserialize(file));
        SpanAnalyzer.Result actual = SpanAnalyzer.analyze(subscriber.getAllSpans());

        List<String> problemSpanIds = compareSpansRecursively(expected, actual, expected.root(), actual.root())
                .collect(ImmutableList.toImmutableList());
        if (!problemSpanIds.isEmpty()) {
            // TODO(dfox): render nicely here
            throw new AssertionError("traces did not match up with expected, use -Drecreate=true to overwrite");
        }
    }

    private static Stream<String> compareSpansRecursively(
            SpanAnalyzer.Result expected,
            SpanAnalyzer.Result actual,
            Span ex,
            Span ac) {
        Assertions.assertEquals(ex.getOperation(), ac.getOperation(), "Spans should have the same operation name");
        if (!ex.getOperation().equals(ac.getOperation())) {
            // highlight these to the user
            return Stream.of(ex.getSpanId(), ac.getSpanId());
        }
        // other fields, type, params, metadata(???)

        // TODO(dfox): when there are non-overlapping spans (aka no concurrency/async), we do care about order
        // i.e. queue then run is different from run then queue.

        // ensure we have the same number of children, same child operation names in the same order
        ImmutableList<Span> sortedExpectedChildren = SpanAnalyzer.children(expected.graph(), ex)
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .collect(ImmutableList.toImmutableList());
        ImmutableList<Span> sortedActualChildren = SpanAnalyzer.children(actual.graph(), ac)
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .collect(ImmutableList.toImmutableList());

        if (sortedExpectedChildren.size() != sortedActualChildren.size()) {
            // just highlighting the parents for now.
            return Stream.of(ex.getSpanId(), ac.getSpanId());
        }

        return IntStream.range(0, sortedActualChildren.size())
                .mapToObj(i -> compareSpansRecursively(expected, actual, sortedExpectedChildren.get(i), sortedActualChildren.get(i)))
                .flatMap(Function.identity());
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
