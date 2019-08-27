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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

final class TestTracingExtension implements BeforeEachCallback, AfterEachCallback {

    private final TestTracingSubscriber subscriber = new TestTracingSubscriber();

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
            Serialization.serialize(file, subscriber.getAllSpans());
            return;
        }

        // TODO(df0x): filter for just one traceId (??) to figure out concurrency
        SpanAnalyzer.Result expected = SpanAnalyzer.analyze(Serialization.deserialize(file));
        SpanAnalyzer.Result actual = SpanAnalyzer.analyze(subscriber.getAllSpans());

        ImmutableSet<String> problemSpanIds = compareSpansRecursively(expected, actual, expected.root(), actual.root())
                .collect(ImmutableSet.toImmutableSet());

        if (!problemSpanIds.isEmpty()) {
            Path actualPath = Paths.get("/Users/dfox/Downloads/actual.html");

            HtmlFormatter.builder()
                    .spans(actual.orderedSpans())
                    .path(actualPath)
                    .displayName("actual")
                    .problemSpanIds(problemSpanIds)
                    .buildAndFormat();

            Path expectedPath = Paths.get("/Users/dfox/Downloads/expected.html");
            HtmlFormatter.builder()
                    .spans(expected.orderedSpans())
                    .path(expectedPath)
                    .displayName("expected")
                    .problemSpanIds(problemSpanIds)
                    .buildAndFormat();

            // TODO(dfox): render nicely here
            throw new AssertionError(
                    String.format(
                            "traces did not match the expected file '%s'.\n"
                                    + "Compare:\n"
                                    + " - expected: %s\n"
                                    + " - actual:   %s\n"
                                    + "Or re-run with -Drecreate=true to accept the new behaviour.",
                            file,
                            expectedPath,
                            actualPath));
        }
    }

    private static Stream<String> compareSpansRecursively(
            SpanAnalyzer.Result expected,
            SpanAnalyzer.Result actual,
            Span ex,
            Span ac) {
        if (!ex.getOperation().equals(ac.getOperation())) {
            // TODO(dfox): explain to users that these needed span operations to be equal
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
                .mapToObj(i -> compareSpansRecursively(
                        expected,
                        actual,
                        sortedExpectedChildren.get(i),
                        sortedActualChildren.get(i)))
                .flatMap(Function.identity());
    }

    private static String testName(ExtensionContext context) {
        return context.getRequiredTestClass().getSimpleName() + "/" + context.getRequiredTestMethod().getName();
    }
}
