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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import com.spotify.dataenum.DataEnum;
import com.spotify.dataenum.dataenum_case;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
        String name = testName(context);
        Tracer.unsubscribe(name);

        Path snapshotFile = Paths.get("src/test/resources").resolve(name);

        // match recorded traces against expected file (or create)
        Collection<Span> actualSpans = subscriber.getAllSpans();
        if (!Files.exists(snapshotFile) || Boolean.valueOf(System.getProperty("recreate", "false"))) {
            Serialization.serialize(snapshotFile, actualSpans);
            return;
        }

        // TODO(df0x): filter for just one traceId (??) to figure out concurrency
        List<Span> expectedSpans = Serialization.deserialize(snapshotFile);
        SpanAnalyzer.Result expected = SpanAnalyzer.analyze(expectedSpans);
        SpanAnalyzer.Result actual = SpanAnalyzer.analyze(actualSpans);

        Set<ComparisonFailure> failures = compareSpansRecursively(expected, actual, expected.root(), actual.root())
                .collect(ImmutableSet.toImmutableSet());

        Path outputPath = Paths.get("build/reports/tracing").resolve(name);
        Files.createDirectories(outputPath);

        Path actualPath = outputPath.resolve("actual.html");
        HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                .spans(actualSpans)
                .path(actualPath)
                .displayName("actual")
                .problemSpanIds(failures.stream()
                        .map(res -> res.map(
                                ComparisonFailure.unequalOperation::expected,
                                ComparisonFailure.unequalChildren::expected,
                                ComparisonFailure.incompatibleStructure::expected))
                        .map(Span::getSpanId)
                        .collect(ImmutableSet.toImmutableSet()))
                .layoutStrategy(HtmlFormatter.LayoutStrategy.SPLIT_BY_TRACE)
                .build());


        Path expectedPath = outputPath.resolve("expected.html");
        HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                .spans(expectedSpans)
                .path(expectedPath)
                .displayName("expected")
                .problemSpanIds(failures.stream()
                        .map(res -> res.map(
                                ComparisonFailure.unequalOperation::actual,
                                ComparisonFailure.unequalChildren::actual,
                                ComparisonFailure.incompatibleStructure::actual))
                        .map(Span::getSpanId)
                        .collect(ImmutableSet.toImmutableSet()))
                .layoutStrategy(HtmlFormatter.LayoutStrategy.SPLIT_BY_TRACE)
                .build());

        if (!failures.isEmpty()) {
            throw new AssertionError(
                    String.format(
                            "Traces did not match the expected file '%s'.\n"
                                    + "%s\n"
                                    + "Visually Compare:\n"
                                    + " - expected: %s\n"
                                    + " - actual:   %s\n"
                                    + "Or re-run with -Drecreate=true to accept the new behaviour.",
                            snapshotFile,
                            failures.stream()
                                    .map(TestTracingExtension::renderFailure)
                                    .collect(Collectors.joining("\n")),
                            expectedPath.toAbsolutePath(),
                            actualPath.toAbsolutePath()));
        }
    }

    private static Stream<ComparisonFailure> compareSpansRecursively(
            SpanAnalyzer.Result expected,
            SpanAnalyzer.Result actual,
            Span ex,
            Span ac) {
        if (!ex.getOperation().equals(ac.getOperation())) {
            return Stream.of(ComparisonFailure.unequalOperation(ex, ac));
        }
        // other fields, type, params, metadata(???)

        // ensure we have the same number of children, same child operation names in the same order
        List<Span> sortedExpectedChildren = SpanAnalyzer.children(expected.graph(), ex)
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .collect(ImmutableList.toImmutableList());
        List<Span> sortedActualChildren = SpanAnalyzer.children(actual.graph(), ac)
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .collect(ImmutableList.toImmutableList());

        if (sortedExpectedChildren.size() != sortedActualChildren.size()) {
            // just highlighting the parents for now.
            return Stream.of(ComparisonFailure.unequalChildren(ex, ac, sortedExpectedChildren, sortedActualChildren));
        }

        boolean expectedContainsOverlappingSpans = containsOverlappingSpans(sortedExpectedChildren);
        boolean actualContainsOverlappingSpans = containsOverlappingSpans(sortedActualChildren);
        if (expectedContainsOverlappingSpans ^ actualContainsOverlappingSpans) {
            // Either Both or neither tree should have concurrent spans
            return Stream.of(ComparisonFailure.incompatibleStructure(ex, ac));
        }

        if (actualContainsOverlappingSpans) {
            return sudokuMatching(expected, actual, ex, ac, sortedExpectedChildren, sortedActualChildren);
        }

        return IntStream.range(0, sortedActualChildren.size())
                .mapToObj(i -> compareSpansRecursively(
                        expected,
                        actual,
                        sortedExpectedChildren.get(i),
                        sortedActualChildren.get(i)))
                .flatMap(Function.identity());
    }

    private static Stream<ComparisonFailure> sudokuMatching(
            SpanAnalyzer.Result expected,
            SpanAnalyzer.Result actual,
            Span exParent,
            Span acParent,
            List<Span> ex,
            List<Span> ac) {
        Boolean[][] compatibility = new Boolean[ex.size()][ac.size()];

        for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
            for (int acIndex = 0; acIndex < ac.size(); acIndex++) {
                long numFailures = compareSpansRecursively(expected, actual, ex.get(exIndex), ac.get(acIndex)).count();
                compatibility[exIndex][acIndex] = numFailures == 0;
            }
        }

        // check rows first
        for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
            Boolean[] compatibilityRow = compatibility[exIndex];
            boolean rowContainedAtLeastOneSuccess = Arrays.stream(compatibilityRow).anyMatch(Boolean::booleanValue);

            if (!rowContainedAtLeastOneSuccess) {
                return Stream.of(ComparisonFailure.unequalChildren(exParent, acParent, ex, ac));
            }
        }

        // check columns
        for (int acIndex = 0; acIndex < ac.size(); acIndex++) {
            boolean atLeastOneCompatible = false;
            for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
                atLeastOneCompatible |= compatibility[exIndex][acIndex];
            }

            if (!atLeastOneCompatible) {
                return Stream.of(ComparisonFailure.unequalChildren(exParent, acParent, ex, ac));
            }
        }


        return Stream.empty(); // no errors, everything was compatible
    }

    private static String renderFailure(ComparisonFailure failure) {
        return failure.map(
                (ComparisonFailure.unequalOperation t) -> String.format("Expected operation %s but received %s",
                        t.expected().getOperation(), t.actual().getOperation()),
                (ComparisonFailure.unequalChildren t) -> String.format(
                        "Expected children with operations %s but received %s",
                        t.expectedChildren().stream().map(Span::getOperation).collect(ImmutableList.toImmutableList()),
                        t.actualChildren().stream().map(Span::getOperation).collect(ImmutableList.toImmutableList())),
                (ComparisonFailure.incompatibleStructure t) -> String.format(
                        "Expected children to structured similarly"));
    }

    /* Assumes list of spans to be ordered by startTimeMicros */
    private static boolean containsOverlappingSpans(List<Span> spans) {
        if (!spans.isEmpty()) {
            Span currentSpan = spans.get(0);
            for (int i = 1; i < spans.size(); i++) {
                Span nextSpan = spans.get(i);
                if (nextSpan.getStartTimeMicroSeconds() < getEndTimeMicroSeconds(currentSpan)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static long getEndTimeMicroSeconds(Span span) {
        return span.getStartTimeMicroSeconds() + (span.getDurationNanoSeconds() * 1000);
    }

    private static String testName(ExtensionContext context) {
        return context.getRequiredTestClass().getSimpleName() + "/" + context.getRequiredTestMethod().getName();
    }

    @SuppressWarnings("checkstyle:TypeName")
    @DataEnum
    interface ComparisonFailure_dataenum {
        dataenum_case unequalOperation(Span expected, Span actual);

        dataenum_case unequalChildren(
                Span expected, Span actual, List<Span> expectedChildren, List<Span> actualChildren);

        dataenum_case incompatibleStructure(Span expected, Span actual);
    }
}
