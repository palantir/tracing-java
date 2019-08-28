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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TestTracingExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Logger log = LoggerFactory.getLogger(TestTracingExtension.class);
    private final TestTracingSubscriber subscriber = new TestTracingSubscriber();

    @Override
    public void beforeEach(ExtensionContext context) {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.subscribe(testName(context), subscriber);
        // TODO(dfox): sample can be modified by other code, we should be try ensure that the trace is always sampled
        // for the lifetime of the test
        // TODO(dfox): clear existing tracing??
        // TODO(forozco): cleanup stale snapshots from outdated tests cases/classes
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        String name = testName(context);
        Tracer.unsubscribe(name);
        Path outputPath = getOutputPath(name);
        Path snapshotFile = Paths.get("src/test/resources/tracing").resolve(name + ".log");
        Files.createDirectories(outputPath);
        Path actualPath = outputPath.resolve("actual.html");
        Path expectedPath = outputPath.resolve("expected.html");

        TestTracing annotation = AnnotationSupport.findAnnotation(context.getRequiredTestMethod(), TestTracing.class)
                .orElseThrow(() -> new RuntimeException("Expected " + name + " to be annotated with @TestTracing"));

        Collection<Span> actualSpans = subscriber.getAllSpans();

        if (!annotation.snapshot()) {
            HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                    .spans(actualSpans)
                    .path(actualPath)
                    .displayName(name)
                    .layoutStrategy(annotation.layout())
                    .build());
            log.info("Tracing report file://{}", actualPath.toAbsolutePath());
            return;
        }

        // match recorded traces against expected file (or create)
        if (!Files.exists(snapshotFile) || Boolean.valueOf(System.getProperty("recreate", "false"))) {
            Serialization.serialize(snapshotFile, actualSpans);
            HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                    .spans(actualSpans)
                    .path(actualPath)
                    .displayName(name)
                    .layoutStrategy(annotation.layout())
                    .build());
            log.info("Tracing report file://{}", actualPath.toAbsolutePath());
            return;
        }

        // TODO(df0x): filter for just one traceId (??) to figure out concurrency
        List<Span> expectedSpans = Serialization.deserialize(snapshotFile);
        SpanAnalyzer.Result expected = SpanAnalyzer.analyze(expectedSpans);
        SpanAnalyzer.Result actual = SpanAnalyzer.analyze(actualSpans);

        Set<ComparisonFailure> failures = SpanAnalyzer.compareSpansRecursively(
                expected, actual, expected.root(), actual.root()).collect(ImmutableSet.toImmutableSet());

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
                .layoutStrategy(annotation.layout())
                .build());


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
                .layoutStrategy(annotation.layout())
                .build());

        if (!failures.isEmpty()) {
            throw new AssertionError(
                    String.format(
                            "Traces did not match the expected file '%s'.\n"
                                    + "%s\n"
                                    + "Visually Compare:\n"
                                    + " - expected: file://%s\n"
                                    + " - actual:   file://%s\n"
                                    + "Or re-run with -Drecreate=true to accept the new behaviour.",
                            snapshotFile,
                            failures.stream()
                                    .map(TestTracingExtension::renderFailure)
                                    .collect(Collectors.joining("\n")),
                            expectedPath.toAbsolutePath(),
                            actualPath.toAbsolutePath()));
        }
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

    private static String testName(ExtensionContext context) {
        return context.getRequiredTestClass().getSimpleName() + "/" + context.getRequiredTestMethod().getName();
    }

    private static Path getOutputPath(String name) {
        String circleArtifactsDir = System.getenv("CIRCLE_ARTIFACTS");
        if (circleArtifactsDir == null) {
            return Paths.get("build/reports/tracing").resolve(name);
        }

        return Paths.get(circleArtifactsDir).resolve(name);
    }

}
