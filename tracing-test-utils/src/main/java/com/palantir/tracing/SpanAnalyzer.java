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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.immutables.value.Value;

final class SpanAnalyzer {

    private SpanAnalyzer() {}

    private static Stream<Span> depthFirstTraversalOrderedByStartTime(ImmutableGraph<Span> graph, Span parentSpan) {
        Stream<Span> children = children(graph, parentSpan)
                .flatMap(child -> depthFirstTraversalOrderedByStartTime(graph, child));

        return Stream.concat(Stream.of(parentSpan), children);
    }

    public static Stream<Span> children(ImmutableGraph<Span> graph, Span parentSpan) {
        return graph.incidentEdges(parentSpan).stream()
                // we only care about incoming edges to the 'parentSpan', not outgoing ones
                .filter(pair -> pair.nodeV().equals(parentSpan))
                .map(EndpointPair::nodeU)
                .sorted(Comparator.comparing(Span::getStartTimeMicroSeconds));
    }

    // TODO(dfox): make sure we don't re-run this unnecessarily
    public static Result analyze(Collection<Span> spans) {
        TimeBounds bounds = TimeBounds.fromSpans(spans);
        Span fakeRootSpan = createFakeRootSpan(bounds);

        Set<Span> collisions = new HashSet<>();
        Map<String, Span> spansBySpanId = spans.stream()
                .collect(Collectors.toMap(
                        Span::getSpanId,
                        Function.identity(),
                        (left, right) -> {
                            collisions.add(left);
                            collisions.add(right);
                            return left;
                        }));

        Set<Span> parentlessSpans = spansBySpanId.values().stream()
                .filter(span -> span.getParentSpanId().isPresent())
                .collect(ImmutableSet.toImmutableSet());

        // We want to ensure that there is always a single root span to base our graph traversal off of
        Span rootSpan;
        if (parentlessSpans.size() != 1) {
            rootSpan = fakeRootSpan;
        } else {
            rootSpan = Iterables.getOnlyElement(parentlessSpans);
        }

        // people do crazy things with traces - they might have a trace already initialized which doesn't
        // get closed (and therefore emitted) by the time we need to render, so just hook it up to the fake
        ImmutableGraph.Builder<Span> graph = GraphBuilder.directed().immutable();
        spans.stream()
                .filter(span -> !span.getSpanId().equals(rootSpan.getSpanId()))
                .forEach(span -> graph.putEdge(
                        span,
                        span.getParentSpanId()
                                .flatMap(parentSpanId -> Optional.ofNullable(spansBySpanId.get(parentSpanId)))
                                .orElse(fakeRootSpan)));
        ImmutableGraph<Span> spanGraph = graph.build();

        return ImmutableResult.builder()
                .graph(spanGraph)
                .root(rootSpan)
                .collisions(collisions)
                .bounds(bounds)
                .build();
    }

    public static Map<String, Result> analyzeByTraceId(Collection<Span> spans) {
        Map<String, List<Span>> spansByTraceId = spans.stream()
                .collect(Collectors.groupingBy(Span::getTraceId));

        return Maps.transformValues(spansByTraceId, SpanAnalyzer::analyze);
    }

    static Stream<ComparisonFailure> compareSpansRecursively(
            Result expected,
            Result actual,
            Span ex,
            Span ac) {
        if (!ex.getOperation().equals(ac.getOperation())) {
            return Stream.of(ComparisonFailure.unequalOperation(ex, ac));
        }
        // other fields, type, params, metadata(???)

        // ensure we have the same number of children, same child operation names in the same order
        List<Span> sortedExpectedChildren = children(expected.graph(), ex)
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .collect(ImmutableList.toImmutableList());
        List<Span> sortedActualChildren = children(actual.graph(), ac)
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

        if (actualContainsOverlappingSpans && !compatibleOverlappingSpans(
                expected, actual, sortedExpectedChildren, sortedActualChildren)) {
            return Stream.of(ComparisonFailure.unequalChildren(ex, ac, sortedExpectedChildren, sortedActualChildren));
        }

        return IntStream.range(0, sortedActualChildren.size())
                .mapToObj(i -> compareSpansRecursively(
                        expected,
                        actual,
                        sortedExpectedChildren.get(i),
                        sortedActualChildren.get(i)))
                .flatMap(Function.identity());
    }

    /**
     * When async spans are involved, there can be many overlapping children with the same operation name.
     * We exhaustively check each possible pair, and require that each span in the 'expected' list lines up with
     * something and each span in the 'actual' list also lines up with something.
     *
     * It's OK for some spans to be compatible with more than one span (as subtrees could be identical).
     */
    private static boolean compatibleOverlappingSpans(
            Result expected, Result actual, List<Span> ex, List<Span> ac) {
        boolean[][] compatibility = new boolean[ex.size()][ac.size()];

        for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
            for (int acIndex = 0; acIndex < ac.size(); acIndex++) {
                long numFailures = compareSpansRecursively(expected, actual, ex.get(exIndex), ac.get(acIndex)).count();
                compatibility[exIndex][acIndex] = numFailures == 0;
            }
        }

        // check rows first
        for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
            boolean atLeastOneCompatible = false;
            for (int acIndex = 0; acIndex < ac.size(); acIndex++) {
                atLeastOneCompatible |= compatibility[exIndex][acIndex];
            }

            if (!atLeastOneCompatible) {
                return false;
            }
        }

        // check columns
        for (int acIndex = 0; acIndex < ac.size(); acIndex++) {
            boolean atLeastOneCompatible = false;
            for (int exIndex = 0; exIndex < ex.size(); exIndex++) {
                atLeastOneCompatible |= compatibility[exIndex][acIndex];
            }

            if (!atLeastOneCompatible) {
                return false;
            }
        }

        return true;
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

    @Value.Immutable
    interface Result {
        ImmutableGraph<Span> graph();
        Span root();
        Set<Span> collisions();

        TimeBounds bounds();

        @Value.Lazy
        default ImmutableList<Span> orderedSpans() {
            return depthFirstTraversalOrderedByStartTime(graph(), root())
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /** Synthesizes a root span which encapsulates all known spans. */
    private static Span createFakeRootSpan(TimeBounds bounds) {
        return Span.builder()
                .type(SpanType.LOCAL)
                .startTimeMicroSeconds(bounds.startMicros())
                .durationNanoSeconds(bounds.endNanos() - bounds.startNanos())
                .spanId("???")
                .traceId("???")
                .operation("<unknown root span>")
                .build();
    }
}
