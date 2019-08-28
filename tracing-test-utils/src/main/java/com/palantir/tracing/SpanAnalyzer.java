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
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
