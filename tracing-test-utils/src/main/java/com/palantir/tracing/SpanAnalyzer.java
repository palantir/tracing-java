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

    public static Result analyze(Collection<Span> spans) {
        ImmutableGraph.Builder<Span> graph = GraphBuilder.directed().immutable();
        // MutableGraph<Span> graph = immutable.build();
        spans.forEach(graph::addNode);

        // it's possible there's an unclosed parent, so we can make up a fake root span just in case we need it later
        Span fakeRootSpan = createFakeRootSpan(spans);

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

        Span rootSpan = spansBySpanId.values().stream()
                .filter(span -> !span.getParentSpanId().isPresent())
                .findFirst()
                .orElse(fakeRootSpan);

        // set up edges:
        for (Span span : spans) {
            // the root node will have no parentSpanId
            span.getParentSpanId().ifPresent(parentSpanId -> {
                Optional<Span> parentSpan = Optional.ofNullable(spansBySpanId.get(parentSpanId));

                if (!parentSpan.isPresent()) {
                    // people do crazy things with traces - they might have a trace already initialized which doesn't
                    // get closed (and therefore emitted) by the time we need to render, so just hook it up to the fake
                    graph.putEdge(span, fakeRootSpan);
                    return;
                }

                graph.putEdge(span, parentSpan.get());
            });
        }

        ImmutableGraph<Span> spanGraph = graph.build();

        ImmutableList<Span> orderedspans = depthFirstTraversalOrderedByStartTime(spanGraph, rootSpan)
                .filter(span -> !span.equals(fakeRootSpan))
                .collect(ImmutableList.toImmutableList());

        TimeBounds bounds = TimeBounds.fromSpans(orderedspans);
        return new Result() {

            @Override
            public ImmutableGraph<Span> graph() {
                return spanGraph;
            }

            @Override
            public Span root() {
                return rootSpan;
            }

            @Override
            public Set<Span> collisions() {
                return collisions;
            }

            @Override
            public TimeBounds bounds() {
                return bounds;
            }

            @Override
            public ImmutableList<Span> orderedSpans() {
                return orderedspans;
            }
        };
    }

    interface Result {
        ImmutableGraph<Span> graph();
        Span root();
        Set<Span> collisions();
        TimeBounds bounds();
        ImmutableList<Span> orderedSpans();
    }

    /** Synthesizes a root span which encapsulates all known spans. */
    private static Span createFakeRootSpan(Collection<Span> spans) {
        TimeBounds bounds = TimeBounds.fromSpans(spans);
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
