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

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class HtmlSpanObserver implements SpanObserver {

    private final Map<String, Span> spansBySpanId = new ConcurrentHashMap<>();

    @Override
    public void consume(Span span) {
        spansBySpanId.put(span.getSpanId(), span);
    }

    public void output() throws IOException {

        // TODO(dfox): assert we're only dealing with one traceid

        MutableGraph<Span> graph = GraphBuilder.directed()
                .build();

        for (Span span : spansBySpanId.values()) {
            graph.addNode(span);
            span.getParentSpanId().ifPresent(parent -> {
                graph.putEdge(span, spansBySpanId.get(parent));
            });
        }

        Span rootSpan = spansBySpanId.values().stream().filter(span -> !span.getParentSpanId().isPresent()).findFirst().get();
        Stream<Span> orderedSpans = depthFirstTraversalOrderedByStartTime(graph, rootSpan);

        StringBuilder stringBuilder = new StringBuilder();
        orderedSpans.forEachOrdered(span -> {
            stringBuilder.append(formatSpan(rootSpan, span));
        });

        Path file = Files.createTempFile("spans", ".html");
        Files.write(
                file,
                stringBuilder.toString().getBytes(StandardCharsets.UTF_8));

        System.out.println(file);
    }

    private Stream<Span> depthFirstTraversalOrderedByStartTime(MutableGraph<Span> graph, Span parentSpan) {
        Stream<Span> children = graph.incidentEdges(parentSpan).stream()
                .filter(pair -> pair.nodeV().equals(parentSpan))
                .map(pair -> pair.nodeU())
                .sorted(Comparator.comparing(Span::getStartTimeMicroSeconds))
                .flatMap(child -> depthFirstTraversalOrderedByStartTime(graph, child));

        return Stream.concat(Stream.of(parentSpan), children);
    }

    private static String formatSpan(Span rootSpan, Span span) {
        long rootDurationMicros = TimeUnit.MICROSECONDS.convert(
                rootSpan.getDurationNanoSeconds(),
                TimeUnit.NANOSECONDS);

        long transposedStartMicros = span.getStartTimeMicroSeconds() - rootSpan.getStartTimeMicroSeconds();

        long startMillis = TimeUnit.MILLISECONDS.convert(transposedStartMicros, TimeUnit.MICROSECONDS);
        return String.format("<div style=\"position: relative; "
                        + "left: %s%%; "
                        + "width: %s%%; "
                        + "background: grey;\""
                        + "title=\"start-time: %s millis, finish-time: %s millis\">"
                        + "%s"
                        + "</div>",
                percentage(transposedStartMicros, rootDurationMicros),
                percentage(span.getDurationNanoSeconds(), rootSpan.getDurationNanoSeconds()),
                startMillis,
                startMillis + TimeUnit.MILLISECONDS.convert(span.getDurationNanoSeconds(), TimeUnit.NANOSECONDS),
                span.getOperation());
    }

    private static float percentage(long numerator, long denominator) {
        return 100f * numerator / denominator;
    }
}
