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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class SpanRenderer implements SpanObserver {

    private final List<Span> allSpans = new ArrayList<>();

    @Override
    public synchronized void consume(Span span) {
        allSpans.add(span);
    }

    void output() {
        Map<String, List<Span>> distinctTraces = allSpans.stream().collect(Collectors.groupingBy(Span::getTraceId));
        distinctTraces.forEach((traceId, spans) -> {
            renderSingleTrace(spans);
        });
    }

    @SuppressWarnings("BanSystemOut")
    private void renderSingleTrace(List<Span> spans) {

        Map<String, Span> spansBySpanId = spans.stream()
                .collect(Collectors.toMap(Span::getSpanId, Function.identity()));

        MutableGraph<Span> graph = GraphBuilder.directed()
                .build();

        for (Span span : spansBySpanId.values()) {
            graph.addNode(span);
            span.getParentSpanId().ifPresent(parent -> {
                graph.putEdge(span, spansBySpanId.get(parent));
            });
        }

        Span rootSpan = spansBySpanId.values().stream()
                .filter(span -> !span.getParentSpanId().isPresent())
                .findFirst()
                .get();

        List<Span> orderedspans = depthFirstTraversalOrderedByStartTime(graph, rootSpan)
                .collect(ImmutableList.toImmutableList());

        // emit HTML first
        HtmlFormatter htmlFormatter = new HtmlFormatter(rootSpan);
        Path file = htmlFormatter.emitToTempFile(orderedspans);
        System.out.println("HTML span visualization: " + file);

        // emit ASCII
        AsciiFormatter ascii = new AsciiFormatter(rootSpan);
        for (Span span : orderedspans) {
            System.out.println(ascii.formatSpan(span));
        }
    }

    private Stream<Span> depthFirstTraversalOrderedByStartTime(MutableGraph<Span> graph, Span parentSpan) {
        Stream<Span> children = graph.incidentEdges(parentSpan).stream()
                .filter(pair -> pair.nodeV().equals(parentSpan))
                .map(pair -> pair.nodeU())
                .sorted(Comparator.comparing(Span::getStartTimeMicroSeconds))
                .flatMap(child -> depthFirstTraversalOrderedByStartTime(graph, child));

        return Stream.concat(Stream.of(parentSpan), children);
    }

    private static float percentage(long numerator, long denominator) {
        return 100f * numerator / denominator;
    }

    private static final class HtmlFormatter {
        private final Span rootSpan;

        HtmlFormatter(Span rootSpan) {
            this.rootSpan = rootSpan;
        }

        public String formatSpan(Span span) {
            long rootDurationMicros = TimeUnit.MICROSECONDS.convert(
                    rootSpan.getDurationNanoSeconds(),
                    TimeUnit.NANOSECONDS);

            long transposedStartMicros = span.getStartTimeMicroSeconds() - rootSpan.getStartTimeMicroSeconds();
            long startMillis = TimeUnit.MILLISECONDS.convert(transposedStartMicros, TimeUnit.MICROSECONDS);

            return String.format("<div style=\"position: relative; "
                            + "left: %s%%; "
                            + "width: %s%%; "
                            + "background: grey;\""
                            + "title=\"start-time: %s ms, finish-time: %s ms\">"
                            + "%s"
                            + "</div>",
                    percentage(transposedStartMicros, rootDurationMicros),
                    percentage(span.getDurationNanoSeconds(), rootSpan.getDurationNanoSeconds()),
                    startMillis,
                    startMillis + TimeUnit.MILLISECONDS.convert(
                            span.getDurationNanoSeconds(),
                            TimeUnit.NANOSECONDS),
                    span.getOperation());
        }

        public Path emitToTempFile(List<Span> spans) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Span span : spans) {
                stringBuilder.append(formatSpan(span));
            }
            try {
                Path file = Files.createTempFile("trace-" + rootSpan.getTraceId() + "-", ".html");
                Files.write(
                        file,
                        stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
                return file;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class AsciiFormatter {
        private final Span rootSpan;

        AsciiFormatter(Span rootSpan) {
            this.rootSpan = rootSpan;
        }

        public String formatSpan(Span span) {
            long rootDurationMicros = TimeUnit.MICROSECONDS.convert(
                    rootSpan.getDurationNanoSeconds(),
                    TimeUnit.NANOSECONDS);

            long transposedStartMicros = span.getStartTimeMicroSeconds() - rootSpan.getStartTimeMicroSeconds();
            float leftPercentage = percentage(transposedStartMicros, rootDurationMicros);
            float widthPercentage = percentage(span.getDurationNanoSeconds(), rootSpan.getDurationNanoSeconds());

            int numSpaces = (int) Math.floor(leftPercentage);
            int numHashes = (int) Math.floor(widthPercentage);

            String spaces = Strings.repeat(" ", numSpaces);

            String name = span.getOperation().substring(0, Math.min(numHashes, span.getOperation().length()));
            String hashes = name + Strings.repeat("-", numHashes - name.length());

            return spaces + hashes;
        }
    }
}
