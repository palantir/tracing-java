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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.hash.Hashing;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"PreferSafeLoggableExceptions", "Slf4jLogsafeArgs"}) // test-lib, no need for SafeArgs
final class SpanRenderer implements SpanObserver {

    private static final Logger log = LoggerFactory.getLogger(SpanRenderer.class);
    private final Collection<Span> allSpans = new ArrayBlockingQueue<>(1000);

    @Override
    public void consume(Span span) {
        allSpans.add(span);
    }

    @SuppressWarnings({"JavaTimeDefaultTimeZone", "BanSystemOut"}) // I actually want the system default time zone!
    void output(String displayName, Path path) {
        TimeBounds bounds = bounds(allSpans);

        Map<String, List<Span>> spansByTraceId = allSpans.stream()
                .collect(Collectors.groupingBy(Span::getTraceId));

        Map<String, AnalyzedSpans> analyzedByTraceId = Maps.transformValues(spansByTraceId, SpanRenderer::analyze);

        HtmlFormatter formatter = new HtmlFormatter(bounds);
        StringBuilder sb = new StringBuilder();

        sb.append("<h1>");
        sb.append(displayName);
        sb.append("</h1>");
        sb.append("<p>");
        sb.append(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                .format(LocalDateTime.now(Clock.systemDefaultZone())));
        sb.append("</p>");
        analyzedByTraceId.entrySet()
                .stream()
                .sorted(Comparator.comparingLong(e1 -> e1.getValue().bounds().startMicros()))
                .forEachOrdered(entry -> {
                    AnalyzedSpans analysis = entry.getValue();
                    formatter.renderAllSpansForOneTraceId(entry.getKey(), analysis, sb);
                });

        formatter.rawSpanJson(allSpans, sb);

        try {
            Files.write(
                    path,
                    sb.toString().getBytes(StandardCharsets.UTF_8));
            System.out.println(path.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        spansByTraceId.forEach((traceId, spans) -> {
            if (spans.size() > 1) {
                // I really don't think people want to see a visualization with one bar on it.
                AnalyzedSpans analysis = analyze(spans);

                // // emit HTML first
                // Path file = new HtmlFormatter(bounds).emitToTempFile(analysis.orderedSpans());
                // System.out.println("HTML span visualization: " + file);

                // emit ASCII
                AsciiFormatter ascii = new AsciiFormatter(bounds);
                for (Span span : analysis.orderedSpans()) {
                    System.out.println(ascii.formatSpan(span));
                }
            }
        });
    }

    @SuppressWarnings("BanSystemOut")
    private static AnalyzedSpans analyze(List<Span> spans) {
        // every span is a node in a graph, each pointing to their parent with an edge
        MutableGraph<Span> graph = GraphBuilder.directed().build();
        spans.forEach(graph::addNode);

        // it's possible there's an unclosed parent, so we can make up a fake root span just in case we need it later
        Span fakeRootSpan = createFakeRootSpan(spans);

        Set<Span> collisions = new HashSet<>();

        Map<String, Span> spansBySpanId = spans.stream()
                .collect(Collectors.toMap(
                        Span::getSpanId,
                        Function.identity(),
                        (left, right) -> {
                            log.warn("Collision on span id {}", left.getSpanId());
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

        ImmutableList<Span> orderedspans = depthFirstTraversalOrderedByStartTime(graph, rootSpan)
                .filter(span -> !span.equals(fakeRootSpan))
                .collect(ImmutableList.toImmutableList());

        TimeBounds bounds = bounds(orderedspans);
        return new AnalyzedSpans() {
            @Override
            public Span rootSpan() {
                return rootSpan;
            }

            @Override
            public ImmutableList<Span> orderedSpans() {
                return orderedspans;
            }

            @Override
            public TimeBounds bounds() {
                return bounds;
            }

            @Override
            public Set<Span> collisions() {
                return Collections.unmodifiableSet(collisions);
            }
        };
    }

    interface AnalyzedSpans {
        Set<Span> collisions();
        TimeBounds bounds();
        Span rootSpan();
        ImmutableList<Span> orderedSpans();
    }

    private static Stream<Span> depthFirstTraversalOrderedByStartTime(MutableGraph<Span> graph, Span parentSpan) {
        Stream<Span> children = graph.incidentEdges(parentSpan).stream()
                // we only care about incoming edges to the 'parentSpan', not outgoing ones
                .filter(pair -> pair.nodeV().equals(parentSpan))
                .map(EndpointPair::nodeU)
                .sorted(Comparator.comparing(Span::getStartTimeMicroSeconds))
                .flatMap(child -> depthFirstTraversalOrderedByStartTime(graph, child));

        return Stream.concat(Stream.of(parentSpan), children);
    }

    /** Synthesizes a root span which encapsulates all known spans. */
    private static Span createFakeRootSpan(List<Span> spans) {
        TimeBounds bounds = bounds(spans);
        return Span.builder()
                .type(SpanType.LOCAL)
                .startTimeMicroSeconds(bounds.startMicros())
                .durationNanoSeconds(bounds.endNanos() - bounds.startNanos())
                .spanId("???")
                .traceId("???")
                .operation("<unknown root span>")
                .build();
    }

    private static TimeBounds bounds(Collection<Span> spans) {
        long earliestStartMicros = spans.stream().mapToLong(Span::getStartTimeMicroSeconds).min().getAsLong();
        long latestEndNanos = spans.stream()
                .mapToLong(span -> {
                    long startTimeNanos = TimeUnit.NANOSECONDS.convert(
                            span.getStartTimeMicroSeconds(), TimeUnit.MICROSECONDS);
                    return startTimeNanos + span.getDurationNanoSeconds();
                })
                .max()
                .getAsLong();
        return new TimeBounds() {
            @Override
            public long startMicros() {
                return earliestStartMicros;
            }

            @Override
            public long endNanos() {
                return latestEndNanos;
            }
        };
    }

    interface TimeBounds {
        long startMicros();
        long endNanos();
        default long startNanos() {
            return TimeUnit.NANOSECONDS.convert(startMicros(), TimeUnit.MICROSECONDS);
        }
        default long durationNanos() {
            return endNanos() - startNanos();
        }
        default long durationMicros() {
            return TimeUnit.MICROSECONDS.convert(durationNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private static float percentage(long numerator, long denominator) {
        return 100f * numerator / denominator;
    }

    private static final class HtmlFormatter {

        private static final ObjectWriter writer = new ObjectMapper().registerModule(new Jdk8Module()).writer();
        private final TimeBounds bounds;

        HtmlFormatter(TimeBounds bounds) {
            this.bounds = bounds;
        }

        public void renderAllSpansForOneTraceId(String traceId, AnalyzedSpans analysis, StringBuilder sb) {
            sb.append("<div style=\"border-top: 1px solid #E1E8ED\" title=\"" + traceId + "\">\n");
            analysis.orderedSpans().forEach(span -> {
                boolean suspectedCollision = analysis.collisions().contains(span);
                sb.append(formatSpan(span, suspectedCollision));
            });
            sb.append("</div>\n");
        }

        private String formatSpan(Span span, boolean suspectedCollision) {
            long transposedStartMicros = span.getStartTimeMicroSeconds() - bounds.startMicros();

            long hue = Hashing.adler32().hashString(span.getTraceId(), StandardCharsets.UTF_8).padToLong() % 360;

            return String.format(
                    "<div style=\"position: relative; "
                            + "left: %s%%; "
                            + "width: %s%%; "
                            + "background: hsl(%s, 80%%, 80%%); "
                            + "box-shadow: -1px 0px 0px 1.5px hsl(%s, 80%%, 80%%); "
                            + "color: #293742; "
                            + "white-space: nowrap; "
                            + "font-family: monospace; \""
                            + "title=\"%s start: %s, finish: %s\">"
                            + "%s - %s%s"
                            + "</div>\n",
                    percentage(transposedStartMicros, bounds.durationMicros()),
                    percentage(span.getDurationNanoSeconds(), bounds.durationNanos()),
                    hue,
                    hue,
                    span.getTraceId(),
                    renderDuration(transposedStartMicros, TimeUnit.MICROSECONDS),
                    renderDuration(transposedStartMicros + TimeUnit.MICROSECONDS.convert(
                            span.getDurationNanoSeconds(),
                            TimeUnit.NANOSECONDS), TimeUnit.MICROSECONDS),
                    span.getOperation(),
                    renderDuration(span.getDurationNanoSeconds(), TimeUnit.NANOSECONDS),
                    suspectedCollision ? " (collision)" : "");
        }


        public void rawSpanJson(Collection<Span> spans, StringBuilder sb) {
            sb.append("\n<pre style=\"background: #CED9E0;"
                    + "color: #738694;"
                    + "padding: 30px;"
                    + "overflow-x: scroll;"
                    + "margin-top: 100px;\">");
            spans.stream().sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds)).forEach(s -> {
                try {
                    sb.append('\n');
                    sb.append(writer.writeValueAsString(s));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Unable to JSON serialize span " + s, e);
                }
            });
            sb.append("\n</pre>");
        }
    }

    private static class AsciiFormatter {
        private final TimeBounds bounds;

        AsciiFormatter(TimeBounds bounds) {
            this.bounds = bounds;
        }

        public String formatSpan(Span span) {
            long transposedStartMicros = span.getStartTimeMicroSeconds() - bounds.startMicros();
            float leftPercentage = percentage(transposedStartMicros, bounds.durationMicros());
            float widthPercentage = percentage(span.getDurationNanoSeconds(), bounds.durationNanos());

            int numSpaces = (int) Math.floor(leftPercentage);
            int numHashes = (int) Math.floor(widthPercentage);

            String spaces = Strings.repeat(" ", numSpaces);

            String name = span.getOperation().substring(0, Math.min(numHashes, span.getOperation().length()));
            String hashes = name + Strings.repeat("-", numHashes - name.length());

            return spaces + (hashes.isEmpty() ? "|" : hashes);
        }
    }

    private static String renderDuration(float amount, TimeUnit timeUnit) {
        ImmutableMap<TimeUnit, TimeUnit> largerUnit = ImmutableMap.<TimeUnit, TimeUnit>builder()
                .put(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS)
                .put(TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS)
                .put(TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
                .build();

        ImmutableMap<TimeUnit, String> abbreviation = ImmutableMap.<TimeUnit, String>builder()
                .put(TimeUnit.NANOSECONDS, "ns")
                .put(TimeUnit.MICROSECONDS, "micros")
                .put(TimeUnit.MILLISECONDS, "ms")
                .put(TimeUnit.SECONDS, "s")
                .build();

        TimeUnit bigger = largerUnit.get(timeUnit);
        if (amount >= 1000 && bigger != null) {
            return renderDuration(amount / 1000, bigger);
        }

        return String.format("%.2f %s", amount, abbreviation.get(timeUnit));
    }
}
