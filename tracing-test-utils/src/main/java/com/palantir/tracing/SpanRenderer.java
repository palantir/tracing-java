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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings({"PreferSafeLoggableExceptions", "Slf4jLogsafeArgs"}) // test-lib, no need for SafeArgs
final class SpanRenderer implements SpanObserver {

    private final Collection<Span> allSpans = new ArrayBlockingQueue<>(1000);

    @Override
    public void consume(Span span) {
        allSpans.add(span);
    }

    @SuppressWarnings("BanSystemOut")
    void output(String displayName, Path path) {
        SpanAnalyzer.TimeBounds bounds = SpanAnalyzer.bounds(allSpans);

        Map<String, List<Span>> spansByTraceId = allSpans.stream()
                .collect(Collectors.groupingBy(Span::getTraceId));


        HtmlFormatter formatter = new HtmlFormatter(bounds, displayName);
        StringBuilder sb = new StringBuilder();

        formatter.header(sb);

        // TODO(dfox): switch betwen plain chrono and topological
        allSpans.stream().sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds)).forEachOrdered(span -> {
            formatter.formatSpan(span, false, sb);
        });

        Map<String, SpanAnalyzer.Result> analyzedByTraceId = Maps.transformValues(spansByTraceId, SpanAnalyzer::analyze);
        analyzedByTraceId.entrySet()
                .stream()
                .sorted(Comparator.comparingLong(e1 -> e1.getValue().bounds().startMicros()))
                .forEachOrdered(entry -> {
                    SpanAnalyzer.Result analysis = entry.getValue();
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
                SpanAnalyzer.Result analysis = SpanAnalyzer.analyze(spans);

                // emit ASCII
                AsciiFormatter ascii = new AsciiFormatter(bounds);
                for (Span span : analysis.orderedSpans()) {
                    System.out.println(ascii.formatSpan(span));
                }
            }
        });
    }

    private static float percentage(long numerator, long denominator) {
        return 100f * numerator / denominator;
    }

    private static final class HtmlFormatter {

        private static final ObjectWriter writer = new ObjectMapper().registerModule(new Jdk8Module()).writer();
        private final SpanAnalyzer.TimeBounds bounds;
        private String displayName;

        HtmlFormatter(SpanAnalyzer.TimeBounds bounds, String displayName) {
            this.bounds = bounds;
            this.displayName = displayName;
        }

        @SuppressWarnings("JavaTimeDefaultTimeZone") // I actually want the system default time zone!
        private void header(StringBuilder sb) {
            sb.append("<h1>");
            sb.append(displayName);
            sb.append("</h1>");
            sb.append("<p>");
            sb.append(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                    .format(LocalDateTime.now(Clock.systemDefaultZone())));
            sb.append("</p>");
        }

        public void renderAllSpansForOneTraceId(String traceId, SpanAnalyzer.Result analysis, StringBuilder sb) {
            sb.append("<div style=\"border-top: 1px solid #E1E8ED\" title=\"" + traceId + "\">\n");
            analysis.orderedSpans().forEach(span -> {
                boolean suspectedCollision = analysis.collisions().contains(span);
                formatSpan(span, suspectedCollision, sb);
            });
            sb.append("</div>\n");
        }

        public void formatSpan(Span span, boolean suspectedCollision, StringBuilder sb) {
            long transposedStartMicros = span.getStartTimeMicroSeconds() - bounds.startMicros();

            long hue = Hashing.adler32().hashString(span.getTraceId(), StandardCharsets.UTF_8).padToLong() % 360;

            sb.append(String.format(
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
                    suspectedCollision ? " (collision)" : ""));
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
        private final SpanAnalyzer.TimeBounds bounds;

        AsciiFormatter(SpanAnalyzer.TimeBounds bounds) {
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
