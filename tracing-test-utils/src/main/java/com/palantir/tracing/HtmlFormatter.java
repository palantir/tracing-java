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
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.palantir.tracing.api.Span;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class HtmlFormatter {

    private static final ObjectWriter writer = new ObjectMapper().registerModule(new Jdk8Module()).writer();
    private final TimeBounds bounds;

    private HtmlFormatter(TimeBounds bounds) {
        this.bounds = bounds;
    }

    public static void renderByTraceId(Collection<Span> spans, Path path, String displayName) throws IOException {
        StringBuilder sb = new StringBuilder();

        HtmlFormatter formatter = new HtmlFormatter(TimeBounds.fromSpans(spans));
        formatter.header(displayName, sb);
        formatter.renderSplitByTraceId(spans, sb);
        formatter.rawSpanJson(spans, sb);

        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void renderChronologically(Collection<Span> spans, Path path, String displayName) throws IOException {
        StringBuilder sb = new StringBuilder();

        HtmlFormatter formatter = new HtmlFormatter(TimeBounds.fromSpans(spans));
        formatter.header(displayName, sb);
        formatter.renderChronological(spans, sb);
        formatter.rawSpanJson(spans, sb);

        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("JavaTimeDefaultTimeZone") // I actually want the system default time zone!
    private void header(String displayName, StringBuilder sb) throws IOException {
        String template = Resources.toString(Resources.getResource("header.html"), StandardCharsets.UTF_8);

        String date = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                .format(LocalDateTime.now(Clock.systemDefaultZone()));
        sb.append(template
                .replaceAll("\\{\\{DISPLAY_NAME\\}\\}", displayName)
                .replaceAll("\\{\\{DATE\\}\\}", date));
    }

    private void renderAllSpansForOneTraceId(String traceId, SpanAnalyzer.Result analysis, StringBuilder sb) {
        sb.append("<div style=\"border-top: 1px solid #E1E8ED\" title=\"" + traceId + "\">\n");
        analysis.orderedSpans().forEach(span -> {
            boolean suspectedCollision = analysis.collisions().contains(span);
            formatSpan(span, suspectedCollision, sb);
        });
        sb.append("</div>\n");
    }

    private void formatSpan(Span span, boolean suspectedCollision, StringBuilder sb) {
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
                Utils.percentage(transposedStartMicros, bounds.durationMicros()),
                Utils.percentage(span.getDurationNanoSeconds(), bounds.durationNanos()),
                hue,
                hue,
                span.getTraceId(),
                Utils.renderDuration(transposedStartMicros, TimeUnit.MICROSECONDS),
                Utils.renderDuration(transposedStartMicros + TimeUnit.MICROSECONDS.convert(
                        span.getDurationNanoSeconds(),
                        TimeUnit.NANOSECONDS), TimeUnit.MICROSECONDS),
                span.getOperation(),
                Utils.renderDuration(span.getDurationNanoSeconds(), TimeUnit.NANOSECONDS),
                suspectedCollision ? " (collision)" : ""));
    }


    private void rawSpanJson(Collection<Span> spans, StringBuilder sb) {
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

    private void renderChronological(Collection<Span> spans, StringBuilder sb) {
        spans.stream().sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds)).forEachOrdered(span -> {
            formatSpan(span, false, sb);
        });
    }

    void renderSplitByTraceId(Collection<Span> spans, StringBuilder sb) {

        Map<String, List<Span>> spansByTraceId = spans.stream()
                .collect(Collectors.groupingBy(Span::getTraceId));

        Map<String, SpanAnalyzer.Result> analyzedByTraceId = Maps.transformValues(spansByTraceId, SpanAnalyzer::analyze);
        analyzedByTraceId.entrySet()
                .stream()
                .sorted(Comparator.comparingLong(e1 -> e1.getValue().bounds().startMicros()))
                .forEachOrdered(entry -> {
                    SpanAnalyzer.Result analysis = entry.getValue();
                    renderAllSpansForOneTraceId(entry.getKey(), analysis, sb);
                });
    }
}
