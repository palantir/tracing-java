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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.io.UncheckedIOException;
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
    private final Builder builder;
    private final TimeBounds bounds;

    private HtmlFormatter(Builder builder, TimeBounds bounds) {
        this.bounds = bounds;
        this.builder = builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static void render(Builder builder) throws IOException {
        StringBuilder sb = new StringBuilder();

        HtmlFormatter formatter = new HtmlFormatter(builder, TimeBounds.fromSpans(builder.spans));
        formatter.header(sb);

        if (builder.chronological) {
            formatter.renderChronological(sb);
        } else {
            formatter.renderSplitByTraceId(sb);
        }

        formatter.rawSpanJson(sb);

        Files.write(builder.path, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void renderChronological(StringBuilder sb) {
        builder.spans.stream().sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds)).forEachOrdered(span -> {
            formatSpan(span, false, sb);
        });
    }

    private void renderSplitByTraceId(StringBuilder sb) {
        Map<String, List<Span>> spansByTraceId = builder.spans.stream()
                .collect(Collectors.groupingBy(Span::getTraceId));

        Map<String, SpanAnalyzer.Result> analyzedByTraceId =
                Maps.transformValues(spansByTraceId, SpanAnalyzer::analyze);
        analyzedByTraceId.entrySet()
                .stream()
                .sorted(Comparator.comparingLong(e1 -> e1.getValue().bounds().startMicros()))
                .forEachOrdered(entry -> {
                    SpanAnalyzer.Result analysis = entry.getValue();
                    renderAllSpansForOneTraceId(entry.getKey(), analysis, sb);
                });
    }

    private void renderAllSpansForOneTraceId(String traceId, SpanAnalyzer.Result analysis, StringBuilder sb) {
        sb.append("<div style=\"border-top: 1px solid #E1E8ED\" title=\"" + traceId + "\">\n");
        analysis.orderedSpans().forEach(span -> {
            boolean suspectedCollision = analysis.collisions().contains(span);
            formatSpan(span, suspectedCollision, sb);
        });
        sb.append("</div>\n");
    }

    @SuppressWarnings("JavaTimeDefaultTimeZone") // I actually want the system default time zone!
    private void header(StringBuilder sb) throws IOException {
        sb.append(template("header.html", ImmutableMap.<String, String>builder()
                .put("{{DISPLAY_NAME}}", builder.displayName)
                .put("{{DATE}}",
                        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                                .format(LocalDateTime.now(Clock.systemDefaultZone())))
                .build()));
    }

    private void formatSpan(Span span, boolean suspectedCollision, StringBuilder sb) {
        long transposedStartMicros = span.getStartTimeMicroSeconds() - bounds.startMicros();

        long hue = Hashing.adler32().hashString(span.getTraceId(), StandardCharsets.UTF_8).padToLong() % 360;

        sb.append(template("span.html", ImmutableMap.<String, String>builder()
                .put("{{LEFT}}", Float.toString(Utils.percent(transposedStartMicros, bounds.durationMicros())))
                .put("{{WIDTH}}", Float.toString(Utils.percent(span.getDurationNanoSeconds(), bounds.durationNanos())))
                .put("{{HUE}}", Long.toString(hue))
                .put("{{TRACEID}}", span.getTraceId())
                .put("{{CLASS}}", builder.problemSpanIds.contains(span.getSpanId()) ? "problem-span" : "")
                .put("{{START}}", Utils.renderDuration(transposedStartMicros, TimeUnit.MICROSECONDS))
                .put("{{FINISH}}", Utils.renderDuration(transposedStartMicros + TimeUnit.MICROSECONDS.convert(
                        span.getDurationNanoSeconds(),
                        TimeUnit.NANOSECONDS), TimeUnit.MICROSECONDS))
                .put("{{OPERATION}}", span.getOperation())
                .put("{{DURATION}}", Utils.renderDuration(span.getDurationNanoSeconds(), TimeUnit.NANOSECONDS))
                .put("{{COLLISION}}", suspectedCollision ? " (collision)" : "")
                .build()));
    }


    private void rawSpanJson(StringBuilder sb) {
        sb.append("\n<pre style=\"background: #CED9E0;"
                + "color: #738694;"
                + "padding: 30px;"
                + "overflow-x: scroll;"
                + "margin-top: 100px;\">");
        builder.spans.stream().sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds)).forEach(s -> {
            try {
                sb.append('\n');
                sb.append(writer.writeValueAsString(s));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to JSON serialize span " + s, e);
            }
        });
        sb.append("\n</pre>");
    }

    private static String template(String resourceName, Map<String, String> values) {
        try {
            String template = Resources.toString(Resources.getResource(resourceName), StandardCharsets.UTF_8);
            for (Map.Entry<String, String> entry : values.entrySet()) {
                template = template.replace(entry.getKey(), entry.getValue());
            }
            return template;
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read resource " + resourceName, e);
        }
    }

    public static class Builder {
        private Collection<Span> spans;
        private Path path;
        private String displayName;
        private boolean chronological = true;
        private ImmutableSet<String> problemSpanIds;

        public Builder spans(Collection<Span> value) {
            this.spans = value;
            return this;
        }

        public Builder path(Path value) {
            this.path = value;
            return this;
        }

        public Builder displayName(String value) {
            this.displayName = value;
            return this;
        }

        public Builder chronological(boolean value) {
            this.chronological = value;
            return this;
        }

        public Builder problemSpanIds(ImmutableSet<String> value) {
            this.problemSpanIds = value;
            return this;
        }

        public void buildAndFormat() throws IOException {
            HtmlFormatter.render(this);
        }
    }
}
