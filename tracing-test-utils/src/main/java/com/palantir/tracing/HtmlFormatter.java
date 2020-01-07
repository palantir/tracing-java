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

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

final class HtmlFormatter {
    private RenderConfig config;

    private HtmlFormatter(RenderConfig config) {
        this.config = config;
    }

    @Value.Immutable
    interface RenderConfig {
        Collection<Span> spans();

        Path path();

        String displayName();

        LayoutStrategy layoutStrategy();

        Set<String> problemSpanIds();

        @Value.Derived
        default TimeBounds bounds() {
            return TimeBounds.fromSpans(spans());
        }

        class Builder extends ImmutableRenderConfig.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }

    public static void render(RenderConfig config) throws IOException {
        HtmlFormatter formatter = new HtmlFormatter(config);

        StringBuilder sb = new StringBuilder();
        formatter.header(sb);

        switch (config.layoutStrategy()) {
            case CHRONOLOGICAL:
                formatter.renderChronological(sb);
                break;
            case SPLIT_BY_TRACE:
                formatter.renderSplitByTraceId(sb);
                break;
        }

        formatter.rawSpanJson(sb);

        Files.write(config.path(), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void renderChronological(StringBuilder sb) {
        config.spans().stream()
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .forEachOrdered(span -> formatSpan(span, false, sb));
    }

    private void renderSplitByTraceId(StringBuilder sb) {
        Map<String, SpanAnalyzer.Result> analyzedByTraceId = SpanAnalyzer.analyzeByTraceId(config.spans());
        analyzedByTraceId.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getValue().bounds()))
                .forEachOrdered(entry -> {
                    SpanAnalyzer.Result analysis = entry.getValue();
                    renderAllSpansForOneTraceId(entry.getKey(), analysis, sb);
                });
    }

    private void renderAllSpansForOneTraceId(String traceId, SpanAnalyzer.Result analysis, StringBuilder sb) {
        sb.append("<div style=\"border-top: 1px solid #E1E8ED\" title=\"" + traceId + "\">\n");
        analysis.orderedSpans().stream()
                .filter(s -> !SpanAnalyzer.isSyntheticRoot(s))
                .forEach(span -> {
                    boolean suspectedCollision = analysis.collisions().contains(span);
                    formatSpan(span, suspectedCollision, sb);
                });
        sb.append("</div>\n");
    }

    private void header(StringBuilder sb) throws IOException {
        OffsetDateTime startTime = Instant.ofEpochMilli(
                        TimeUnit.MILLISECONDS.convert(config.bounds().startMicros(), TimeUnit.MICROSECONDS))
                .atOffset(ZoneOffset.UTC);
        OffsetDateTime endTime = Instant.ofEpochMilli(
                        TimeUnit.MILLISECONDS.convert(config.bounds().endNanos(), TimeUnit.NANOSECONDS))
                .atOffset(ZoneOffset.UTC);
        sb.append(template(
                "header.html",
                ImmutableMap.<String, String>builder()
                        .put("{{DISPLAY_NAME}}", config.displayName())
                        .put("{{START_TIME}}", DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(startTime))
                        .put("{{END_TIME}}", DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(endTime))
                        .build()));
    }

    private void formatSpan(Span span, boolean suspectedCollision, StringBuilder sb) {
        long transposedStartMicros = span.getStartTimeMicroSeconds() - config.bounds().startMicros();

        long hue = Hashing.adler32().hashString(span.getTraceId(), StandardCharsets.UTF_8).padToLong() % 360;

        sb.append(template(
                "span.html",
                ImmutableMap.<String, String>builder()
                        .put(
                                "{{LEFT}}",
                                Float.toString(Utils.percent(transposedStartMicros, config.bounds().durationMicros())))
                        .put(
                                "{{WIDTH}}",
                                Float.toString(
                                        Utils.percent(span.getDurationNanoSeconds(), config.bounds().durationNanos())))
                        .put("{{HUE}}", Long.toString(hue))
                        .put("{{SPANID}}", span.getSpanId())
                        .put("{{CLASS}}", config.problemSpanIds().contains(span.getSpanId()) ? "problem-span" : "")
                        .put("{{START}}", Utils.renderDuration(transposedStartMicros, TimeUnit.MICROSECONDS))
                        .put(
                                "{{FINISH}}",
                                Utils.renderDuration(
                                        transposedStartMicros
                                                + TimeUnit.MICROSECONDS.convert(
                                                        span.getDurationNanoSeconds(), TimeUnit.NANOSECONDS),
                                        TimeUnit.MICROSECONDS))
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
        config.spans().stream()
                .sorted(Comparator.comparingLong(Span::getStartTimeMicroSeconds))
                .forEach(s -> {
                    sb.append('\n');
                    sb.append(Serialization.toString(s));
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
}
