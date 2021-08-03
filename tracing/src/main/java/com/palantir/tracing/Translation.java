/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;

final class Translation {

    private Translation() {}

    static SpanKind toOpenTelemetry(SpanType palantirType) {
        switch (palantirType) {
            case SERVER_INCOMING:
                return SpanKind.SERVER;
            case CLIENT_OUTGOING:
                return SpanKind.CLIENT;
            case LOCAL:
                return SpanKind.INTERNAL;
        }
        throw new UnsupportedOperationException();
    }

    static SpanType fromOpenTelemetryLossy(SpanKind openTelemetryKind) {
        switch (openTelemetryKind) {
            case INTERNAL:
                return SpanType.LOCAL;
            case SERVER:
                return SpanType.SERVER_INCOMING;
            case CLIENT:
                return SpanType.CLIENT_OUTGOING;
            case PRODUCER:
                return SpanType.LOCAL;
            case CONSUMER:
                return SpanType.LOCAL;
        }
        throw new UnsupportedOperationException();
    }

    public static Span fromOpenTelemetry(ReadableSpan span) {
        SpanData spanData = span.toSpanData();

        Span.Builder spanBuilder = Span.builder()
                .traceId(spanData.getTraceId())
                .spanId(spanData.getSpanId())
                .type(fromOpenTelemetryLossy(spanData.getKind()))
                .operation(spanData.getName())
                .startTimeMicroSeconds(toEpochMicros(spanData.getStartEpochNanos()))
                .durationNanoSeconds(Math.max(1, spanData.getEndEpochNanos() - spanData.getStartEpochNanos()));

        if (!spanData.getParentSpanId().equals(SpanId.getInvalid())) {
            spanBuilder.parentSpanId(spanData.getParentSpanId());
        }

        spanData.getAttributes()
                .forEach((key, value) -> spanBuilder.putMetadata(key.getKey(), valueToString(key, value)));

        return spanBuilder.build();
    }

    private static String valueToString(AttributeKey<?> key, Object attributeValue) {
        AttributeType type = key.getType();
        switch (type) {
            case STRING:
            case BOOLEAN:
            case LONG:
            case DOUBLE:
                return String.valueOf(attributeValue);
            case STRING_ARRAY:
            case BOOLEAN_ARRAY:
            case LONG_ARRAY:
            case DOUBLE_ARRAY:
                return commaSeparated((List<?>) attributeValue);
        }
        throw new IllegalStateException("Unknown attribute type: " + type);
    }

    private static String commaSeparated(List<?> values) {
        StringBuilder builder = new StringBuilder();
        for (Object value : values) {
            if (builder.length() != 0) {
                builder.append(',');
            }
            builder.append(value);
        }
        return builder.toString();
    }

    private static long toEpochMicros(long epochNanos) {
        return NANOSECONDS.toMicros(epochNanos);
    }
}
