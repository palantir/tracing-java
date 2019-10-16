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

package com.palantir.tracing.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/** Copy of {@link com.palantir.tracing.api.Span}, just with jackson annotations. */
@Value.Immutable
@JsonDeserialize(as = ImmutableSerializableSpan.class)
abstract class SerializableSpan {

    public abstract String getTraceId();

    public abstract Optional<String> getParentSpanId();

    public abstract String getSpanId();

    @Value.Default
    public SpanType getType() {
        return SpanType.LOCAL;
    }

    public abstract String getOperation();

    public abstract long getStartTimeMicroSeconds();

    public abstract long getDurationNanoSeconds();

    public abstract Map<String, String> getMetadata();

    public Span asSpan() {
        return Span.builder()
                .traceId(getTraceId())
                .parentSpanId(getParentSpanId())
                .spanId(getSpanId())
                .type(getType())
                .operation(getOperation())
                .startTimeMicroSeconds(getStartTimeMicroSeconds())
                .durationNanoSeconds(getDurationNanoSeconds())
                .metadata(getMetadata())
                .build();
    }

    public static SerializableSpan fromSpan(Span span) {
        return ImmutableSerializableSpan.builder()
                .traceId(span.getTraceId())
                .parentSpanId(span.getParentSpanId())
                .spanId(span.getSpanId())
                .type(span.type())
                .operation(span.getOperation())
                .startTimeMicroSeconds(span.getStartTimeMicroSeconds())
                .durationNanoSeconds(span.getDurationNanoSeconds())
                .metadata(span.getMetadata())
                .build();
    }
}
