/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.tracing.api.SpanType;
import java.util.Map;

/**
 * Wraps the {@link Tracer} methods in a closeable resource to enable the usage of the try-with-resources pattern.
 *
 * Usage:
 *   try (CloseableTracer trace = CloseableTracer.start("traceName")) {
 *       [...]
 *   }
 *
 */
public final class CloseableTracer implements AutoCloseable {

    private static final CloseableTracer INSTANCE = new CloseableTracer(ImmutableMap.of());

    private final Map<String, String> metadata;

    private CloseableTracer(Map<String, String> metadata) {
        this.metadata = ImmutableMap.copyOf(metadata);
    }

    /**
     * Opens a new span for this thread's call trace with a {@link SpanType#LOCAL LOCAL} span type, labeled with the
     * provided operation.
     */
    public static CloseableTracer startSpan(String operation) {
        return startSpan(operation, SpanType.LOCAL, ImmutableMap.of());
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType}, labeled with the provided
     * operation.
     */
    public static CloseableTracer startSpan(String operation, Map<String, String> metadata) {
        return startSpan(operation, SpanType.LOCAL, metadata);
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType}, labeled with the provided
     * operation.
     */
    public static CloseableTracer startSpan(String operation, SpanType spanType) {
        return startSpan(operation, spanType, ImmutableMap.of());
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType},
     * labeled with the provided operation, and closed with the provided metadata.
     *
     * If you need to a span that may complete on another thread, use {@link DetachedSpan#start} instead.
     */
    public static CloseableTracer startSpan(String operation, SpanType spanType, Map<String, String> metadata) {
        Tracer.fastStartSpan(operation, spanType);
        return metadata.isEmpty()
                ? INSTANCE
                : new CloseableTracer(metadata);
    }

    @Override
    public void close() {
        Tracer.fastCompleteSpan(metadata);
    }
}
