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

import com.palantir.tracing.api.SpanType;
import java.util.Map;

/**
 * Wraps the {@link Tracer} methods in a closeable resource to enable the usage of the try-with-resources pattern.
 *
 * <p>Usage: try (CloseableTracer trace = CloseableTracer.start("traceName")) { [...] }
 */
public class CloseableTracer implements AutoCloseable {
    private static final CloseableTracer INSTANCE = new CloseableTracer();

    private CloseableTracer() {}

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     */
    public static CloseableTracer startSpan(String operation) {
        return startSpan(operation, SpanType.LOCAL);
    }

    /**
     * Opens a new {@link SpanType#LOCAL LOCAL} span for this thread's call trace, labeled with the provided operation.
     */
    public static CloseableTracer startSpan(String operation, Map<String, String> metadata) {
        return startSpan(operation, MapTagRecorder.INSTANCE, metadata, SpanType.LOCAL);
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType}, labeled with the provided
     * operation.
     *
     * <p>If you need to a span that may complete on another thread, use {@link DetachedSpan#start} instead.
     */
    public static CloseableTracer startSpan(String operation, SpanType spanType) {
        Tracer.fastStartSpan(operation, spanType);
        return INSTANCE;
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType}, labeled with the provided
     * operation. Equivalent to {@link #startSpan(String, TagRecorder, Object, SpanType)} with {@link SpanType#LOCAL}.
     *
     * <p>If you need to a span that may complete on another thread, use {@link DetachedSpan#start} instead.
     */
    public static <T> CloseableTracer startSpan(String operation, TagRecorder<? super T> recorder, T data) {
        return startSpan(operation, recorder, data, SpanType.LOCAL);
    }

    /**
     * Opens a new span for this thread's call trace with the provided {@link SpanType}, labeled with the provided
     * operation.
     *
     * <p>If you need to a span that may complete on another thread, use {@link DetachedSpan#start} instead.
     */
    public static <T> CloseableTracer startSpan(
            String operation, TagRecorder<? super T> recorder, T data, SpanType spanType) {
        Tracer.fastStartSpan(operation, spanType);
        if (!Tracer.isTraceObservable() || recorder.isEmpty(data)) {
            return INSTANCE;
        }
        return new TaggedCloseableTracer<>(recorder, data);
    }

    @Override
    public void close() {
        Tracer.fastCompleteSpan();
    }

    private static final class TaggedCloseableTracer<T> extends CloseableTracer {
        private final TagRecorder<? super T> recorder;
        private final T data;

        TaggedCloseableTracer(TagRecorder<? super T> recorder, T data) {
            this.recorder = recorder;
            this.data = data;
        }

        @Override
        public void close() {
            Tracer.fastCompleteSpan(recorder, data);
        }
    }
}
