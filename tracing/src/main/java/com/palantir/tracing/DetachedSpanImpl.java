/*
 * (c) Copyright 2025 Palantir Technologies Inc. All rights reserved.
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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.util.Optional;

final class DetachedSpanImpl implements DetachedSpan {

    private final TraceState traceState;
    private final Optional<String> parentSpanId;
    private final InternalSpan span;

    DetachedSpanImpl(TraceState traceState, Optional<String> parentSpanId, String operation, SpanType type) {
        this.traceState = traceState;
        this.span = Tracer.newSpan(traceState, operation, parentSpanId, type);
        this.parentSpanId = span.metadata().map(SpanMetadata::spanId).or(() -> parentSpanId);
    }

    TraceState traceState() {
        return traceState;
    }

    @Override
    @MustBeClosed
    public <T> CloseableSpan childSpan(String operation, TagTranslator<? super T> translator, T data, SpanType type) {
        InternalSpan childSpan = Tracer.newSpan(traceState, operation, parentSpanId, type);

        if (!translator.isEmpty(data)) {
            translator.translate(SpanTagAdapter.INSTANCE, childSpan, data);
        }

        return childSpan.open();
    }

    @Override
    public DetachedSpan childDetachedSpan(String operation, SpanType type) {
        return new DetachedSpanImpl(traceState, parentSpanId, operation, type);
    }

    @Override
    @MustBeClosed
    public CloseableSpan attach() {
        return span.attach();
    }

    @Override
    public void complete() {
        complete(NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE);
    }

    @Override
    public <T> void complete(TagTranslator<? super T> tagTranslator, T data) {
        span.complete(tagTranslator, data);
    }
}
