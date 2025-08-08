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

import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.OpenSpan;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.util.Optional;

final class DetachedImpl implements Detached {

    private final TraceState traceState;
    private final Optional<String> parentSpanId;
    private final InternalSpan span;

    DetachedImpl(TraceState traceState, Optional<String> parentSpanId, String operation, SpanType type) {
        this.traceState = traceState;
        this.span = new EnabledSpan(traceState, Tracers.randomId(), parentSpanId, operation, type);
        this.parentSpanId = span.metadata().map(SpanMetadata::spanId).or(() -> parentSpanId);
        span.start();
    }

    TraceState traceState() {
        return traceState;
    }

    @Override
    public <T> CloseableSpan childSpan(String operation, TagTranslator<? super T> translator, T data, SpanType type) {
        OpenSpan openSpan = Tracer.newSpan(operation, parentSpanId, type).open();

        translator.translate(OpenSpanTagAdapter.INSTANCE, openSpan, data);

        return openSpan::close;
    }

    @Override
    public DetachedSpan childDetachedSpan(String operation, SpanType type) {
        return new DetachedImpl(traceState, parentSpanId, operation, type);
    }

    @Override
    public CloseableSpan attach() {
        span.attach();
        return span::detach;
    }
}
