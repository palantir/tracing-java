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
import java.util.Optional;

final class DetachedImpl implements Detached {

    private final TraceState traceState;
    private final Optional<String> parentSpanId;

    DetachedImpl(TraceState traceState, Optional<String> parentSpanId) {
        this.traceState = traceState;
        this.parentSpanId = parentSpanId;
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
    public CloseableSpan attach() {
        return new DisabledSpan(traceState).attach();
    }
}
