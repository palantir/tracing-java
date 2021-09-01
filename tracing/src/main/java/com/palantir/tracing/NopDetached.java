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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Safe;
import com.palantir.tracing.api.SpanType;

/**
 * {@link Detached} implementation which represents state with no trace.
 * Operations which create child traces will each generate a unique traceId and sample independently.
 */
enum NopDetached implements Detached {
    INSTANCE;

    private static final CloseableSpan COMPLETE_SPAN = Tracer::fastCompleteSpan;

    @Override
    @MustBeClosed
    public <T> CloseableSpan childSpan(
            String operationName, TagTranslator<? super T> translator, T data, SpanType type) {
        return CloseableTracer.startSpan(operationName, translator, data, type)::close;
    }

    @Override
    @MustBeClosed
    public CloseableSpan childSpan(@Safe String operationName, SpanType type) {
        Tracer.fastStartSpan(operationName, type);
        return COMPLETE_SPAN;
    }

    @Override
    @MustBeClosed
    public CloseableSpan attach() {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public DetachedSpan childDetachedSpan(String operation, SpanType type) {
        return DetachedSpan.start(operation, type);
    }

    private enum NopCloseableSpan implements CloseableSpan {
        INSTANCE;

        @Override
        public void close() {
            // nop
        }
    }
}
