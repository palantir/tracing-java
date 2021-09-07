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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Safe;
import com.palantir.tracing.api.SpanType;
import java.util.Map;
import javax.annotation.CheckReturnValue;

/**
 * Detached tracing component which is not bound to thread state, and can be used on any thread.
 *
 * This class must not be implemented externally.
 */
public interface Detached {

    /**
     * Equivalent to {@link Tracer#startSpan(String, SpanType)}, but using this {@link DetachedSpan} as the parent
     * instead of thread state.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, SpanType type) {
        return childSpan(operationName, NoTagTranslator.INSTANCE, NoTagTranslator.INSTANCE, type);
    }

    /**
     * Equivalent to {@link #childSpan(String, Map, SpanType)} using a {@link SpanType#LOCAL span}.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, @Safe Map<String, String> metadata) {
        return childSpan(operationName, metadata, SpanType.LOCAL);
    }

    @MustBeClosed
    default <T> CloseableSpan childSpan(@Safe String operationName, TagTranslator<? super T> translator, T data) {
        return childSpan(operationName, translator, data, SpanType.LOCAL);
    }

    /**
     * Equivalent to {@link #childSpan(String, SpanType)}, but using {@link Map metadata} tags.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName, @Safe Map<String, String> metadata, SpanType type) {
        return childSpan(operationName, MapTagTranslator.INSTANCE, metadata, type);
    }

    @MustBeClosed
    <T> CloseableSpan childSpan(@Safe String operationName, TagTranslator<? super T> translator, T data, SpanType type);

    /**
     * Equivalent to {@link Tracer#startSpan(String)}, but using this {@link DetachedSpan} as the parent instead of
     * thread state.
     */
    @MustBeClosed
    default CloseableSpan childSpan(@Safe String operationName) {
        return childSpan(operationName, SpanType.LOCAL);
    }

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent.
     */
    @CheckReturnValue
    DetachedSpan childDetachedSpan(String operation, SpanType type);

    /**
     * Starts a child {@link DetachedSpan} using this instance as the parent. Equivalent to
     * {@link #childDetachedSpan(String, SpanType)} using {@link SpanType#LOCAL}.
     */
    @CheckReturnValue
    default DetachedSpan childDetachedSpan(@Safe String operation) {
        return childDetachedSpan(operation, SpanType.LOCAL);
    }

    /**
     * Attaches the current {@link DetachedSpan} state to the current thread without creating additional spans.
     * This is useful when a long-lived {@link DetachedSpan} measures many smaller operations (like async-io)
     * in which we don't want to produce spans for each task, but do need tracing state associated for logging
     * and potential child traces.
     * @apiNote This must be executed within a try-with-resources block, and the parent detached span must still be
     * completed separately.
     */
    @MustBeClosed
    CloseableSpan attach();
}
