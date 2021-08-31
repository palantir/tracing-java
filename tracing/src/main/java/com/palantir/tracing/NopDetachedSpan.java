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

import com.palantir.tracing.api.SpanType;
import java.util.Map;

enum NopDetachedSpan implements DetachedSpan {
    INSTANCE;

    @Override
    public CloseableSpan childSpan(String _operationName, SpanType _type) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public CloseableSpan childSpan(String _operationName, Map<String, String> _metadata) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public <T> CloseableSpan childSpan(String _operationName, TagTranslator<? super T> _translator, T _data) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public CloseableSpan childSpan(String _operationName, Map<String, String> _metadata, SpanType _type) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public <T> CloseableSpan childSpan(
            String _operationName, TagTranslator<? super T> _translator, T _data, SpanType _type) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public CloseableSpan childSpan(String _operationName) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public CloseableSpan completeAndStartChild(String _operationName, SpanType _type) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public CloseableSpan completeAndStartChild(String _operationName) {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public DetachedSpan childDetachedSpan(String _operation, SpanType _type) {
        return NopDetachedSpan.INSTANCE;
    }

    @Override
    public DetachedSpan childDetachedSpan(String _operation) {
        return NopDetachedSpan.INSTANCE;
    }

    @Override
    public CloseableSpan attach() {
        return NopCloseableSpan.INSTANCE;
    }

    @Override
    public void complete() {
        // nop
    }

    @Override
    public void complete(Map<String, String> _metadata) {
        // nop
    }

    @Override
    public <T> void complete(TagTranslator<? super T> _tagTranslator, T _data) {
        // nop
    }

    private enum NopCloseableSpan implements CloseableSpan {
        INSTANCE;

        @Override
        public void close() {
            // nop
        }
    }
}
