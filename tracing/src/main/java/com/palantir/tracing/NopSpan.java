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

import com.palantir.tracing.api.Span;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.util.Optional;

enum NopSpan implements InternalSpan {
    INSTANCE;

    @Override
    public Optional<SpanMetadata> metadata() {
        return Optional.empty();
    }

    @Override
    public void tag(String _name, String _value) {}

    @Override
    public InternalOpenSpan open() {
        return NopOpenSpan.INSTANCE;
    }

    @Override
    public InternalOpenSpan attach() {
        return NopOpenSpan.INSTANCE;
    }

    @Override
    public void complete() {}

    @Override
    public <T> Optional<Span> complete(TagTranslator<? super T> _tagTranslator, T _data) {
        return Optional.empty();
    }
}
