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

import java.util.Map;

enum SpanBuilderTagAdapter implements TagTranslator.TagAdapter<com.palantir.tracing.api.Span.Builder> {
    INSTANCE;

    @Override
    public void tag(com.palantir.tracing.api.Span.Builder target, String key, String value) {
        if (key != null && value != null) {
            target.putMetadata(key, value);
        }
    }

    @Override
    public void tag(com.palantir.tracing.api.Span.Builder target, Map<String, String> tags) {
        target.putAllMetadata(tags);
    }
}
