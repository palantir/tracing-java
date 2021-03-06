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

import java.util.Map;

/**
 * Internal tag translator for {@link Map maps}.
 */
enum MapTagTranslator implements TagTranslator<Map<String, String>> {
    INSTANCE;

    @Override
    public <T> void translate(TagAdapter<T> adapter, T target, Map<String, String> data) {
        adapter.tag(target, data);
    }

    @Override
    public boolean isEmpty(Map<String, String> data) {
        return data == null || data.isEmpty();
    }
}
