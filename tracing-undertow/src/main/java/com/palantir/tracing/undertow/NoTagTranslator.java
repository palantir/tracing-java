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

package com.palantir.tracing.undertow;

import com.palantir.tracing.TagTranslator;

/** Internal {@link TagTranslator} which adds no tags. */
enum NoTagTranslator implements TagTranslator<Object> {
    INSTANCE;

    @Override
    public <T> void translate(TagAdapter<T> _adapter, T _target, Object _data) {}

    @Override
    public boolean isEmpty(Object _data) {
        return true;
    }
}
