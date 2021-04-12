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

public interface TagProducer<S> {

    <T> void tag(TagSink<T> sink, TagAdapter<T> tagAdapter, T target, S state);

    interface TagAdapter<T> {
        void addTag(T object, String key, String value);

        void addTags(T object, Map<String, String> tags);
    }

    interface TagSink<T> {
        void apply(String key, String value, TagAdapter<T> tagger, T target);

        void apply(Map<String, String> tags, TagAdapter<T> tagger, T target);
    }
}
