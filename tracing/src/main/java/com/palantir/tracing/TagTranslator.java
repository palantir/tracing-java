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
 * Interface which allows tracing spans to be provided with tag metadata. This interface
 * is structured such that implementations can be created once and reused, given {@link S data}
 * from which all tag values are extracted. Unlike other approaches, this doesn't require per-span
 * lambdas and has minimal overhead in both the sampled and unsampled cases. This allows unsampled spans
 * to result in zero allocation.
 *
 * @param <S> Stateful object type, an instance of which is passed through method calls with the {@link TagTranslator}.
 */
public interface TagTranslator<S> {

    /**
     * Implementations may add tags based on {@code data}. Tags are applied using
     * {@link TagAdapter#tag(Object, String, String)} with the provided {@code target}.
     *
     * <pre>{@code
     * enum FirstElementTagTranslator implements TagTranslator<List<String>> {
     *     INSTANCE;
     *     <T> void translate(TagAdapter<T> adapter, T target, List<String> data) {
     *         adapter.tag(target, "firstElement", data.get(0));
     *     }
     * }
     * }</pre>
     */
    <T> void translate(TagAdapter<T> adapter, T target, S data);

    /**
     * May return {@code true} if the input data will not produce any tags. This allows
     * the tracing framework to avoid tracking additional data unnecessarily, but is not
     * required to implement. It is always safe to return {@code false} even if
     * {@link #translate(TagAdapter, Object, Object)} may not produce any tags.
     */
    default boolean isEmpty(S _data) {
        return false;
    }

    /**
     * Tag adapter object which insulates the implementation of the underlying data structure from callers.
     */
    interface TagAdapter<T> {
        void tag(T target, String key, String value);

        void tag(T target, Map<String, String> tags);
    }
}
