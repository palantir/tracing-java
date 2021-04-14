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
 * @param <S> Stateful object type, an instance of which is passed through method calls with the {@link TagRecorder}.
 */
public interface TagRecorder<S> {

    /** Implementations add tags based on {@code state}. */
    <T> void record(TagAdapter<T> sink, T target, S state);

    default boolean isEmpty(S state) {
        return false;
    }

    /** Tag adapter object which insulates the implementation of the underlying data structure from callers. */
    interface TagAdapter<T> {
        void tag(T target, String key, String value);

        void tag(T target, Map<String, String> tags);
    }
}
