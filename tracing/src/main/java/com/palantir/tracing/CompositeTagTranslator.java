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

/**
 * Default implementation of {@link TagTranslator} used by {@link TagTranslator#andThen(TagTranslator)}.
 */
final class CompositeTagTranslator<S> implements TagTranslator<S> {

    private final TagTranslator<? super S> first;
    private final TagTranslator<? super S> second;

    CompositeTagTranslator(TagTranslator<? super S> first, TagTranslator<? super S> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public <T> void translate(TagAdapter<T> adapter, T target, S data) {
        first.translate(adapter, target, data);
        second.translate(adapter, target, data);
    }

    @Override
    public boolean isEmpty(S data) {
        return first.isEmpty(data) && second.isEmpty(data);
    }

    @Override
    public String toString() {
        return "CompositeTagTranslator{first=" + first + ", second=" + second + '}';
    }
}
