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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class TagTranslatorTest {

    @Test
    public void testCombine() {
        AtomicInteger stringTranslatorCalled = new AtomicInteger();
        TagTranslator<String> translator1 = new TagTranslator<String>() {
            @Override
            public <T> void translate(TagAdapter<T> adapter, T target, String data) {
                stringTranslatorCalled.incrementAndGet();
            }

            @Override
            public boolean isEmpty(String data) {
                return data.isEmpty();
            }
        };
        TagTranslator<String> combined = translator1.andThen(NoTagTranslator.INSTANCE);
        assertThat(combined.isEmpty("")).isTrue();
        combined.translate(NopTagAdapter.INSTANCE, NopTagAdapter.INSTANCE, "value");
        assertThat(stringTranslatorCalled).hasValue(1);
    }

    private enum NopTagAdapter implements TagTranslator.TagAdapter<NopTagAdapter> {
        INSTANCE;

        @Override
        public void tag(NopTagAdapter _target, String _key, String _value) {}

        @Override
        public void tag(NopTagAdapter _target, Map<String, String> _tags) {}
    }
}
