/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableSet;
import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SpanAnalyzerTest {
    @Test
    void cjr_test_1() throws IOException {
        String testPrefix = "src/test/resources/cjr-test-1/";
        List<Span> spans1 = Serialization.deserialize(Paths.get(testPrefix + "spans-1.log"));
        List<Span> spans2 = Serialization.deserialize(Paths.get(testPrefix + "spans-2.log"));
        SpanAnalyzer.Result result1 = SpanAnalyzer.analyze(spans1);
        SpanAnalyzer.Result result2 = SpanAnalyzer.analyze(spans2);

        Set<ComparisonFailure> failures = SpanAnalyzer.compareSpansRecursively(
                        result1, result2, result1.root(), result2.root())
                .collect(ImmutableSet.toImmutableSet());
        assertThat(failures).isEmpty();
    }
}
