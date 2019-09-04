/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

        System.out.println(result1.root());
        System.out.println(result2.root());

        Set<ComparisonFailure> failures = SpanAnalyzer.compareSpansRecursively(
                result1, result2, result1.root(), result2.root()).collect(ImmutableSet.toImmutableSet());
        assertThat(failures).isEmpty();
    }
}
