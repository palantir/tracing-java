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

import org.junit.jupiter.api.Test;

public class BasicTracingDemos {

    @Test
    void name() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);

        assertThat(Tracer.getTraceId()).isEqualTo("00000000000000000000000000000000");

        try (CloseableTracer hello = CloseableTracer.startSpan("hello")) {
            System.out.println("World");
            System.out.println(Tracer.getTraceId());
            assertThat(Tracer.getTraceId()).doesNotContain("00000000000000000000000000000000");
            System.out.println(Tracer.maybeGetTraceMetadata());
            assertThat(Tracer.isTraceObservable()).isTrue();
        }

        assertThat(Tracer.getTraceId()).isEqualTo("00000000000000000000000000000000");
    }
}
