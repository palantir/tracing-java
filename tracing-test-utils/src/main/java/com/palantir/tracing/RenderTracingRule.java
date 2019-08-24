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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class RenderTracingRule implements TestRule {
    private final SpanRenderer renderer = new SpanRenderer();

    @Override
    public Statement apply(
            Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    Tracer.setSampler(AlwaysSampler.INSTANCE);
                    Tracer.subscribe("RenderTracingRule", renderer);

                    base.evaluate();
                } finally {
                    Tracer.unsubscribe("RenderTracingRule");
                    renderer.output(description.getClassName() + "#" + description.getMethodName());
                }
            }
        };
    }
}
