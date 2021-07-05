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

package com.palantir.tracing2;

import java.util.Optional;
import java.util.function.BiFunction;

final class DefaultTrace implements Trace {
    private final BiFunction<Optional<Span>, String, Span> spanFactory;
    private final String traceId;

    DefaultTrace(String traceId, boolean isObservable) {
        spanFactory = isObservable
                ? (parent, opName) -> DefaultSpan.create(this, parent, opName)
                : (parent, opName) -> EmptySpan.INSTANCE;
        this.traceId = traceId;
    }

    @Override
    public Span rootSpan(String opName) {
        return spanFactory.apply(Optional.empty(), opName);
    }

    @Override
    public String traceId() {
        return traceId;
    }
}
