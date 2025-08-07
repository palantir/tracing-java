/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import static com.palantir.logsafe.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Represents a trace as an ordered list of non-completed spans. Supports adding and removing of spans. This class is
 * not thread-safe and is intended to be used in a thread-local context.
 */
public final class Trace {

    private final TraceState traceState;
    private final List<EnabledSpan> stack = new ArrayList<>();
    private int disabledCount = 0;

    private Trace(TraceState traceState) {
        checkNotNull(traceState, "Trace state must not be null");
        this.traceState = traceState;
    }

    void pushDisabled() {
        disabledCount++;
    }

    void popDisabled() {
        if (disabledCount > 0) {
            disabledCount--;
        }
    }

    void push(EnabledSpan span) {
        stack.add(span);
    }

    @Nullable
    EnabledSpan pop() {
        if (stack.isEmpty()) {
            return null;
        }

        return stack.remove(stack.size() - 1);
    }

    @Nullable
    EnabledSpan remove(String spanId) {
        for (int i = stack.size() - 1; i >= 0; i--) {
            if (stack.get(i).spanId().endsWith(spanId)) {
                return stack.remove(i);
            }
        }
        return null;
    }

    @Nullable
    EnabledSpan current() {
        if (stack.isEmpty()) {
            return null;
        }

        return stack.get(stack.size() - 1);
    }

    boolean isEmpty() {
        return stack.isEmpty() && disabledCount == 0;
    }

    TraceState traceState() {
        return traceState;
    }

    static Trace of(TraceState traceState) {
        return new Trace(traceState);
    }
}
