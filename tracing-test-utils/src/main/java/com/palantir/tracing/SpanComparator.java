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

import com.palantir.tracing.api.Span;
import java.util.Comparator;

enum SpanComparator implements Comparator<Span> {
    INSTANCE;

    @Override
    public int compare(Span o1, Span o2) {
        int startTimeCompare = Long.compare(o1.getStartTimeMicroSeconds(), o2.getStartTimeMicroSeconds());
        if (startTimeCompare != 0) {
            return startTimeCompare;
        }
        int durationCompare = Long.compare(o1.getDurationNanoSeconds(), o2.getDurationNanoSeconds());
        if (durationCompare != 0) {
            return durationCompare;
        }
        throw new IllegalStateException(String.format("comparing duplicate spans %s %s", o1, o2));
    }
}
