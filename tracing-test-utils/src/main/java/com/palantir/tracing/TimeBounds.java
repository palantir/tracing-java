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
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

interface TimeBounds extends Comparable<TimeBounds> {

    Comparator<TimeBounds> COMPARATOR =
            Comparator.comparingLong(TimeBounds::startMicros).thenComparing(TimeBounds::endNanos);

    long startMicros();

    long endNanos();

    default long startNanos() {
        return TimeUnit.NANOSECONDS.convert(startMicros(), TimeUnit.MICROSECONDS);
    }

    default long durationNanos() {
        return endNanos() - startNanos();
    }

    default long durationMicros() {
        return TimeUnit.MICROSECONDS.convert(durationNanos(), TimeUnit.NANOSECONDS);
    }

    static TimeBounds fromSpans(Collection<Span> spans) {
        long earliestStartMicros = spans.stream()
                .mapToLong(Span::getStartTimeMicroSeconds)
                .min()
                .getAsLong();
        long latestEndNanos = spans.stream()
                .mapToLong(span -> {
                    long startTimeNanos =
                            TimeUnit.NANOSECONDS.convert(span.getStartTimeMicroSeconds(), TimeUnit.MICROSECONDS);
                    return startTimeNanos + span.getDurationNanoSeconds();
                })
                .max()
                .getAsLong();
        return new TimeBounds() {
            @Override
            public int compareTo(TimeBounds other) {
                return COMPARATOR.compare(this, other);
            }

            @Override
            public long startMicros() {
                return earliestStartMicros;
            }

            @Override
            public long endNanos() {
                return latestEndNanos;
            }
        };
    }
}
