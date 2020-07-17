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

import static com.palantir.logsafe.Preconditions.checkArgument;

import com.palantir.logsafe.SafeArg;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A sampler that returns true at a rate approximately equivalent to the one specified. The specified rate should be
 * between 0 and 1 (inclusive).
 */
public final class RandomSampler implements TraceSampler {

    private final float rate;

    /**
     * Prefer the factory method {@link RandomSampler#create(float)}.
     *
     * @deprecated prefer the factory method
     */
    @Deprecated
    public RandomSampler(float rate) {
        checkArgument(rate >= 0 && rate <= 1, "Rate should be between 0 and 1", SafeArg.of("rate", rate));
        this.rate = rate;
    }

    public static TraceSampler create(float rate) {
        checkArgument(rate >= 0 && rate <= 1, "Rate should be between 0 and 1", SafeArg.of("rate", rate));
        if (rate == 0) {
            return NeverSampler.INSTANCE;
        }

        if (rate == 1) {
            return AlwaysSampler.INSTANCE;
        }
        return new RandomSampler(rate);
    }

    @Override
    public boolean sample() {
        return ThreadLocalRandom.current().nextFloat() < rate;
    }
}
