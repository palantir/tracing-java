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

import com.google.common.util.concurrent.Runnables;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
@SuppressWarnings({"checkstyle:hideutilityclassconstructor", "checkstyle:VisibilityModifier"})
public class TracingBenchmark {

    private static final Runnable nestedSpans = createNestedSpan(100);

    @SuppressWarnings("ImmutableEnumChecker")
    public enum BenchmarkObservability {
        SAMPLE(AlwaysSampler.INSTANCE),
        DO_NOT_SAMPLE(() -> false),
        UNDECIDED(new RandomSampler(0.01f));

        private final TraceSampler traceSampler;

        BenchmarkObservability(TraceSampler traceSampler) {
            this.traceSampler = traceSampler;
        }

        public TraceSampler getTraceSampler() {
            return traceSampler;
        }
    }

    @Param({"SAMPLE", "DO_NOT_SAMPLE", "UNDECIDED"})
    public BenchmarkObservability observability;

    @Setup
    public final void before(Blackhole blackhole) {
        Tracer.setSampler(observability.getTraceSampler());
        Tracer.subscribe("jmh", blackhole::consume);
        // clear any existing trace to make sure this sampler is used
        Tracer.getAndClearTrace();
    }

    @TearDown
    public final void after() {
        Tracer.unsubscribe("jmh");
    }

    @Benchmark
    public static void nestedSpans() {
        nestedSpans.run();
    }

    private static Runnable createNestedSpan(int depth) {
        if (depth <= 0) {
            return Runnables.doNothing();
        } else {
            return wrapWithSpan("benchmark-span-" + depth, createNestedSpan(depth - 1));
        }
    }

    private static Runnable wrapWithSpan(String operation, Runnable next) {
        return () -> {
            Tracer.fastStartSpan(operation);
            try {
                next.run();
            } finally {
                Tracer.fastCompleteSpan();
            }
        };
    }

    public static void main(String[] _args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TracingBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(1)
                .threads(4)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(3))
                .build();
        new Runner(opt).run();
    }
}
