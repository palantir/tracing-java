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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(16)
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
public class TracingBenchmark {

    private static final Callable<Object> NOOP_CALLABLE = () -> null;

    private static final Callable<Object> undecidedCallable = Tracers.wrapWithNewTrace("test",
            Observability.UNDECIDED, NOOP_CALLABLE);

    private static final Callable<Object> sampledCallable = Tracers.wrapWithNewTrace("test",
            Observability.SAMPLE, NOOP_CALLABLE);

    private static final Callable<Object> unsampledCallable = Tracers.wrapWithNewTrace("test",
            Observability.DO_NOT_SAMPLE, NOOP_CALLABLE);

    @Setup
    public final void before() {
        Tracer.setSampler(new RandomSampler(0.1f));
    }

    @TearDown
    public final void after() {
        Tracer.setSampler(AlwaysSampler.INSTANCE);
    }

    @Benchmark
    public static Object raw() throws Exception {
        return NOOP_CALLABLE.call();
    }

    @Benchmark
    public static Object undecidedCallable() throws Exception {
        return undecidedCallable.call();
    }

    @Benchmark
    public static Object sampledCallable() throws Exception {
        return sampledCallable.call();
    }

    @Benchmark
    public static Object unsampledCallable() throws Exception {
        return unsampledCallable.call();
    }

    @Benchmark
    public static void unsampledTracerSpan(Blackhole blackhole) throws Exception {
        Tracer.initTrace(Observability.DO_NOT_SAMPLE, Tracers.randomId());
        for (int i = 0; i < 100; i++) {
            Callable<Object> callable = () -> {
                Tracer.startSpan("span");
                try {
                    return NOOP_CALLABLE.call();
                } finally {
                    Tracer.fastCompleteSpan();
                }
            };
            blackhole.consume(callable.call());
        }
    }

    @Benchmark
    public static void undecidedTracerSpan(Blackhole blackhole) throws Exception {
        Tracer.initTrace(Observability.UNDECIDED, Tracers.randomId());
        for (int i = 0; i < 100; i++) {
            Callable<Object> callable = () -> {
                Tracer.startSpan("span");
                try {
                    return NOOP_CALLABLE.call();
                } finally {
                    Tracer.fastCompleteSpan();
                }
            };
            blackhole.consume(callable.call());
        }
    }

    @Benchmark
    public static void sampledTracerSpan(Blackhole blackhole) throws Exception {
        Tracer.initTrace(Observability.SAMPLE, Tracers.randomId());
        for (int i = 0; i < 100; i++) {
            Callable<Object> callable = () -> {
                Tracer.startSpan("span");
                try {
                    return NOOP_CALLABLE.call();
                } finally {
                    Tracer.fastCompleteSpan();
                }
            };
            blackhole.consume(callable.call());
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TracingBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(1)
                .threads(16)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(3))
                .build();
        new Runner(opt).run();
    }
}
