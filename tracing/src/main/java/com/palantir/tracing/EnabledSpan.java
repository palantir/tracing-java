/*
 * (c) Copyright 2025 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.OpenSpan;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class EnabledSpan implements InternalSpan, SpanMetadata, SpanStackEntry {

    private static final int NEW = 0;
    private static final int DETACHED = 1;
    private static final int ATTACHED = 2;
    private static final int COMPLETED = 3;

    private static final AtomicIntegerFieldUpdater<EnabledSpan> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EnabledSpan.class, "state");

    private final TraceState traceState;
    private final String spanId;
    private final Optional<String> parentSpanId;
    private final String operation;
    private final SpanType type;

    private final Map<String, String> tags = new ConcurrentHashMap<>();

    private volatile long startTimeMicroSeconds;
    private volatile long startClockNanoSeconds;

    private volatile int state = NEW;

    EnabledSpan(TraceState traceState, String spanId, Optional<String> parentSpanId, String operation, SpanType type) {
        this.traceState = traceState;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.operation = operation;
        this.type = type;
    }

    @Override
    public TraceState traceState() {
        return traceState;
    }

    @Override
    public Optional<SpanMetadata> metadata() {
        return Optional.of(this);
    }

    @Override
    public String spanId() {
        return spanId;
    }

    @Override
    public Optional<String> parentSpanId() {
        return parentSpanId;
    }

    @Override
    public String operation() {
        return operation;
    }

    SpanType type() {
        return type;
    }

    long startTimeMicroSeconds() {
        return startTimeMicroSeconds;
    }

    long startClockNanoSeconds() {
        return startClockNanoSeconds;
    }

    Map<String, String> tags() {
        return Collections.unmodifiableMap(tags);
    }

    @Override
    public void tag(String name, String value) {
        tags.put(name, value);
    }

    @Override
    public OpenSpan open() {
        if (!STATE_UPDATER.compareAndSet(this, NEW, ATTACHED)) {
            throw new SafeIllegalStateException("Span cannot be opened", SafeArg.of("state", state));
        }

        startTimeMicroSeconds = toEpochMicrosecond(Clock.systemUTC().instant());
        startClockNanoSeconds = System.nanoTime();

        return this;
    }

    @Override
    public void close() {
        detach();

        int previousState = STATE_UPDATER.getAndSet(this, COMPLETED)
        if (previousState == DETACHED || previousState == ATTACHED)) {
            Tracer.notifyObservers(this);
        }
    }

    // TODO(pkoenig): We probably need some sort of validation for these state transitions
    @Override
    public void start() {
        if (STATE_UPDATER.compareAndSet(this, NEW, DETACHED)) {
            throw new SafeIllegalStateException("Span cannot be started", SafeArg.of("state", state));
        }

        startTimeMicroSeconds = toEpochMicrosecond(Clock.systemUTC().instant());
        startClockNanoSeconds = System.nanoTime();
    }

    @Override
    public void attach() {
        if (STATE_UPDATER.compareAndSet(this, DETACHED, ATTACHED)) {
            Tracer.pushEntry(this);
        }
    }

    @Override
    public void detach() {
        if (STATE_UPDATER.compareAndSet(this, ATTACHED, DETACHED)) {
            Tracer.removeEntry(this);
        }
    }

    @Override
    public void complete() {
        STATE_UPDATER.getAndSet(this, COMPLETED);
    }

    private static long toEpochMicrosecond(Instant instant) {
        return (instant.getEpochSecond() * 1_000_000) + (instant.getNano() / 1_000);
    }
}
