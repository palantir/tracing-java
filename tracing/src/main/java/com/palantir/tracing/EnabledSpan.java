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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class EnabledSpan implements InternalSpan, InternalOpenSpan, SpanMetadata, SpanStackEntry {

    private static final int NOT_COMPLETE = 0;
    private static final int COMPLETE = 1;

    private static final AtomicIntegerFieldUpdater<EnabledSpan> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EnabledSpan.class, "state");

    private final TraceState traceState;
    private final String spanId;
    private final Optional<String> parentSpanId;
    private final String operation;
    private final SpanType type;
    private final long startTimeMicroSeconds;
    private final long startClockNanoSeconds;

    private final Map<String, String> tags = new ConcurrentHashMap<>();

    private volatile int state = NOT_COMPLETE;

    EnabledSpan(TraceState traceState, String spanId, Optional<String> parentSpanId, String operation, SpanType type) {
        this.traceState = traceState;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.operation = operation;
        this.type = type;
        startTimeMicroSeconds = toEpochMicrosecond(Clock.systemUTC().instant());
        startClockNanoSeconds = System.nanoTime();
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
    public InternalOpenSpan open() {
        if (state == COMPLETE) {
            throw new SafeIllegalStateException("Span is already complete");
        }

        Tracer.pushEntry(this);

        return this;
    }

    @Override
    public void close() {
        Tracer.removeEntry(this);
        complete();
    }

    @Override
    public InternalOpenSpan attach() {
        if (state == COMPLETE) {
            throw new SafeIllegalStateException("Span is already complete");
        }

        Tracer.pushEntry(this);

        return () -> Tracer.removeEntry(this);
    }

    @Override
    public void complete() {
        if (STATE_UPDATER.compareAndSet(this, NOT_COMPLETE, COMPLETE)) {
            Tracer.notifyObservers(this);
        }
    }

    @Override
    public <T> Optional<Span> complete(TagTranslator<? super T> tagTranslator, T data) {
        if (STATE_UPDATER.compareAndSet(this, NOT_COMPLETE, COMPLETE)) {
            Span completedSpan = Tracer.notifyObservers(this, tagTranslator, data);
            return Optional.of(completedSpan);
        }

        return Optional.empty();
    }

    private static long toEpochMicrosecond(Instant instant) {
        return (instant.getEpochSecond() * 1_000_000) + (instant.getNano() / 1_000);
    }
}
