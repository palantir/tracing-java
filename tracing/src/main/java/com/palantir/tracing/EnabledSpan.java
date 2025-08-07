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
import com.palantir.tracing.logger.api.Span;
import com.palantir.tracing.logger.api.SpanMetadata;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class EnabledSpan implements Span, OpenSpan, SpanMetadata {

    private static final int NEW = 0;
    private static final int OPEN = 1;
    private static final int CLOSED = 2;

    private static final AtomicIntegerFieldUpdater<EnabledSpan> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EnabledSpan.class, "state");

    private final String spanId;
    private final Optional<String> parentSpanId;
    private final String operation;
    private final SpanType type;

    private final Map<String, String> tags = new ConcurrentHashMap<>();

    private volatile long startEpochMicrosecond;
    private volatile long startNanoTime;

    private volatile int state = NEW;

    EnabledSpan(String spanId, Optional<String> parentSpanId, String operation, SpanType type) {
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.operation = operation;
        this.type = type;
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

    long startEpochMicrosecond() {
        return startEpochMicrosecond;
    }

    long startNanoTime() {
        return startNanoTime;
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
        start();

        Tracer.pushSpan(this);

        return this;
    }

    public void start() {
        if (!STATE_UPDATER.compareAndSet(this, NEW, OPEN)) {
            throw new SafeIllegalStateException("Span cannot be opened", SafeArg.of("state", state));
        }

        startEpochMicrosecond = toEpochMicrosecond(Clock.systemUTC().instant());
        startNanoTime = System.nanoTime();
    }

    @Override
    public void close() {
        if (complete()) {
            Tracer.completeSpan(spanId);
        }
    }

    public boolean complete() {
        return STATE_UPDATER.getAndSet(this, CLOSED) == OPEN;
    }

    private static long toEpochMicrosecond(Instant instant) {
        return (instant.getEpochSecond() * 1_000_000) + (instant.getNano() / 1_000);
    }
}
