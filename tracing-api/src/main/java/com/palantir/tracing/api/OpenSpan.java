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

package com.palantir.tracing.api;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A value object represented an open (i.e., non-completed) span. Once completed, the span is represented by a {@link
 * Span} object.
 */
@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public abstract class OpenSpan {
    private static final Clock CLOCK = Clock.systemUTC();

    /** Returns a description of the operation for this event. */
    @Value.Parameter
    public abstract String getOperation();

    /**
     * Returns the start time in microseconds since epoch start of the span represented by this state.
     *
     * <p>Users of this class should not set this value manually in the builder, it is configured automatically when
     * using the {@link #builder()} static.
     */
    @Value.Parameter
    public abstract long getStartTimeMicroSeconds();

    /**
     * Returns the starting clock position in nanoseconds for use in computing span duration.
     *
     * <p>Users of this class should not set this value manually in the builder, it is configured automatically when
     * using the {@link #builder()} static.
     */
    @Value.Parameter
    public abstract long getStartClockNanoSeconds();

    /** Returns the identifier of the parent span for the current span, if one exists. */
    @Value.Parameter
    public abstract Optional<String> getParentSpanId();

    /**
     * Returns the identifier of the 'originating' span if one exists.
     *
     * @see TraceHttpHeaders
     */
    @Value.Parameter
    public abstract Optional<String> getOriginatingSpanId();

    /** Returns a globally unique identifier representing a single span within the call trace. */
    @Value.Parameter
    public abstract String getSpanId();

    /** Indicates the {@link SpanType} of this span, e.g., a server-side vs. client-side vs local span. */
    @Value.Parameter
    public abstract SpanType type();

    /**
     * Indicates if this trace state was sampled public abstract boolean isSampled();
     *
     * <p>/** Returns a builder for {@link OpenSpan} pre-initialized to use the current time.
     *
     * <p>Users should not set the {@code startTimeMs} value manually.
     */
    public static Builder builder() {
        return new Builder().startTimeMicroSeconds(getNowInMicroSeconds()).startClockNanoSeconds(System.nanoTime());
    }

    /** Use this factory method to avoid allocate {@link Builder} in hot path. */
    public static OpenSpan of(
            String operation,
            String spanId,
            SpanType type,
            Optional<String> parentSpanId,
            Optional<String> originatingSpanId) {
        return ImmutableOpenSpan.of(
                operation, getNowInMicroSeconds(), System.nanoTime(), parentSpanId, originatingSpanId, spanId, type);
    }

    /**
     * Deprecated.
     *
     * @deprecated Use the variant that accepts an originating span id
     */
    @Deprecated
    public static OpenSpan of(String operation, String spanId, SpanType type, Optional<String> parentSpanId) {
        return of(operation, spanId, type, parentSpanId, Optional.empty());
    }

    private static long getNowInMicroSeconds() {
        Instant now = CLOCK.instant();
        return (1000000 * now.getEpochSecond()) + (now.getNano() / 1000);
    }

    public static class Builder extends ImmutableOpenSpan.Builder {}
}
