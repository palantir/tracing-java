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
import com.palantir.tracing.logger.api.OpenTrace;
import com.palantir.tracing.logger.api.Trace;
import com.palantir.tracing.logger.api.TraceMetadata;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class EnabledTrace implements Trace, OpenTrace, TraceMetadata, SpanStackEntry {

    private static final int NOT_COMPLETE = 0;
    private static final int COMPLETE = 1;

    private static final AtomicIntegerFieldUpdater<EnabledTrace> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(EnabledTrace.class, "state");

    private final TraceState traceState;

    private volatile int state = NOT_COMPLETE;

    EnabledTrace(TraceState traceState) {
        this.traceState = traceState;
    }

    @Override
    public Optional<TraceMetadata> metadata() {
        return Optional.of(this);
    }

    @Override
    public String traceId() {
        return traceState.traceId();
    }

    @Override
    public TraceState traceState() {
        return traceState;
    }

    @Override
    public OpenTrace open() {
        if (state == COMPLETE) {
            throw new SafeIllegalStateException("Span is already complete");
        }

        Tracer.pushEntry(this);

        return this;
    }

    @Override
    public void close() {
        STATE_UPDATER.set(this, COMPLETE);
    }
}
