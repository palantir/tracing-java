/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

/**
 * Class representing the state which is created for each {@link Trace}. Contains the globally non-unique identifier of
 * a trace and a request identifier used to identify different requests sent from the same trace.
 */
final class TraceState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String traceId;

    @Nullable
    private final String requestId;

    @Nullable
    private final String forUserAgent;

    @Nullable
    private volatile TraceLocalMap traceLocals;

    private static final AtomicReferenceFieldUpdater<TraceState, TraceLocalMap> traceLocalsUpdater =
            AtomicReferenceFieldUpdater.newUpdater(TraceState.class, TraceLocalMap.class, "traceLocals");

    static TraceState of(String traceId, Optional<String> requestId, Optional<String> forUserAgent) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        checkNotNull(requestId, "requestId should be not-null");
        checkNotNull(forUserAgent, "forUserAgent should be not-null");
        return new TraceState(traceId, requestId.orElse(null), forUserAgent.orElse(null));
    }

    private TraceState(String traceId, @Nullable String requestId, @Nullable String forUserAgent) {
        this.traceId = traceId;
        this.requestId = requestId;
        this.forUserAgent = forUserAgent;
        this.traceLocals = null;
    }

    /**
     * The globally unique non-empty identifier for this call trace.
     * */
    String traceId() {
        return traceId;
    }

    /**
     * The request identifier of this trace.
     * <p>
     * The request identifier is an implementation detail of this tracing library. A new identifier is generated
     * each time a new trace is created with a SERVER_INCOMING root span. This is a convenience in order to
     * distinguish between requests with the same traceId.
     */
    @Nullable
    String requestId() {
        return requestId;
    }

    /**
     * The user agent propagated throughout the duration of this trace.
     */
    @Nullable
    String forUserAgent() {
        return forUserAgent;
    }

    public Map<TraceLocal<?>, Object> getOrCreateTraceLocals() {
        TraceLocalMap result = traceLocalsUpdater.get(this);

        if (result == null) {
            result = new TraceLocalMap();
            if (!traceLocalsUpdater.compareAndSet(this, null, result)) {
                return traceLocalsUpdater.get(this);
            }
        }

        return result;
    }

    @Nullable
    public Map<TraceLocal<?>, Object> getTraceLocals() {
        return traceLocals;
    }

    @Override
    public String toString() {
        return "TraceState{"
                + "traceId='" + traceId + "', "
                + "requestId='" + requestId + "', "
                + "forUserAgent='" + forUserAgent
                + "'}";
    }

    private class TraceLocalMap extends ConcurrentHashMap<TraceLocal<?>, Object> {}
}
