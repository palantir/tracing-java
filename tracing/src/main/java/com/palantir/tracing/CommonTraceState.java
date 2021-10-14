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
import java.util.Optional;

/**
 * Class representing the state which is created for each {@link Trace}. Contains the globally non-unique identifier of
 * a trace and a request identifier used to identify different requests sent from the same trace.
 */
final class CommonTraceState implements Serializable {
    private final String traceId;
    private final Optional<String> requestId;

    static CommonTraceState create(String traceId, Optional<String> requestId) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        checkNotNull(requestId, "requestId should be not-null");
        return new CommonTraceState(traceId, requestId);
    }

    private CommonTraceState(String traceId, Optional<String> requestId) {
        this.traceId = traceId;
        this.requestId = requestId;
    }

    /**
     * The globally unique non-empty identifier for this call trace.
     * */
    String getTraceId() {
        return traceId;
    }

    /**
     * The request identifier of this trace.
     *
     * The request identifier is an implementation detail of this tracing library. A new identifier is generated
     * each time a new trace is created with a SERVER_INCOMING root span. This is a convenience in order to
     * distinguish between requests with the same traceId.
     */
    Optional<String> getRequestId() {
        return requestId;
    }

    CommonTraceState deepCopy() {
        return new CommonTraceState(traceId, requestId);
    }

    @Override
    public String toString() {
        return "CommonTraceState{" + "traceId='" + traceId + '\'' + ", requestId=" + requestId.orElse(null) + '}';
    }
}
