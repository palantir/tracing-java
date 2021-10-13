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
import java.util.Optional;

public final class CommonTraceState {
    private final String traceId;
    private final Optional<String> requestId;

    public static CommonTraceState create(String traceId, Optional<String> requestId) {
        checkArgument(!Strings.isNullOrEmpty(traceId), "traceId must be non-empty");
        checkNotNull(requestId, "requestId should be not-null");
        return new CommonTraceState(traceId, requestId);
    }

    private CommonTraceState(String traceId, Optional<String> requestId) {
        this.traceId = traceId;
        this.requestId = requestId;
    }

    public String getTraceId() {
        return traceId;
    }

    public Optional<String> getRequestId() {
        return requestId;
    }
}
