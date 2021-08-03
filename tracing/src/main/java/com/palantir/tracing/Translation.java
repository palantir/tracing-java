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

import com.palantir.tracing.api.SpanType;
import io.opentelemetry.api.trace.SpanKind;

final class Translation {

    private Translation() {}

    static SpanKind toOpenTelemetry(SpanType palantirType) {
        switch (palantirType) {
            case SERVER_INCOMING:
                return SpanKind.SERVER;
            case CLIENT_OUTGOING:
                return SpanKind.CLIENT;
            case LOCAL:
                return SpanKind.INTERNAL;
        }
        throw new UnsupportedOperationException();
    }

    static SpanType fromOpenTelemetryLossy(SpanKind openTelemetryKind) {
        switch (openTelemetryKind) {
            case INTERNAL:
                return SpanType.LOCAL;
            case SERVER:
                return SpanType.SERVER_INCOMING;
            case CLIENT:
                return SpanType.CLIENT_OUTGOING;
            case PRODUCER:
                return SpanType.LOCAL;
            case CONSUMER:
                return SpanType.LOCAL;
        }
        throw new UnsupportedOperationException();
    }
}
