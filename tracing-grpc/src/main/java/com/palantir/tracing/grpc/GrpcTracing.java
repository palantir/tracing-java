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

package com.palantir.tracing.grpc;

import com.palantir.tracing.api.TraceHttpHeaders;
import io.grpc.Metadata;

/**
 * Internal utility class used to deduplicate logic between the client and server interceptors.
 *
 * Intentionally package-private.
 */
final class GrpcTracing {
    static final Metadata.Key<String> TRACE_ID =
            Metadata.Key.of(TraceHttpHeaders.TRACE_ID, Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> SPAN_ID =
            Metadata.Key.of(TraceHttpHeaders.SPAN_ID, Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> IS_SAMPLED =
            Metadata.Key.of(TraceHttpHeaders.IS_SAMPLED, Metadata.ASCII_STRING_MARSHALLER);

    private GrpcTracing() {}
}
