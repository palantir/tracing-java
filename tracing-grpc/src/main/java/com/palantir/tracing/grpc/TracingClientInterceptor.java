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

import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.Observability;
import com.palantir.tracing.TraceMetadata;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Optional;

/**
 * A {@link ClientInterceptor} which propagates Zipkin trace information through gRPC calls.
 */
public final class TracingClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new TracingClientCall<>(
                next.newCall(method, callOptions), method, Tracer.maybeGetTraceMetadata(), Tracer.isTraceObservable());
    }

    private static final class TracingClientCall<ReqT, RespT>
            extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final Optional<TraceMetadata> metadata;
        private final boolean isTraceObservable;

        TracingClientCall(
                ClientCall<ReqT, RespT> delegate,
                MethodDescriptor<ReqT, RespT> method,
                Optional<TraceMetadata> metadata,
                boolean isTraceObservable) {
            super(delegate);
            this.method = method;
            this.metadata = metadata;
            this.isTraceObservable = isTraceObservable;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            DetachedSpan span = detachedSpan();

            // the only way to get at the metadata of a detached span is to make an attached child :(
            try (CloseableSpan propagationSpan = span.childSpan("grpc: start", SpanType.CLIENT_OUTGOING)) {
                TraceMetadata propagationMetadata =
                        Tracer.maybeGetTraceMetadata().get();
                headers.put(GrpcTracing.TRACE_ID, propagationMetadata.getTraceId());
                headers.put(GrpcTracing.SPAN_ID, propagationMetadata.getSpanId());
                headers.put(GrpcTracing.IS_SAMPLED, Tracer.isTraceObservable() ? "1" : "0");
            }

            super.start(new TracingClientCallListener<>(responseListener, span), headers);
        }

        private DetachedSpan detachedSpan() {
            return DetachedSpan.start(
                    getObservability(),
                    metadata.isPresent() ? metadata.get().getTraceId() : Tracers.randomId(),
                    metadata.map(TraceMetadata::getSpanId),
                    method.getFullMethodName(),
                    SpanType.LOCAL);
        }

        private Observability getObservability() {
            if (!metadata.isPresent()) {
                return Observability.UNDECIDED;
            } else if (isTraceObservable) {
                return Observability.SAMPLE;
            } else {
                return Observability.DO_NOT_SAMPLE;
            }
        }
    }

    private static final class TracingClientCallListener<RespT>
            extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {
        private final DetachedSpan span;

        TracingClientCallListener(ClientCall.Listener<RespT> delegate, DetachedSpan span) {
            super(delegate);
            this.span = span;
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            span.complete();
            super.onClose(status, trailers);
        }
    }
}
