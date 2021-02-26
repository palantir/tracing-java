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

import com.google.common.base.Strings;
import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracers;
import com.palantir.tracing.api.SpanType;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ServerInterceptor} which extracts Zipkin trace data propagated from the gRPC client and wraps the
 * execution of the request in spans.
 *
 * <p>Depending on the style of gRPC request, the actual server handler code will run in the
 * {@link #interceptCall(ServerCall, Metadata, ServerCallHandler)} method of the {@link TracingServerInterceptor}, or
 * in the {@link TracingServerCallListener#onHalfClose()} method of the {@link TracingServerCallListener}. Certain
 * user callbacks can also be invoked in other {@link TracingServerCallListener} methods, so they are all spanned.
 *
 * <p>The request is considered completed when the {@link ServerCall#close(Status, Metadata)} method is invoked.
 * Since the ordering of a close call and certain terminal {@link ServerCall.Listener} callbacks are not specified,
 * there's some extra logic to try to avoid creating child spans after the root span has already been completed.
 */
public final class TracingServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String maybeTraceId = headers.get(GrpcTracing.TRACE_ID);
        boolean newTraceId = maybeTraceId == null;
        String traceId = newTraceId ? Tracers.randomId() : maybeTraceId;

        DetachedSpan span = detachedSpan(newTraceId, traceId, call, headers);
        AtomicReference<DetachedSpan> spanRef = new AtomicReference<>(span);

        try (CloseableSpan guard = span.childSpan("grpc: interceptCall")) {
            return new TracingServerCallListener<>(
                    next.startCall(new TracingServerCall<>(call, spanRef), headers), spanRef);
        }
    }

    private DetachedSpan detachedSpan(boolean newTrace, String traceId, ServerCall<?, ?> call, Metadata headers) {
        return DetachedSpan.start(
                getObservabilityFromHeader(headers),
                traceId,
                newTrace ? Optional.empty() : Optional.ofNullable(headers.get(GrpcTracing.SPAN_ID)),
                call.getMethodDescriptor().getFullMethodName(),
                SpanType.SERVER_INCOMING);
    }

    private Observability getObservabilityFromHeader(Metadata headers) {
        String header = headers.get(GrpcTracing.IS_SAMPLED);
        if (Strings.isNullOrEmpty(header)) {
            return Observability.UNDECIDED;
        } else {
            return "1".equals(header) ? Observability.SAMPLE : Observability.DO_NOT_SAMPLE;
        }
    }

    private static final class TracingServerCall<ReqT, RespT>
            extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {
        private final AtomicReference<DetachedSpan> span;

        TracingServerCall(ServerCall<ReqT, RespT> delegate, AtomicReference<DetachedSpan> span) {
            super(delegate);
            this.span = span;
        }

        @Override
        public void close(Status status, Metadata trailers) {
            DetachedSpan maybeSpan = span.getAndSet(null);
            if (maybeSpan != null) {
                maybeSpan.complete();
            }
            super.close(status, trailers);
        }
    }

    private static final class TracingServerCallListener<ReqT>
            extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {
        private final AtomicReference<DetachedSpan> span;

        TracingServerCallListener(Listener<ReqT> delegate, AtomicReference<DetachedSpan> span) {
            super(delegate);
            this.span = span;
        }

        @Override
        public void onMessage(ReqT message) {
            maybeSpanned("grpc: onMessage", () -> super.onMessage(message));
        }

        @Override
        public void onHalfClose() {
            maybeSpanned("grpc: onHalfClose", super::onHalfClose);
        }

        @Override
        public void onCancel() {
            maybeSpanned("grpc: onCancel", super::onCancel);
        }

        @Override
        public void onComplete() {
            maybeSpanned("grpc: onComplete", super::onComplete);
        }

        @Override
        public void onReady() {
            maybeSpanned("grpc: onReady", super::onReady);
        }

        /**
         * Wraps a callback in a span if the root span has not already been closed. The gRPC glue can call listener
         * methods after the ServerCall has already been closed in some cases, and we want to avoid log spam from the
         * tracing internals warning about making a child off a completed span.
         */
        private void maybeSpanned(String spanName, Runnable runnable) {
            DetachedSpan maybeSpan = span.get();
            if (maybeSpan == null) {
                runnable.run();
            } else {
                try (CloseableSpan guard = maybeSpan.childSpan(spanName)) {
                    runnable.run();
                }
            }
        }
    }
}
