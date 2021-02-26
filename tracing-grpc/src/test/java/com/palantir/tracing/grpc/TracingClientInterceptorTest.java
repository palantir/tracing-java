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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.tracing.Observability;
import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.SpanType;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TracingClientInterceptorTest {
    private static final SimpleServiceGrpc.SimpleServiceImplBase SERVICE =
            new SimpleServiceGrpc.SimpleServiceImplBase() {
                @Override
                public void unaryRpc(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
                    responseObserver.onNext(SimpleResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Captor
    private ArgumentCaptor<Metadata> metadataCaptor;

    @Mock
    private TraceSampler traceSampler;

    private final ServerInterceptor mockServerInterceptor =
            mock(ServerInterceptor.class, delegatesTo(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                    return next.startCall(call, headers);
                }
            }));

    private SimpleServiceGrpc.SimpleServiceBlockingStub blockingStub;

    @Before
    public void before() throws Exception {
        Tracer.setSampler(traceSampler);

        when(traceSampler.sample()).thenReturn(true);

        String serverName = InProcessServerBuilder.generateName();
        grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(SERVICE, mockServerInterceptor))
                .build()
                .start());
        ManagedChannel channel =
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).build());
        blockingStub = SimpleServiceGrpc.newBlockingStub(
                ClientInterceptors.intercept(channel, new TracingClientInterceptor()));
    }

    @Test
    public void traceIsPropagated_whenNoTraceIsPresent() {
        Tracer.getAndClearTrace();
        blockingStub.unaryRpc(SimpleRequest.newBuilder().build());

        verify(mockServerInterceptor).interceptCall(any(), metadataCaptor.capture(), any());
        verifyNoMoreInteractions(mockServerInterceptor);

        Metadata intercepted = metadataCaptor.getValue();
        assertThat(intercepted.getAll(GrpcTracing.TRACE_ID)).hasSize(1);
        assertThat(intercepted.getAll(GrpcTracing.SPAN_ID)).hasSize(1);
        assertThat(intercepted.getAll(GrpcTracing.IS_SAMPLED)).hasSize(1);
    }

    @Test
    public void traceIsPropagated_whenTraceIsPresent() {
        Tracer.initTraceWithSpan(Observability.SAMPLE, "id", "operation", SpanType.LOCAL);
        try {
            blockingStub.unaryRpc(SimpleRequest.newBuilder().build());
        } finally {
            Tracer.fastCompleteSpan();
        }

        verify(mockServerInterceptor).interceptCall(any(), metadataCaptor.capture(), any());
        verifyNoMoreInteractions(mockServerInterceptor);

        Metadata intercepted = metadataCaptor.getValue();
        assertThat(intercepted.getAll(GrpcTracing.TRACE_ID)).containsExactly("id");
        assertThat(intercepted.getAll(GrpcTracing.SPAN_ID)).hasSize(1);
        assertThat(intercepted.getAll(GrpcTracing.IS_SAMPLED)).containsExactly("1");
    }
}
