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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.tracing.TraceSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.testing.StreamRecorder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TracingServerInterceptorTest {
    private static final SimpleServiceGrpc.SimpleServiceImplBase SERVICE =
            new SimpleServiceGrpc.SimpleServiceImplBase() {
                @Override
                public void unaryRpc(SimpleRequest request, StreamObserver<SimpleResponse> responseObserver) {
                    Tracer.fastStartSpan("handler");
                    Tracer.fastCompleteSpan();
                    responseObserver.onNext(SimpleResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }

                @Override
                public StreamObserver<SimpleRequest> clientStreamingRpc(
                        StreamObserver<SimpleResponse> responseObserver) {
                    Tracer.fastStartSpan("handler");
                    Tracer.fastCompleteSpan();

                    return new StreamObserver<SimpleRequest>() {
                        @Override
                        public void onNext(SimpleRequest value) {
                            Tracer.fastStartSpan("observer");
                            Tracer.fastCompleteSpan();
                        }

                        @Override
                        public void onError(Throwable _throwable) {}

                        @Override
                        public void onCompleted() {
                            responseObserver.onNext(SimpleResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                    };
                }
            };

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Mock
    private TraceSampler traceSampler;

    @Mock
    private SpanObserver spanObserver;

    @Captor
    private ArgumentCaptor<Span> spanCaptor;

    private ManagedChannel channel;

    @Before
    public void before() throws Exception {
        Tracer.subscribe("", spanObserver);
        Tracer.setSampler(traceSampler);

        String serverName = InProcessServerBuilder.generateName();
        grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(SERVICE, new TracingServerInterceptor()))
                .build()
                .start());
        channel =
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).build());

        when(traceSampler.sample()).thenReturn(true);
    }

    @After
    public void after() {
        Tracer.unsubscribe("");
    }

    @Test
    public void whenNoTraceIsInHeader_generatesNewTrace() {
        SimpleServiceGrpc.SimpleServiceBlockingStub blockingStub = SimpleServiceGrpc.newBlockingStub(channel);

        blockingStub.unaryRpc(SimpleRequest.newBuilder().build());

        verify(traceSampler).sample();
        verifyNoMoreInteractions(traceSampler);

        verify(spanObserver, atLeastOnce()).consume(spanCaptor.capture());
        List<Span> spans = spanCaptor.getAllValues();
        assertThat(spans).map(Span::getTraceId).containsOnly(spans.get(0).getTraceId());
    }

    @Test
    public void whenTraceIsInHeader_usesGivenTraceId() {
        SimpleServiceGrpc.SimpleServiceBlockingStub blockingStub =
                SimpleServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                                next.newCall(method, callOptions)) {
                            @Override
                            public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                                headers.put(GrpcTracing.TRACE_ID, "traceId");
                                headers.put(GrpcTracing.SPAN_ID, "spanId");
                                headers.put(GrpcTracing.IS_SAMPLED, "1");
                                super.start(responseListener, headers);
                            }
                        };
                    }
                }));

        blockingStub.unaryRpc(SimpleRequest.newBuilder().build());

        verifyNoInteractions(traceSampler);

        verify(spanObserver, atLeastOnce()).consume(spanCaptor.capture());
        List<Span> spans = spanCaptor.getAllValues();
        assertThat(spans).map(Span::getTraceId).containsOnly("traceId");
        assertThat(spans).map(Span::getParentSpanId).contains(Optional.of("spanId"));
    }

    @Test
    public void unaryCallHandlerIsSpanned() {
        SimpleServiceGrpc.SimpleServiceBlockingStub blockingStub = SimpleServiceGrpc.newBlockingStub(channel);

        blockingStub.unaryRpc(SimpleRequest.newBuilder().build());

        verify(traceSampler).sample();
        verifyNoMoreInteractions(traceSampler);

        verify(spanObserver, atLeastOnce()).consume(spanCaptor.capture());
        List<Span> spans = spanCaptor.getAllValues();
        assertThat(spans).map(Span::getOperation).contains("handler");
    }

    @Test
    public void serverStreamObserverIsSpanned() throws Exception {
        SimpleServiceGrpc.SimpleServiceStub stub = SimpleServiceGrpc.newStub(channel);

        StreamRecorder<SimpleResponse> responseObserver = StreamRecorder.create();
        StreamObserver<SimpleRequest> requestObserver = stub.clientStreamingRpc(responseObserver);
        requestObserver.onNext(SimpleRequest.newBuilder().build());
        requestObserver.onCompleted();
        responseObserver.awaitCompletion();

        verify(traceSampler).sample();
        verifyNoMoreInteractions(traceSampler);

        verify(spanObserver, atLeastOnce()).consume(spanCaptor.capture());
        List<Span> spans = spanCaptor.getAllValues();
        assertThat(spans).map(Span::getOperation).contains("handler", "observer");
        assertThat(spans).map(Span::getTraceId).containsOnly(spans.get(0).getTraceId());
    }
}
