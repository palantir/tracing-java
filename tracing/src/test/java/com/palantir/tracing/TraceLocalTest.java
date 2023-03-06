/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.tracing.TraceLocal.Observer;
import java.util.Optional;
import org.junit.Test;

public final class TraceLocalTest {

    @Test
    public void testOnTraceComplete_noTraceLocals() {
        Observer<String> observer = mock(Observer.class);
        TraceLocal<String> traceLocal =
                TraceLocal.<String>builder().observer(observer).build();

        traceLocal.onTraceComplete(TraceState.of("traceid", Optional.of("requestid"), Optional.empty()));
        // observer not called because there are no trace locals
        verifyNoInteractions(observer);
    }

    @Test
    public void testOnTraceComplete_thisTraceLocalNotSet() {
        Observer<String> observer = mock(Observer.class);
        TraceLocal<String> traceLocal =
                TraceLocal.<String>builder().observer(observer).build();
        TraceLocal<String> other = TraceLocal.of();

        TraceState traceState = TraceState.of("traceid", Optional.of("requestid"), Optional.empty());
        traceState.getOrCreateTraceLocals().put(other, "other");

        traceLocal.onTraceComplete(traceState);
        // observer not called because this trace local not set
        verifyNoInteractions(observer);
    }

    @Test
    public void testOnTraceComplete() {
        Observer<String> observer = mock(Observer.class);
        TraceLocal<String> traceLocal =
                TraceLocal.<String>builder().observer(observer).build();

        TraceState traceState = TraceState.of("traceid", Optional.of("requestid"), Optional.empty());
        traceState.getOrCreateTraceLocals().put(traceLocal, "value");

        traceLocal.onTraceComplete(traceState);
        verify(observer, times(1)).onTraceComplete("traceid", "requestid", "value");

        // empty request id
        traceState = TraceState.of("traceid", Optional.empty(), Optional.empty());
        traceState.getOrCreateTraceLocals().put(traceLocal, "value");

        traceLocal.onTraceComplete(traceState);
        verify(observer, times(1)).onTraceComplete("traceid", null, "value");

        verifyNoMoreInteractions(observer);
    }
}
