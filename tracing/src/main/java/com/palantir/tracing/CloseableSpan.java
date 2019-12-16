/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.io.Closeable;

/** Closeable marker around a tracing span operation. This object should be used in a try/with block. */
public interface CloseableSpan extends Closeable {

    /**
     * Completes the Span marked by this {@link CloseableSpan}.
     *
     * <p>{@link #close} must be invoked on the same thread which started this span.
     */
    @Override
    void close();
}
