/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.tracing.undertow;

import com.palantir.tracing.Detached;
import com.palantir.tracing.DetachedSpan;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Attachable;
import io.undertow.util.AttachmentKey;
import javax.annotation.Nullable;

/** Provides public tracing {@link AttachmentKey attachment keys}. */
public final class TracingAttachments {

    /** Attachment to check whether the current request is being traced. */
    public static final AttachmentKey<Boolean> IS_SAMPLED = AttachmentKey.create(Boolean.class);

    /** Attachment providing the request identifier. */
    public static final AttachmentKey<String> REQUEST_ID = AttachmentKey.create(String.class);

    /**
     * Detached span object representing the entire request including asynchronous components.
     * This is intentionally not public, we expose only the {@link Detached} component which critically does not
     * include {@link DetachedSpan#complete()} APIs.
     */
    static final AttachmentKey<DetachedSpan> REQUEST_SPAN = AttachmentKey.create(DetachedSpan.class);

    /**
     * Gets the {@link Detached} trace state which represents the top-level request being processed. This may
     * be used to apply thread state to code executing outside traced handlers, exchange completion
     * listeners, for example.
     */
    @Nullable
    public static Detached requestTrace(HttpServerExchange exchange) {
        return requestTrace((Attachable) exchange);
    }

    /**
     * Gets the {@link Detached} trace state which represents the top-level request being processed. This may
     * be used to apply thread state to code executing outside traced handlers, exchange completion
     * listeners, for example.
     * <p>
     * Note that this intentionally returns a {@link Detached} rather than {@link DetachedSpan} because
     * this framework is responsible for closing the root request span.
     */
    @Nullable
    public static Detached requestTrace(Attachable exchange) {
        return exchange.getAttachment(REQUEST_SPAN);
    }

    private TracingAttachments() {}
}
