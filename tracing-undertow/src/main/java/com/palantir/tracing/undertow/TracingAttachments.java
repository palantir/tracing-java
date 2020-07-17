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

import io.undertow.util.AttachmentKey;

/** Provides public tracing {@link AttachmentKey attachment keys}. */
public final class TracingAttachments {

    /** Attachment to check whether the current request is being traced. */
    public static final AttachmentKey<Boolean> IS_SAMPLED = AttachmentKey.create(Boolean.class);

    /** Attachment providing the request identifier. */
    public static final AttachmentKey<String> REQUEST_ID = AttachmentKey.create(String.class);

    private TracingAttachments() {}
}
