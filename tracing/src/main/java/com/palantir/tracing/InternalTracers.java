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

package com.palantir.tracing;

import java.util.Optional;

/**
 * Internal utilities meant for consumption only inside of the tracing codebase.
 *
 * <p>Note: while this is internal, backwards compatibility should be preserved for a couple releases to prevent
 * breaking libraries which resolve mis-matched tracing dependency versions.
 */
public final class InternalTracers {

    /** Returns true if the provided detachedSpan is sampled. */
    public static boolean isSampled(DetachedSpan detachedSpan) {}

    /** Returns true if the provided detachedSpan is sampled. */
    public static Optional<String> getRequestId(DetachedSpan detachedSpan) {}

    private InternalTracers() {}
}
