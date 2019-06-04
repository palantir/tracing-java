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

/**
 * Represents the desired observability of a new trace.
 */
public enum Observability {
    /**
     * Force the trace to be sampled.
     */
    SAMPLE,

    /**
     * Force the trace to not be sampled.
     */
    DO_NOT_SAMPLE,

    /**
     * Do not force, and let the tracer decide the observability.
     */
    UNDECIDED
}
