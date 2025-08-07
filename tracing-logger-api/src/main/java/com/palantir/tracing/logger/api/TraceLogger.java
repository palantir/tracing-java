/*
 * (c) Copyright 2025 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.tracing.logger.api;

import com.google.errorprone.annotations.CheckReturnValue;
import com.palantir.logsafe.Safe;

@CheckReturnValue
public interface TraceLogger {

    boolean isTraceEnabled();

    Span trace(@Safe String operation);

    boolean isDebugEnabled();

    Span debug(@Safe String operation);

    boolean isInfoEnabled();

    Span info(@Safe String operation);

    boolean isWarnEnabled();

    Span warn(@Safe String operation);

    boolean isErrorEnabled();

    Span error(@Safe String operation);
}
