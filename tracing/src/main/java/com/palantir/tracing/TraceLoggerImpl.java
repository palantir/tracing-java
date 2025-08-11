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

package com.palantir.tracing;

import com.palantir.logsafe.Safe;
import com.palantir.tracing.api.SpanType;
import com.palantir.tracing.logger.api.Span;
import com.palantir.tracing.logger.api.TraceLogger;

final class TraceLoggerImpl implements TraceLogger {

    private final String name;

    TraceLoggerImpl(String name) {
        this.name = name;
    }

    @Override
    public boolean isTraceEnabled() {
        return Tracer.isEnabled(Level.TRACE, name);
    }

    @Override
    public Span trace(@Safe String operation) {
        return span(Level.TRACE, operation);
    }

    @Override
    public boolean isDebugEnabled() {
        return Tracer.isEnabled(Level.DEBUG, name);
    }

    @Override
    public Span debug(@Safe String operation) {
        return span(Level.DEBUG, operation);
    }

    @Override
    public boolean isInfoEnabled() {
        return Tracer.isEnabled(Level.INFO, name);
    }

    @Override
    public Span info(@Safe String operation) {
        return span(Level.INFO, operation);
    }

    @Override
    public boolean isWarnEnabled() {
        return Tracer.isEnabled(Level.WARN, name);
    }

    @Override
    public Span warn(@Safe String operation) {
        return span(Level.WARN, operation);
    }

    @Override
    public boolean isErrorEnabled() {
        return Tracer.isEnabled(Level.ERROR, name);
    }

    @Override
    public Span error(@Safe String operation) {
        return span(Level.ERROR, operation);
    }

    private Span span(Level level, String operation) {
        return Tracer.newSpan(level, operation, SpanType.LOCAL);
    }
}
