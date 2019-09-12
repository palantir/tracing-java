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

package com.palantir.tracing2;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public final class Spans {
    private static final ThreadLocal<Span> currentSpan = new ThreadLocal<>();
    private static final List<Consumer<CompletedSpan>> consumers = Lists.newCopyOnWriteArrayList();

    public static void clearAllConsumers() {
        consumers.clear();
    }

    public static void register(Consumer<CompletedSpan> consumer) {
        consumers.add(consumer);
    }

    /** Creates a new child span for the thread-attached current span. */
    public static Span forCurrentTrace(String opName) {
        return getThreadSpan()
                .orElse(EmptySpan.INSTANCE)
                .child(opName);
    }

    /** Notifies all registered consumers of this completed span. */
    public static void notify(CompletedSpan span) {
        for (Consumer<CompletedSpan> consumer : consumers) {
            consumer.accept(span);
        }
    }

    static void setThreadSpan(Span span) {
        currentSpan.set(span);
    }

    static Optional<Span> getThreadSpan() {
        return Optional.ofNullable(currentSpan.get());
    }

    static void attach(Optional<Span> span) {
        span.ifPresent(currentSpan::set);
    }

    static void remove() {
        currentSpan.remove();
    }

    private Spans() {}
}
