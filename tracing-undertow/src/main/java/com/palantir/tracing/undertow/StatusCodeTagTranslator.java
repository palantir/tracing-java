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

package com.palantir.tracing.undertow;

import com.palantir.tracing.TagTranslator;
import io.undertow.server.HttpServerExchange;

/**
 * Simple default request tracing {@link TagTranslator} which applies a {@code status} tag with the response status
 * code.
 */
enum StatusCodeTagTranslator implements TagTranslator<HttpServerExchange> {
    INSTANCE;

    @Override
    public <T> void translate(TagAdapter<T> adapter, T target, HttpServerExchange exchange) {
        adapter.tag(target, "status", statusString(exchange.getStatusCode()));
    }

    static String statusString(int statusCode) {
        // handle common cases quickly
        switch (statusCode) {
            case 200:
                return "200";
            case 204:
                return "204";
        }
        return Integer.toString(statusCode);
    }
}
