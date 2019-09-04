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

package com.palantir.tracing.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public final class Serialization {

    private static final ObjectMapper mapper = ObjectMappers.newServerObjectMapper();

    private Serialization() {}

    public static List<Span> deserialize(Path file) throws IOException {
        try (Stream<String> lines = Files.lines(file)) {
            return lines.map(line -> {
                try {
                    return mapper.readValue(line, SerializableSpan.class).asSpan();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(ImmutableList.toImmutableList());
        }
    }

    public static void serialize(Path file, Collection<Span> allSpans) throws IOException {
        Files.createDirectories(file.getParent());
        try (OutputStream outputStream = Files.newOutputStream(file)) {
            allSpans.forEach(span -> {
                try {
                    byte[] bytes = mapper.writeValueAsBytes(SerializableSpan.fromSpan(span));
                    outputStream.write(bytes);
                    outputStream.write('\n');
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static String toString(Span span) {
        try {
            return mapper.writeValueAsString(span);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to JSON serialize span " + span, e);
        }
    }
}
