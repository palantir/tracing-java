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

import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HtmlFormatterTest {

    @Test
    void jalkjdhala(@TempDir Path path) throws IOException {

        List<Span> spans = Serialization.deserialize(Paths.get("src/test/resources/log-receiver.txt"));

        Path file = Files.createTempFile("test", ".html");
//        Path file = Paths.get("/Users/dfox/Downloads/file.html");

        HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                .spans(spans)
                .path(file)
                .displayName("poop")
                .layoutStrategy(HtmlFormatter.LayoutStrategy.CHRONOLOGICAL)
                .build());
    }
}
