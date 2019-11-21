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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.tracing.api.Serialization;
import com.palantir.tracing.api.Span;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class HtmlFormatterTest {

    @TempDir public Path path;

    @ParameterizedTest
    @EnumSource(LayoutStrategy.class)
    void should_render_sensible_html(LayoutStrategy strategy) throws IOException {
        List<Span> spans = Serialization.deserialize(Paths.get("src/test/resources/log-receiver.txt"));
        Path output = Paths.get(String.format("src/test/resources/log-receiver-%s.html", strategy));

        if (!Files.exists(output) || Boolean.valueOf(System.getProperty("recreate", "false"))) {
            HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                    .spans(spans)
                    .path(output)
                    .displayName("log-receiver")
                    .layoutStrategy(strategy)
                    .build());
            return;
        }

        Path file = Files.createTempFile("log-receiver", ".html");
        HtmlFormatter.render(HtmlFormatter.RenderConfig.builder()
                .spans(spans)
                .path(file)
                .displayName("log-receiver")
                .layoutStrategy(strategy)
                .build());

        assertThat(file).hasSameContentAs(output);
    }
}
