/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.agent;

import io.mantisrx.server.master.client.config.MetricsCollector;
import io.mantisrx.server.master.client.config.Usage;
import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("unused")
public class DummyMetricsCollector implements MetricsCollector {
    @SuppressWarnings("unused")
    public static DummyMetricsCollector valueOf(Properties properties) {
        return new DummyMetricsCollector();
    }

    @Override
    public Usage get() throws IOException {
        return Usage.builder()
            .cpusLimit(1)
            .cpusSystemTimeSecs(100.)
            .cpusUserTimeSecs(100.)
            .networkWriteBytes(100.)
            .networkReadBytes(100.)
            .memAnonBytes(100.)
            .memLimit(200.)
            .memRssBytes(200.)
            .build();
    }
}
