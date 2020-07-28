/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.mantisrx.connector.iceberg.sink.config.SinkProperties;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties;
import io.mantisrx.runtime.parameter.Parameters;

public class StageOverrideParameters {

    private StageOverrideParameters() {
    }

    public static Parameters newParameters() {
        Map<String, Object> state = new HashMap<>();
        Set<String> required = new HashSet<>();

        required.add(SinkProperties.SINK_CATALOG);
        state.put(SinkProperties.SINK_CATALOG, "catalog");

        required.add(SinkProperties.SINK_DATABASE);
        state.put(SinkProperties.SINK_DATABASE, "database");

        required.add(SinkProperties.SINK_TABLE);
        state.put(SinkProperties.SINK_TABLE, "table");

        required.add(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC);
        state.put(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC, "5000");

        return new Parameters(state, required, required);
    }
}
