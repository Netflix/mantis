/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink;

import com.google.common.collect.Lists;
import io.mantisrx.connector.iceberg.sink.committer.IcebergCommitterStage;
import io.mantisrx.connector.iceberg.sink.config.SinkProperties;
import io.mantisrx.connector.iceberg.sink.writer.IcebergWriterStage;
import io.mantisrx.connector.iceberg.sink.writer.config.WriterProperties;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageOverrideParameters {

    private StageOverrideParameters() {
    }

    public static Parameters newParameters() {
        Map<String, ParameterDefinition<?>> definition = new HashMap<>();
        IcebergWriterStage.parameters().forEach(p -> definition.put(p.getName(), p));
        IcebergCommitterStage.parameters().forEach(p -> definition.put(p.getName(), p));

        List<Parameter> parameters = Lists.newArrayList(
            new Parameter(SinkProperties.SINK_CATALOG, "catalog"),
            new Parameter(SinkProperties.SINK_DATABASE, "database"),
            new Parameter(SinkProperties.SINK_TABLE, "table"),
            new Parameter(WriterProperties.WRITER_FLUSH_FREQUENCY_MSEC, "500"),
            new Parameter(WriterProperties.WRITER_MAXIMUM_POOL_SIZE, "10")
        );

        return ParameterUtils.createContextParameters(definition, parameters);
    }
}
