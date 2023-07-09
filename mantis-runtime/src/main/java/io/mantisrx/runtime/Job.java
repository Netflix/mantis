/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.runtime;

import io.mantisrx.runtime.lifecycle.Lifecycle;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.List;
import java.util.Map;


public class Job<T> {

    private final Metadata metadata;
    private final SourceHolder<?> source;
    private final List<StageConfig<?, ?>> stages;
    private final SinkHolder<T> sink;
    private final Lifecycle lifecycle;
    private final Map<String, ParameterDefinition<?>> parameterDefinitions;

    Job(SourceHolder<?> source, List<StageConfig<?, ?>> stages, SinkHolder<T> sink,
        Lifecycle lifecycle, Metadata metadata,
        Map<String, ParameterDefinition<?>> parameterDefinitions) {
        this.source = source;
        this.stages = stages;
        this.sink = sink;
        this.lifecycle = lifecycle;
        this.metadata = metadata;
        this.parameterDefinitions = parameterDefinitions;
    }

    public Map<String, ParameterDefinition<?>> getParameterDefinitions() {
        return parameterDefinitions;
    }

    public Lifecycle getLifecycle() {
        return lifecycle;
    }

    public SourceHolder<?> getSource() {
        return source;
    }

    public List<StageConfig<?, ?>> getStages() {
        return stages;
    }

    public SinkHolder<T> getSink() {
        return sink;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
