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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mantisrx.runtime.lifecycle.Lifecycle;
import io.mantisrx.runtime.parameter.ParameterDefinition;


public class Job<T> {

    private Metadata metadata;
    private SourceHolder<?> source;
    private List<StageConfig<?, ?>> stages;
    private SinkHolder<T> sink;
    private Lifecycle lifecycle;
    private Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();

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
