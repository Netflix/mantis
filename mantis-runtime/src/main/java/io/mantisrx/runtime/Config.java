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
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Config<T> {

    private Metadata metadata = new Metadata();
    private final SourceHolder<?> source;
    private final List<StageConfig<?, ?>> stages;
    private final SinkHolder<T> sink;
    private Lifecycle lifecycle = DefaultLifecycleFactory.getInstance();
    private final Map<String, ParameterDefinition<?>> parameterDefinitions = new HashMap<>();

    Config(Stages<?> stages, SinkHolder<T> observable) {
        this.source = stages.getSource();
        this.stages = stages.getStages();
        this.sink = observable;
        initParams();
    }

    private void putParameterOnce(ParameterDefinition<?> definition) {
        final String name = definition.getName();
        if (parameterDefinitions.containsKey(name)) {
            throw new IllegalArgumentException("cannot have two parameters with same name " + name);
        }
        parameterDefinitions.put(name, definition);
    }

    private void initParams() {
        // add parameters from Source, Stage and Sink and ensure we don't have naming conflicts between params defined by Source, Stage and Sink
        source.getSourceFunction().getParameters().forEach(this::putParameterOnce);
        for (StageConfig<?, ?> stage : stages) {
            stage.getParameters().forEach(this::putParameterOnce);
        }
        sink.getSinkAction().getParameters().forEach(this::putParameterOnce);
    }

    public Config<T> lifecycle(Lifecycle lifecycle) {
        this.lifecycle = lifecycle;
        return this;
    }

    public Config<T> metadata(Metadata metadata) {
        this.metadata = metadata;
        return this;
    }

    /**
     * define a parameter definition at the Job level, this allows overriding defaults
     * for parameters that might be already defined by a Source, Stage or Sink
     *
     * @param definition Parameter definition
     *
     * @return Config object
     */
    public Config<T> parameterDefinition(ParameterDefinition<?> definition) {
        parameterDefinitions.put(definition.getName(), definition);
        return this;
    }

    public Job<T> create() {
        return new Job<>(source, stages, sink, lifecycle,
            metadata, parameterDefinitions);
    }
}
