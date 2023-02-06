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

package io.mantisrx.runtime.core;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.runtime.Config;
import io.mantisrx.runtime.GroupToGroup;
import io.mantisrx.runtime.GroupToScalar;
import io.mantisrx.runtime.KeyToKey;
import io.mantisrx.runtime.KeyToScalar;
import io.mantisrx.runtime.KeyedStages;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.ScalarStages;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.ScalarToKey;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.SourceHolder;
import io.mantisrx.runtime.Stages;
import io.mantisrx.runtime.computation.Computation;
import io.mantisrx.runtime.computation.GroupComputation;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.computation.KeyComputation;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.computation.ToKeyComputation;
import io.mantisrx.runtime.computation.ToScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MantisJobBuilder {
    private SourceHolder<?> sourceHolder;
    private Stages<?> currentStage;
    private Config<?> jobConfig;

    MantisJobBuilder() {}

    public Config<?> buildJobConfig() {
        Preconditions.checkNotNull(jobConfig, "Need to configure a sink for the stream to build MantisJob. `jobConfig` is null!");
        return jobConfig;
    }

    public MantisJobBuilder addStage(Source<?> source) {
        this.sourceHolder = MantisJob.source(source);
        return this;
    }

    public MantisJobBuilder addStage(Computation compFn, Codec<?> codec) {
        if (compFn == null) {
            return null;
        }
        return this.addStage(compFn, codec, null);
    }

    public MantisJobBuilder addStage(Computation compFn, Codec<?> codec, Codec<?> keyCodec) {
        if (compFn == null) {
            return this;
        }
        if (this.sourceHolder == null) {
            throw new IllegalArgumentException(
                "SourceHolder not currently set. Uninitialized MantisJob configuration");
        }
        if (currentStage == null) {
            this.currentStage = addInitStage(compFn, codec, keyCodec);
        } else {
            this.currentStage = addMoreStages(compFn, codec, keyCodec);
        }
        return this;
    }

    public MantisJobBuilder addStage(Sink<?> sink) {
        ScalarStages scalarStages = (ScalarStages) this.currentStage;
        this.jobConfig = scalarStages.sink(sink);
        return this;
    }

    public MantisJobBuilder addStage(SelfDocumentingSink<?> sink) {
        ScalarStages scalarStages = (ScalarStages) this.currentStage;
        this.jobConfig = scalarStages.sink(sink);
        return this;
    }

    private Stages<?> addInitStage(Computation compFn, Codec<?> codec, Codec<?> keyCodec) {
        if (compFn instanceof ScalarComputation) {
            return this.sourceHolder.stage((ScalarComputation) compFn, new ScalarToScalar.Config().codec(codec));
        } else if (compFn instanceof ToGroupComputation) {
            return this.sourceHolder.stage((ToGroupComputation) compFn, new ScalarToGroup.Config().codec(codec).keyCodec(keyCodec));
        }
        return this.sourceHolder.stage((ToKeyComputation) compFn, new ScalarToKey.Config().codec(codec).keyCodec(keyCodec));
    }

    private Stages<?> addMoreStages(Computation compFn, Codec<?> codec, Codec<?> keyCodec) {
        if (this.currentStage instanceof ScalarStages) {
            ScalarStages scalarStages = (ScalarStages) this.currentStage;
            if (compFn instanceof ScalarComputation) {
                return scalarStages.stage((ScalarComputation) compFn, new ScalarToScalar.Config().codec(codec));
            } else if (compFn instanceof ToKeyComputation) {
                return scalarStages.stage((ToKeyComputation) compFn, new ScalarToKey.Config().codec(codec).keyCodec(keyCodec));
            }
            return scalarStages.stage((ToGroupComputation) compFn, new ScalarToGroup.Config().codec(codec).keyCodec(keyCodec));
        }
        KeyedStages keyedStages = (KeyedStages) this.currentStage;
        if (compFn instanceof ToScalarComputation) {
            return keyedStages.stage((ToScalarComputation) compFn, new KeyToScalar.Config().codec(codec));
        } else if (compFn instanceof GroupToScalarComputation) {
            return keyedStages.stage((GroupToScalarComputation) compFn, new GroupToScalar.Config().codec(codec));
        } else if (compFn instanceof KeyComputation) {
            return keyedStages.stage((KeyComputation) compFn, new KeyToKey.Config().codec(codec).keyCodec(keyCodec));
        }
        return keyedStages.stage((GroupComputation) compFn, new GroupToGroup.Config().codec(codec).keyCodec(keyCodec));
    }

    public MantisJobBuilder addParameters(Iterable<ParameterDefinition<?>> params) {
        params.forEach(p -> this.jobConfig = this.jobConfig.parameterDefinition(p));
        return this;
    }
}
