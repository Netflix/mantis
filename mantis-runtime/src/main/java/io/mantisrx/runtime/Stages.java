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


import java.util.LinkedList;
import java.util.List;

import io.mantisrx.common.codec.Codec;


public class Stages<T> {

    Codec<T> inputCodec;
    private SourceHolder<?> source;
    private List<StageConfig<?, ?>> stages;

    Stages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        this.source = source;
        this.stages = new LinkedList<StageConfig<?, ?>>();
        this.stages.add(stage);
        this.inputCodec = inputCodec;
    }

    Stages(SourceHolder<?> source, List<StageConfig<?, ?>> stages,
           StageConfig<?, ?> stage, Codec<T> inputCodec) {
        this.source = source;
        this.stages = stages;
        this.stages.add(stage);
        this.inputCodec = inputCodec;
    }

    public List<StageConfig<?, ?>> getStages() {
        return stages;
    }

    public SourceHolder<?> getSource() {
        return source;
    }
}
