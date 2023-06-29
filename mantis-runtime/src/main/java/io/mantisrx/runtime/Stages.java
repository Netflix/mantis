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


import io.mantisrx.common.codec.Codec;
import java.util.LinkedList;
import java.util.List;


public class Stages<T> {

    final Codec<?> inputKeyCodec;
    final Codec<T> inputCodec;
    private final SourceHolder<?> source;
    private final List<StageConfig<?, ?>> stages;

    Stages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        this(source, stage, null, inputCodec);
    }

    Stages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<?> inputKeyCodec, Codec<T> inputCodec) {
        this(source, new LinkedList<>(), stage, inputKeyCodec, inputCodec);
    }

    Stages(SourceHolder<?> source, List<StageConfig<?, ?>> stages,
           StageConfig<?, ?> stage, Codec<T> inputCodec) {
        this(source, stages, stage, null, inputCodec);
    }

    Stages(SourceHolder<?> source, List<StageConfig<?, ?>> stages,
           StageConfig<?, ?> stage, Codec<?> inputKeyCodec, Codec<T> inputCodec) {
        this.source = source;
        this.stages = stages;
        this.stages.add(stage);
        this.inputCodec = inputCodec;
        this.inputKeyCodec = inputKeyCodec;
    }

    public List<StageConfig<?, ?>> getStages() {
        return stages;
    }

    public SourceHolder<?> getSource() {
        return source;
    }
}
