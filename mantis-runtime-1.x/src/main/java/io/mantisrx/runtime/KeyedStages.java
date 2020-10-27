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
import io.mantisrx.runtime.computation.GroupComputation;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.computation.KeyComputation;
import io.mantisrx.runtime.computation.ToScalarComputation;


public class KeyedStages<T> extends Stages<T> {

    KeyedStages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        super(source, stage, inputCodec);
    }

    KeyedStages(Stages<?> self, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        super(self.getSource(), self.getStages(), stage, inputCodec);
    }

    public <R> KeyedStages<R> stage(KeyComputation<String, T, String, R> computation,
                                    KeyToKey.Config<String, T, String, R> config) {
        return new KeyedStages<R>(this, new KeyToKey<String, T, String, R>(computation, config, inputCodec), config.getCodec());
    }

    public <R> KeyedStages<R> stage(GroupComputation<String, T, String, R> computation,
                                    GroupToGroup.Config<String, T, String, R> config) {
        return new KeyedStages<R>(this, new GroupToGroup<String, T, String, R>(computation, config, inputCodec), config.getCodec());
    }

    public <R> ScalarStages<R> stage(ToScalarComputation<String, T, R> computation,
                                     KeyToScalar.Config<String, T, R> config) {
        return new ScalarStages<R>(this, new KeyToScalar<String, T, R>(computation, config, inputCodec), config.getCodec());
    }

    public <R> ScalarStages<R> stage(GroupToScalarComputation<String, T, R> computation,
                                     GroupToScalar.Config<String, T, R> config) {
        return new ScalarStages<R>(this, new GroupToScalar<String, T, R>(computation, config, inputCodec), config.getCodec());
    }
}
