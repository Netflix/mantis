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


public class KeyedStages<K1, T> extends Stages<T> {

    KeyedStages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<?> keyCodec, Codec<T> inputCodec) {
        super(source, stage, keyCodec, inputCodec);
    }

    KeyedStages(Stages<?> self, StageConfig<?, ?> stage, Codec<?> keyCodec, Codec<T> inputCodec) {
        super(self.getSource(), self.getStages(), stage, keyCodec, inputCodec);
    }

    public <K2, R> KeyedStages<K2, R> stage(KeyComputation<K1, T, K2, R> computation,
                                    KeyToKey.Config<K1, T, K2, R> config) {
        return new KeyedStages<>(this, new KeyToKey<>(computation, config, (Codec<K1>) keyCodec, inputCodec), config.getKeyCodec(), config.getCodec());
    }

    public <K2, R> KeyedStages<K2, R> stage(GroupComputation<K1, T, K2, R> computation,
                                    GroupToGroup.Config<K1, T, K2, R> config) {
        return new KeyedStages<>(this, new GroupToGroup<>(computation, config, (Codec<K1>) keyCodec, inputCodec), config.getKeyCodec(), config.getCodec());
    }

    public <K, R> ScalarStages<R> stage(ToScalarComputation<K, T, R> computation,
                                     KeyToScalar.Config<K, T, R> config) {
        return new ScalarStages<>(this, new KeyToScalar<>(computation, config, (Codec<K>) keyCodec, inputCodec), config.getCodec());
    }

    public <K, R> ScalarStages<R> stage(GroupToScalarComputation<K, T, R> computation,
                                     GroupToScalar.Config<K, T, R> config) {
        return new ScalarStages<>(this, new GroupToScalar<>(computation, config, (Codec<K>) keyCodec, inputCodec), config.getCodec());
    }
}
