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
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.computation.ToKeyComputation;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.Sink;


public class ScalarStages<T> extends Stages<T> {

    ScalarStages(SourceHolder<?> source, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        super(source, stage, inputCodec);
    }

    ScalarStages(Stages<?> self, StageConfig<?, ?> stage, Codec<T> inputCodec) {
        super(self.getSource(), self.getStages(), stage, inputCodec);
    }

    public <R> KeyedStages<R> stage(ToKeyComputation<T, String, R> computation,
                                    ScalarToKey.Config<T, String, R> config) {
        return new KeyedStages<R>(this,
                new ScalarToKey<T, String, R>(computation, config, inputCodec), config.getCodec());
    }

    /**
     * Use instead of ToKeyComputation for high cardinality, high throughput use cases
     *
     * @param computation The computation that transforms a scalar to a group
     * @param config      stage config
     *
     * @return
     */
    public <R> KeyedStages<R> stage(ToGroupComputation<T, String, R> computation,
                                    ScalarToGroup.Config<T, String, R> config) {
        return new KeyedStages<>(this, new ScalarToGroup<>(computation, config, inputCodec), config.getCodec());
    }

    public <R> ScalarStages<R> stage(ScalarComputation<T, R> computation,
                                     ScalarToScalar.Config<T, R> config) {
        return new ScalarStages<R>(this,
                new ScalarToScalar<T, R>(computation, config, inputCodec), config.getCodec());
    }

    public Config<T> sink(Sink<T> sink) {
        return new Config<T>(this, new SinkHolder<T>(sink));
    }

    public Config<T> sink(SelfDocumentingSink<T> sink) {
        return new Config<T>(this, new SinkHolder<T>(sink));
    }
}
