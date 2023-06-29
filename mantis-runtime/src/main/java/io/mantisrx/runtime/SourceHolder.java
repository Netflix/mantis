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
import io.mantisrx.runtime.source.SelfDocumentingSource;
import io.mantisrx.runtime.source.Source;


public class SourceHolder<T> {

    private Metadata metadata;
    private final Source<T> sourceFunction;

    // There is no need for input codec
    // for a source, if called
    // throw exception to detect error
    private final Codec<T> failCodec = new Codec<T>() {
        @Override
        public byte[] encode(T value) {
            throw new RuntimeException("Attempting to encode source data");
        }

        @Override
        public T decode(byte[] bytes) {
            throw new RuntimeException("Attempting to decode source data");
        }
    };

    SourceHolder(Source<T> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    SourceHolder(SelfDocumentingSource<T> sourceFunction) {
        this.metadata = sourceFunction.metadata();
        this.sourceFunction = sourceFunction;
    }

    public Source<T> getSourceFunction() {
        return sourceFunction;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public <K, R> KeyedStages<K, R> stage(ToKeyComputation<T, K, R> computation,
                                    ScalarToKey.Config<T, K, R> config) {
        return new KeyedStages<>(this, new ScalarToKey<>(computation, config, failCodec), config.getKeyCodec(), config.getCodec());
    }

    /**
     * Use instead of ToKeyComputation for high cardinality, high throughput use cases
     *
     * @param computation The computation that transforms a scalar to a group
     * @param config      stage config
     *
     * @return KeyedStages
     */
    public <K, R> KeyedStages<K, R> stage(ToGroupComputation<T, K, R> computation,
                                    ScalarToGroup.Config<T, K, R> config) {
        return new KeyedStages<>(this, new ScalarToGroup<>(computation, config, failCodec), config.getKeyCodec(), config.getCodec());
    }


    public <R> ScalarStages<R> stage(ScalarComputation<T, R> computation,
                                     ScalarToScalar.Config<T, R> config) {
        return new ScalarStages<>(this, new ScalarToScalar<>(computation, config, failCodec), config.getCodec());
    }
}
