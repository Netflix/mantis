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

package io.mantisrx.runtime;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.List;

/** An intermediate abstract class that holds the keyCodec for the output key
 *  for mantis events
 *
 * @param <T> Input datatype
 * @param <K> Output keytype
 * @param <R> Output value datatype
 */
public abstract class KeyValueStageConfig<T, K, R> extends StageConfig<T, R> {

    private final Codec<K> keyCodec;

    public KeyValueStageConfig(String description, Codec<?> inputKeyCodec, Codec<T> inputCodec, Codec<K> outputKeyCodec, Codec<R> outputCodec, INPUT_STRATEGY inputStrategy, List<ParameterDefinition<?>> params) {
        this(description, inputKeyCodec, inputCodec, outputKeyCodec, outputCodec, inputStrategy, params, DEFAULT_STAGE_CONCURRENCY);
    }

    public KeyValueStageConfig(String description, Codec<?> inputKeyCodec, Codec<T> inputCodec, Codec<K> outputKeyCodec, Codec<R> outputCodec, INPUT_STRATEGY inputStrategy, List<ParameterDefinition<?>> params, int concurrency) {
        super(description, inputKeyCodec, inputCodec, outputCodec, inputStrategy, params, concurrency);
        this.keyCodec = outputKeyCodec;
    }

    public Codec<K> getOutputKeyCodec() {
        if (this.keyCodec == null) {
            return (Codec<K>) Codecs.string();
        }
        return this.keyCodec;
    }
}
