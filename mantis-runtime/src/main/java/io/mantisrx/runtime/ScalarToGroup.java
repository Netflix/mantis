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
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.Collections;
import java.util.List;


/**
 * This should be used as an alternative to ScalarToKey for high cardinality, high volume data.
 * It transforms a T to  MantisGroup K,R
 *
 * @param <T> Input data type
 * @param <K> Key type
 * @param <R> Output data type
 *
 * @author njoshi
 */

public class ScalarToGroup<T, K, R> extends KeyValueStageConfig<T, K, R> {

    private ToGroupComputation<T, K, R> computation;
    private long keyExpireTimeSeconds;


    /**
     * @param computation
     * @param config
     * @param inputCodec
     *
     * @deprecated As of release 0.603, use {@link #ScalarToGroup(ToGroupComputation, Config, Codec)} instead
     */
    ScalarToGroup(ToGroupComputation<T, K, R> computation,
                  Config<T, K, R> config, final io.reactivex.netty.codec.Codec<T> inputCodec) {
        this(computation, config, NettyCodec.fromNetty(inputCodec));
    }

    ScalarToGroup(ToGroupComputation<T, K, R> computation,
                  Config<T, K, R> config, Codec<T> inputCodec) {
        super(config.description, null, inputCodec, config.keyCodec, config.codec, config.inputStrategy, config.parameters);
        this.computation = computation;
        this.keyExpireTimeSeconds = config.keyExpireTimeSeconds;

    }

    public ToGroupComputation<T, K, R> getComputation() {
        return computation;
    }

    public long getKeyExpireTimeSeconds() {
        return keyExpireTimeSeconds;
    }


    public static class Config<T, K, R> {

        private Codec<R> codec;
        private Codec<K> keyCodec;
        private String description;
        // default input type is concurrent for 'grouping' use case
        private INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.CONCURRENT;
        private long keyExpireTimeSeconds = Long.MAX_VALUE; // never expire by default
        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec
         *
         * @return
         *
         * @deprecated As of release 0.603, use {@link #codec(Codec)} instead
         */
        public Config<T, K, R> codec(final io.reactivex.netty.codec.Codec<R> codec) {
            this.codec = NettyCodec.fromNetty(codec);
            return this;
        }

        public Config<T, K, R> codec(Codec<R> codec) {
            this.codec = codec;
            return this;
        }

        public Config<T, K, R> keyCodec(Codec<K> keyCodec) {
            this.keyCodec = keyCodec;
            return this;
        }

        public Config<T, K, R> keyExpireTimeSeconds(long seconds) {
            this.keyExpireTimeSeconds = seconds;
            return this;
        }

        public Config<T, K, R> serialInput() {
            this.inputStrategy = INPUT_STRATEGY.SERIAL;
            return this;
        }

        public Config<T, K, R> concurrentInput() {
            this.inputStrategy = INPUT_STRATEGY.CONCURRENT;
            return this;
        }

        public Config<T, K, R> description(String description) {
            this.description = description;
            return this;
        }

        public Codec<R> getCodec() {
            return codec;
        }

        public Codec<K> getKeyCodec() {
            return keyCodec;
        }

        public String getDescription() {
            return description;
        }

        public INPUT_STRATEGY getInputStrategy() {
            return inputStrategy;
        }

        public long getKeyExpireTimeSeconds() {
            return keyExpireTimeSeconds;
        }

        public Config<T, K, R> withParameters(List<ParameterDefinition<?>> params) {
            this.parameters = params;
            return this;
        }
    }
}
