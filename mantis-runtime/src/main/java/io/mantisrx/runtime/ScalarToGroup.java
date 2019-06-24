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

import java.util.Collections;
import java.util.List;

import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.reactivex.netty.codec.Codec;


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

public class ScalarToGroup<T, K, R> extends StageConfig<T, R> {

    private ToGroupComputation<T, K, R> computation;
    private long keyExpireTimeSeconds;


    /**
     * @param computation
     * @param config
     * @param inputCodec
     *
     * @deprecated As of release 0.603, use {@link #ScalarToGroup(ToGroupComputation, Config, io.mantisrx.common.codec.Codec)} instead
     */
    ScalarToGroup(ToGroupComputation<T, K, R> computation,
                  Config<T, K, R> config, final Codec<T> inputCodec) {
        super(config.description, new io.mantisrx.common.codec.Codec<T>() {
            @Override
            public T decode(byte[] bytes) {
                return inputCodec.decode(bytes);
            }

            @Override
            public byte[] encode(T value) {
                return inputCodec.encode(value);
            }
        }, config.codec, config.inputStrategy, config.parameters);
        this.computation = computation;
        this.keyExpireTimeSeconds = config.keyExpireTimeSeconds;

    }

    ScalarToGroup(ToGroupComputation<T, K, R> computation,
                  Config<T, K, R> config, io.mantisrx.common.codec.Codec<T> inputCodec) {
        super(config.description, inputCodec, config.codec, config.inputStrategy, config.parameters);
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

        private io.mantisrx.common.codec.Codec<R> codec;
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
         * @deprecated As of release 0.603, use {@link #codec(io.mantisrx.common.codec.Codec)} instead
         */
        public Config<T, K, R> codec(final Codec<R> codec) {
            this.codec = new io.mantisrx.common.codec.Codec<R>() {
                @Override
                public R decode(byte[] bytes) {
                    return codec.decode(bytes);
                }

                @Override
                public byte[] encode(R value) {
                    return codec.encode(value);
                }
            };
            return this;
        }

        public Config<T, K, R> codec(io.mantisrx.common.codec.Codec<R> codec) {
            this.codec = codec;
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

        public io.mantisrx.common.codec.Codec<R> getCodec() {
            return codec;
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
