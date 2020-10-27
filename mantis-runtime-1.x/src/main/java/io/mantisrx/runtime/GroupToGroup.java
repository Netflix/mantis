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

import io.mantisrx.runtime.computation.GroupComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.reactivex.netty.codec.Codec;


public class GroupToGroup<K1, T, K2, R> extends StageConfig<T, R> {

    private GroupComputation<K1, T, K2, R> computation;
    private long keyExpireTimeSeconds;

    /**
     * @deprecated As of release 0.603, use {@link #GroupToGroup(GroupComputation, Config, io.mantisrx.common.codec.Codec)} instead
     */
    GroupToGroup(GroupComputation<K1, T, K2, R> computation,
                 Config<K1, T, K2, R> config, final Codec<T> inputCodec) {
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

    GroupToGroup(GroupComputation<K1, T, K2, R> computation,
                 Config<K1, T, K2, R> config, io.mantisrx.common.codec.Codec<T> inputCodec) {
        super(config.description, inputCodec, config.codec, config.inputStrategy, config.parameters);
        this.computation = computation;
        this.keyExpireTimeSeconds = config.keyExpireTimeSeconds;
    }

    public GroupComputation<K1, T, K2, R> getComputation() {
        return computation;
    }

    public long getKeyExpireTimeSeconds() {
        return keyExpireTimeSeconds;
    }

    public static class Config<K1, T, K2, R> {

        private io.mantisrx.common.codec.Codec<R> codec;
        private String description;
        private long keyExpireTimeSeconds = 3600 * 1; // 1 hour default
        // input type for keyToKey is serial
        // always assume a stateful calculation is being made
        // do not allow config to override
        private INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.SERIAL;
        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec
         *
         * @return
         *
         * @deprecated As of release 0.603, use {@link #codec(io.mantisrx.common.codec.Codec)} instead
         */
        public Config<K1, T, K2, R> codec(final Codec<R> codec) {
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

        public Config<K1, T, K2, R> codec(io.mantisrx.common.codec.Codec<R> codec) {
            this.codec = codec;
            return this;
        }

        public Config<K1, T, K2, R> keyExpireTimeSeconds(long seconds) {
            this.keyExpireTimeSeconds = seconds;
            return this;
        }

        public Config<K1, T, K2, R> description(String description) {
            this.description = description;
            return this;
        }

        public io.mantisrx.common.codec.Codec<R> getCodec() {
            return codec;
        }

        public String getDescription() {
            return description;
        }

        public long getKeyExpireTimeSeconds() {
            return keyExpireTimeSeconds;
        }

        public INPUT_STRATEGY getInputStrategy() {
            return inputStrategy;
        }

        public Config<K1, T, K2, R> withParameters(List<ParameterDefinition<?>> params) {
            this.parameters = params;
            return this;
        }

    }

}
