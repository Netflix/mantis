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
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.computation.GroupComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Use to perform another shuffle on the data with a different key
 * It transforms an item of type MantisGroup<K1, T> to MantisGroup<K2,R>
 * @param <K1> Input key type
 * @param <T> Input value type
 * @param <K2> Output key type
 * @param <R> Output value type
 */
public class GroupToGroup<K1, T, K2, R> extends KeyValueStageConfig<T, K2, R> {

    private final GroupComputation<K1, T, K2, R> computation;
    private final long keyExpireTimeSeconds;

    /**
     * @deprecated As of release 0.603, use {@link #GroupToGroup(GroupComputation, Config, Codec)} instead
     */
    GroupToGroup(GroupComputation<K1, T, K2, R> computation,
                 Config<K1, T, K2, R> config, final io.reactivex.netty.codec.Codec<T> inputCodec) {
        this(computation, config, NettyCodec.fromNetty(inputCodec));
    }

    GroupToGroup(GroupComputation<K1, T, K2, R> computation,
                 Config<K1, T, K2, R> config, Codec<T> inputCodec) {
        this(computation, config, (Codec<K1>) Codecs.string(), inputCodec);
    }

    GroupToGroup(GroupComputation<K1, T, K2, R> computation,
                 Config<K1, T, K2, R> config, Codec<K1> inputKeyCodec, Codec<T> inputCodec) {
        super(config.description, inputKeyCodec, inputCodec, config.keyCodec, config.codec, config.inputStrategy, config.parameters);
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

        private Codec<R> codec;
        private Codec<K2> keyCodec;
        private String description;
        private long keyExpireTimeSeconds = TimeUnit.HOURS.toSeconds(1); // 1 hour default
        // input type for keyToKey is serial
        // always assume a stateful calculation is being made
        // do not allow config to override
        private final INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.SERIAL;
        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec
         *
         * @return Config
         *
         * @deprecated As of release 0.603, use {@link #codec(Codec)} instead
         */
        public Config<K1, T, K2, R> codec(final io.reactivex.netty.codec.Codec<R> codec) {
            this.codec = NettyCodec.fromNetty(codec);
            return this;
        }

        public Config<K1, T, K2, R> codec(Codec<R> codec) {
            this.codec = codec;
            return this;
        }

        public Config<K1, T, K2, R> keyCodec(Codec<K2> keyCodec) {
            this.keyCodec = keyCodec;
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

        public Codec<K2> getKeyCodec() {
            return keyCodec;
        }

        public Codec<R> getCodec() {
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
