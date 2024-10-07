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
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Alternative to ToScalar which expects GroupedObservables as input.
 * This computation should be used instead of ToScalar for high cardinality, high volume
 * data.
 *
 * @param <K> Type of the key
 * @param <T> Type of the value
 * @param <R> The transformed value
 *
 * @author njoshi
 */
public class GroupToScalar<K, T, R> extends StageConfig<T, R> {

    private final GroupToScalarComputation<K, T, R> computation;
    private final long keyExpireTimeSeconds;

    /**
     * @deprecated As of release 0.603, use {@link #GroupToScalar(GroupToScalarComputation, Config, Codec)} instead
     */
    GroupToScalar(GroupToScalarComputation<K, T, R> computation,
                  Config<K, T, R> config, final io.reactivex.netty.codec.Codec<T> inputCodec) {
        this(computation, config, NettyCodec.fromNetty(inputCodec));
    }

    GroupToScalar(GroupToScalarComputation<K, T, R> computation,
                  Config<K, T, R> config, Codec<T> inputCodec) {
        this(computation, config, (Codec<K>) Codecs.string(), inputCodec);
    }

    GroupToScalar(GroupToScalarComputation<K, T, R> computation,
                  Config<K, T, R> config, Codec<K> inputKeyCodec, Codec<T> inputCodec) {
        super(config.description, inputKeyCodec, inputCodec, config.codec, config.inputStrategy, config.parameters, config.concurrency);
        this.computation = computation;
        this.keyExpireTimeSeconds = config.keyExpireTimeSeconds;
    }

    public GroupToScalarComputation<K, T, R> getComputation() {
        return computation;
    }

    public long getKeyExpireTimeSeconds() {
        return keyExpireTimeSeconds;
    }

    public static class Config<K, T, R> {

        private Codec<R> codec;
        private String description;
        private long keyExpireTimeSeconds = TimeUnit.HOURS.toSeconds(1); // 1 hour default
        // default input type is serial for
        // 'stateful group calculation' use case
        // do not allow config override
        private INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.SERIAL;
        private int concurrency = DEFAULT_STAGE_CONCURRENCY;
        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec is netty reactivex Codec
         *
         * @return Config
         *
         * @deprecated As of release 0.603, use {@link #codec(io.mantisrx.common.codec.Codec)} instead
         */
        public Config<K, T, R> codec(final io.reactivex.netty.codec.Codec<R> codec) {
            this.codec = NettyCodec.fromNetty(codec);
            return this;
        }

        public Config<K, T, R> codec(Codec<R> codec) {
            this.codec = codec;
            return this;
        }

        /**
         * Not used. As we are not generating GroupedObservables
         *
         * @param seconds is a long
         *
         * @return Config
         */
        public Config<K, T, R> keyExpireTimeSeconds(long seconds) {
            this.keyExpireTimeSeconds = seconds;
            return this;
        }

        public Config<K, T, R> description(String description) {
            this.description = description;
            return this;
        }

        public Config<K, T, R> concurrentInput() {
            this.inputStrategy = INPUT_STRATEGY.CONCURRENT;
            return this;
        }

        public Config<K, T, R> concurrentInput(final int concurrency) {
            this.inputStrategy = INPUT_STRATEGY.CONCURRENT;
			this.concurrency = concurrency;
            return this;
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

        public int getConcurrency() { return concurrency; }

        public Config<K, T, R> withParameters(List<ParameterDefinition<?>> params) {
            this.parameters = params;
            return this;
        }

    }

}
