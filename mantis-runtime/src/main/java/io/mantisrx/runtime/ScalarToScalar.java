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
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.Collections;
import java.util.List;


public class ScalarToScalar<T, R> extends StageConfig<T, R> {

    private final INPUT_STRATEGY inputStrategy;
    private final ScalarComputation<T, R> computation;
    private List<ParameterDefinition<?>> parameters;

    /**
     * @deprecated As of release 0.603, use {@link #ScalarToScalar(ScalarComputation, Config, Codec)} instead
     */
    ScalarToScalar(ScalarComputation<T, R> computation,
                   Config<T, R> config, final io.reactivex.netty.codec.Codec<T> inputCodec) {
        super(config.description, NettyCodec.fromNetty(inputCodec), config.codec, config.inputStrategy, config.parameters, config.concurrency);
        this.computation = computation;
        this.inputStrategy = config.inputStrategy;
    }


    public ScalarToScalar(ScalarComputation<T, R> computation,
                   Config<T, R> config, Codec<T> inputCodec) {
        super(config.description, inputCodec, config.codec, config.inputStrategy, config.parameters, config.concurrency);
        this.computation = computation;
        this.inputStrategy = config.inputStrategy;
        this.parameters = config.parameters;
    }

    public ScalarComputation<T, R> getComputation() {
        return computation;
    }

    public INPUT_STRATEGY getInputStrategy() {
        return inputStrategy;
    }

    @Override
    public List<ParameterDefinition<?>> getParameters() {
        return parameters;
    }

    public static class Config<T, R> {

        private Codec<R> codec;
        private String description;
        // default input type is serial for 'collecting' use case
        private INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.SERIAL;
        private volatile int concurrency = StageConfig.DEFAULT_STAGE_CONCURRENCY;

        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec is Codec of netty reactivex
         *
         * @return Config
         *
         * @deprecated As of release 0.603, use {@link #codec(Codec)} instead
         */
        public Config<T, R> codec(final io.reactivex.netty.codec.Codec<R> codec) {
            this.codec = NettyCodec.fromNetty(codec);
            return this;
        }

        public Config<T, R> codec(Codec<R> codec) {
            this.codec = codec;
            return this;
        }

        public Config<T, R> serialInput() {
            this.inputStrategy = INPUT_STRATEGY.SERIAL;
            return this;
        }

        public Config<T, R> concurrentInput() {
            this.inputStrategy = INPUT_STRATEGY.CONCURRENT;
            return this;
        }

        public Config<T, R> concurrentInput(final int concurrency) {
            this.inputStrategy = INPUT_STRATEGY.CONCURRENT;
            this.concurrency = concurrency;
            return this;
        }

        public Config<T, R> description(String description) {
            this.description = description;
            return this;
        }

        public Codec<R> getCodec() {
            return codec;
        }

        public Config<T, R> withParameters(List<ParameterDefinition<?>> params) {
            this.parameters = params;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public INPUT_STRATEGY getInputStrategy() {
            return inputStrategy;
        }

        public int getConcurrency() {
            return concurrency;
        }
    }
}
