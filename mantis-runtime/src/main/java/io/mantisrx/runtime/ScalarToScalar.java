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

import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.reactivex.netty.codec.Codec;


public class ScalarToScalar<T, R> extends StageConfig<T, R> {

    private INPUT_STRATEGY inputStrategy;
    private ScalarComputation<T, R> computation;
    private List<ParameterDefinition<?>> parameters;

    /**
     * @deprecated As of release 0.603, use {@link #ScalarToScalar(ScalarComputation, Config, io.mantisrx.common.codec.Codec)} instead
     */
    ScalarToScalar(ScalarComputation<T, R> computation,
                   Config<T, R> config, final Codec<T> inputCodec) {
        super(config.description, new io.mantisrx.common.codec.Codec<T>() {
            @Override
            public T decode(byte[] bytes) {
                return inputCodec.decode(bytes);
            }

            @Override
            public byte[] encode(T value) {
                return inputCodec.encode(value);
            }
        }, config.codec, config.inputStrategy, config.parameters, config.concurrency);
        this.computation = computation;
        this.inputStrategy = config.inputStrategy;
    }


    ScalarToScalar(ScalarComputation<T, R> computation,
                   Config<T, R> config, io.mantisrx.common.codec.Codec<T> inputCodec) {
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

        private io.mantisrx.common.codec.Codec<R> codec;
        private String description;
        // default input type is serial for 'collecting' use case
        private INPUT_STRATEGY inputStrategy = INPUT_STRATEGY.SERIAL;
        private volatile int concurrency = StageConfig.DEFAULT_STAGE_CONCURRENCY;

        private List<ParameterDefinition<?>> parameters = Collections.emptyList();

        /**
         * @param codec
         *
         * @return
         *
         * @deprecated As of release 0.603, use {@link #codec(io.mantisrx.common.codec.Codec)} instead
         */
        public Config<T, R> codec(final Codec<R> codec) {
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

        public Config<T, R> codec(io.mantisrx.common.codec.Codec<R> codec) {
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

        public io.mantisrx.common.codec.Codec<R> getCodec() {
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
