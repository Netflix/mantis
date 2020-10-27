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

import io.mantisrx.common.codec.Codec;
import io.mantisrx.runtime.parameter.ParameterDefinition;


public abstract class StageConfig<T, R> {

    // Note: the default of -1 implies the concurrency is not explicitly configured. This defaults to
    // the behaviour of relying on the number of inner observables for concurrency in the system
    public static final int DEFAULT_STAGE_CONCURRENCY = -1;
    private String description;
    private Codec<T> inputCodec;
    private Codec<R> outputCodec;
    private INPUT_STRATEGY inputStrategy;
    private List<ParameterDefinition<?>> parameters;

    // this number determines the number of New Threads created for concurrent Stage processing irrespective of the
    // number of inner observables processed
    private int concurrency = DEFAULT_STAGE_CONCURRENCY;

    public StageConfig(String description, Codec<T> inputCodec,
                       Codec<R> outputCodec, INPUT_STRATEGY inputStrategy) {
        this(description, inputCodec, outputCodec, inputStrategy, Collections.emptyList(), DEFAULT_STAGE_CONCURRENCY);
    }

    public StageConfig(String description, Codec<T> inputCodec,
                       Codec<R> outputCodec, INPUT_STRATEGY inputStrategy, List<ParameterDefinition<?>> params) {
        this(description, inputCodec, outputCodec, inputStrategy, params, DEFAULT_STAGE_CONCURRENCY);
    }

    public StageConfig(String description, Codec<T> inputCodec,
                       Codec<R> outputCodec, INPUT_STRATEGY inputStrategy, int concurrency) {
        this(description, inputCodec, outputCodec, inputStrategy, Collections.emptyList(), concurrency);
    }

    public StageConfig(String description, Codec<T> inputCodec,
                       Codec<R> outputCodec, INPUT_STRATEGY inputStrategy, List<ParameterDefinition<?>> params,
                       int concurrency) {
        this.description = description;
        this.inputCodec = inputCodec;
        this.outputCodec = outputCodec;
        this.inputStrategy = inputStrategy;
        this.parameters = params;
        this.concurrency = concurrency;
    }

    public String getDescription() {
        return description;
    }

    public Codec<T> getInputCodec() {
        return inputCodec;
    }

    public Codec<R> getOutputCodec() {
        return outputCodec;
    }

    public INPUT_STRATEGY getInputStrategy() {
        return inputStrategy;
    }

    public List<ParameterDefinition<?>> getParameters() {
        return parameters;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public enum INPUT_STRATEGY {NONE_SPECIFIED, SERIAL, CONCURRENT}
}
