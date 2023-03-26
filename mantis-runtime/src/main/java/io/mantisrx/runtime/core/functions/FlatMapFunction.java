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

package io.mantisrx.runtime.core.functions;


/**
 * Functional interface for mapping an input value of type {@code IN}
 * to an iterable of output values of type {@code OUT}.
 * There could be zero, one, or more output elements.
 * {@code FunctionalInterface} allows java-8 lambda
 */
@FunctionalInterface
public interface FlatMapFunction<IN, OUT> extends MantisFunction {

    /**
     * Applies the flat map function to the given input value.
     @param in the input value
     @return an iterable of output values
     */
    Iterable<OUT> apply(IN in);
}
