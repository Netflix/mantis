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
 * Functional interface for reducing a collection of input values of type
 * {@code IN} to a single output value of type {@code OUT}.
 */
public interface ReduceFunction<IN, OUT> extends MantisFunction {
    /**
     * Returns the initial value for the reduce operation.
     * @return the initial value
     */
    OUT initialValue();

    /**
     *
     * Reduces the given input value using the accumulator.
     * @param acc the accumulator
     * @param in the input value to reduce
     * @return the reduced output value
     */
    OUT reduce(OUT acc, IN in);

}
