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
 * Functional interface for reducing a collection of input values of type {@code IN}
 * to a single output value of type {@code IN}.
 *
 * This implementation extends the {@link ReduceFunction} interface and provides a
 * single method for reducing the given input value using the accumulator.
 */
@FunctionalInterface
public interface SimpleReduceFunction<IN> extends ReduceFunction<IN, IN> {
    Object EMPTY = new Object();

    /**
     * Applies the reduce operation to the given accumulator and input value.
     *
     * If no values are to be returned, returns {@link SimpleReduceFunction#EMPTY}
     * instead. This means {@link SimpleReduceFunction#apply(IN, IN)} isn't
     * called for the first item which is returned as-is. For subsequent items,
     * apply is called normally.
     *
     * @param acc current accumulator value
     * @param item the input value to reduce
     * @return the reduced output value
     */
    IN apply(IN acc, IN item);

    @Override
    default IN initialValue() {
        return (IN) EMPTY;
    }

    @Override
    default IN reduce(IN acc, IN item) {
        if (acc == EMPTY) {
            return item;
        } else {
            return apply(acc, item);
        }
    }
}
