/*
 * Copyright 2023 Netflix, Inc.
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
package io.mantisrx.runtime.core;

import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.ReduceFunction;

public interface KeyedMantisStream<K, IN> {
    <OUT> KeyedMantisStream<K, OUT> map(MapFunction<IN, OUT> mapFn);

    <OUT> KeyedMantisStream<K, OUT> flatMap(FlatMapFunction<IN, OUT> flatMapFn);

    KeyedMantisStream<K, IN> filter(FilterFunction<IN> filterFn);

    KeyedMantisStream<K, IN> window(WindowSpec spec);

    <OUT> MantisStream<OUT> reduce(ReduceFunction<IN, OUT> reduceFn);
}
