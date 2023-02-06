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

package io.mantisrx.runtime.core;

import io.mantisrx.runtime.Config;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.KeyByFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.sinks.SinkFunction;
import io.mantisrx.runtime.core.sources.SourceFunction;

public interface MantisStream<T> {

    static <OUT> MantisStream<OUT> create(Context context) {
        return MantisStreamImpl.init();
    }

    <OUT> MantisStream<OUT> source(SourceFunction<OUT> sourceFunction);

    Config<T> sink(SinkFunction<T> sinkFunction);

    MantisStream<T> filter(FilterFunction<T> filterFn);
    <OUT> MantisStream<OUT> map(MapFunction<T, OUT> mapFn);
    <OUT> MantisStream<OUT> flatMap(FlatMapFunction<T, OUT> flatMapFn);

    MantisStream<T> materialize();

    <K> KeyedMantisStream<K, T> keyBy(KeyByFunction<K, T> keyFn);

}
