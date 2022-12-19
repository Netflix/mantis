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

import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.KeyByFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.ReduceFunction;
import io.mantisrx.runtime.core.sinks.SinkFunction;
import io.mantisrx.runtime.core.sources.SourceFunction;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.time.Duration;
import lombok.Getter;

public interface MantisStream<T> {

    <OUT> MantisStream<OUT> source(SourceFunction<OUT> sourceFunction);

    MantisStream<Void> sink(SinkFunction<T> sinkFunction);

    <OUT> MantisStream<OUT> create();
    MantisStream<T> filter(FilterFunction<T> filterFn);
    <OUT> MantisStream<OUT> map(MapFunction<T, OUT> mapFn);
    <OUT> MantisStream<OUT> flatMap(FlatMapFunction<T, OUT> flatMapFn);

    MantisStream<T> materialize();

    <K> KeyedMantisStream<K, T> keyBy(KeyByFunction<K, T> keyFn);

    MantisStream<T> parameters(ParameterDefinition<?>... params);

    void execute();

    interface KeyedMantisStream<K, IN> {
        <OUT> KeyedMantisStream<K, OUT> map(MapFunction<IN, OUT> mapFn);

        <OUT> KeyedMantisStream<K, OUT> flatMap(FlatMapFunction<IN, OUT> flatMapFn);

        KeyedMantisStream<K, IN> filter(FilterFunction<IN> filterFn);

        KeyedMantisStream<K, IN> window(MantisStream.WindowSpec spec);

        <OUT> MantisStream<OUT> reduce(ReduceFunction<IN, OUT> reduceFn);
    }

    @Getter
    class WindowSpec {
        private final MantisStream.WindowType type;
        private int numElements;
        private int elementOffset;
        private Duration windowLength;
        private Duration windowOffset;

        WindowSpec(MantisStream.WindowType type, Duration windowLength, Duration windowOffset) {
            this.type = type;
            this.windowLength = windowLength;
            this.windowOffset = windowOffset;
        }

        WindowSpec(MantisStream.WindowType type, int numElements, int elementOffset) {
            this.type = type;
            this.numElements = numElements;
            this.elementOffset = elementOffset;
        }

        public static WindowSpec timed(Duration windowLength) {
            return new WindowSpec(MantisStream.WindowType.TUMBLING, windowLength, windowLength);
        }

        public static WindowSpec timed(Duration windowLength, Duration windowOffset) {
            return new WindowSpec(MantisStream.WindowType.SLIDING, windowLength, windowOffset);
        }

        public static WindowSpec count(int numElements) {
            return new WindowSpec(MantisStream.WindowType.ELEMENT, numElements, numElements);
        }

        public static WindowSpec count(int numElements, int elementOffset) {
            return new WindowSpec(MantisStream.WindowType.ELEMENT_SLIDING, numElements, elementOffset);
        }
    }

    enum WindowType {
        TUMBLING,
        SLIDING,
        ELEMENT,
        ELEMENT_SLIDING
    }
}
