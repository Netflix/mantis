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
import io.mantisrx.runtime.core.functions.MantisFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.ReduceFunction;
import io.mantisrx.runtime.core.functions.WindowFunction;

class KeyedMantisStreamImpl<K, T> implements KeyedMantisStream<K, T> {
    final OperandNode<T> currNode;
    final MantisGraph graph;

    public KeyedMantisStreamImpl(OperandNode<T> node, MantisGraph graph) {
        this.currNode = node;
        this.graph = graph;
    }

    <OUT> KeyedMantisStream<K, OUT> updateGraph(MantisFunction mantisFn) {
        OperandNode<OUT> node = OperandNode.create(graph, mantisFn.getClass().getName() + "OUT");
        graph.putEdge(currNode, node, mantisFn);
        return new KeyedMantisStreamImpl<>(node, graph);
    }

    @Override
    public <OUT> KeyedMantisStream<K, OUT> map(MapFunction<T, OUT> mapFn) {
        //todo(hmittal): figure out a way to make the key available inside these functions
        return updateGraph(mapFn);
    }

    @Override
    public <OUT> KeyedMantisStream<K, OUT> flatMap(FlatMapFunction<T, OUT> flatMapFn) {
        return updateGraph(flatMapFn);
    }

    @Override
    public KeyedMantisStream<K, T> filter(FilterFunction<T> filterFn) {
        return updateGraph(filterFn);
    }

    public KeyedMantisStream<K, T> window(WindowSpec spec) {
        this.graph.putEdge(currNode, currNode, new WindowFunction<>(spec));
        return new KeyedMantisStreamImpl<>(currNode, graph);
    }

    @Override
    public <OUT> MantisStream<OUT> reduce(ReduceFunction<T, OUT> reduceFn) {
        OperandNode<OUT> node = OperandNode.create(graph, "reduceFunctionOut");
        this.graph.putEdge(currNode, node, reduceFn);
        this.graph.putEdge(node, node, MantisFunction.empty());
        return new MantisStreamImpl<>(node, graph);
    }
}
