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

import io.mantisrx.common.MantisGroup;
import io.mantisrx.runtime.Config;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.Computation;
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.FunctionCombinator;
import io.mantisrx.runtime.core.functions.KeyByFunction;
import io.mantisrx.runtime.core.functions.MantisFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.WindowFunction;
import io.mantisrx.runtime.core.sinks.ObservableSinkImpl;
import io.mantisrx.runtime.core.sinks.SinkFunction;
import io.mantisrx.runtime.core.sources.ObservableSourceImpl;
import io.mantisrx.runtime.core.sources.SourceFunction;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.graph.ImmutableValueGraph;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

@Slf4j
public class MantisStreamImpl<T> implements MantisStream<T> {

    final OperandNode<T> currNode;
    final MantisGraph graph;
    final Iterable<ParameterDefinition<?>> params;

    MantisStreamImpl(OperandNode<T> newNode, MantisGraph graph) {
        this(newNode, graph, ImmutableList.of());
    }

    MantisStreamImpl(OperandNode<T> node, MantisGraph graph, Iterable<ParameterDefinition<?>> params) {
        this.currNode = node;
        this.graph = graph;
        this.params = params;
    }

    public static <T> MantisStream<T> init() {
        OperandNode<T> node = new OperandNode<>(0, "init");
        return new MantisStreamImpl<>(node, new MantisGraph().addNode(node));
    }

    public <OUT> MantisStream<OUT> source(SourceFunction<OUT> source) {
        return updateGraph(source);
    }

    @Override
    public Config<T> sink(SinkFunction<T> sink) {
        MantisStreamImpl<Void> mantisStream = updateGraph(sink);
        ImmutableValueGraph<OperandNode<?>, MantisFunction> graphDag = mantisStream.graph.immutable();
        Iterable<OperandNode<?>> operandNodes = topSortTraversal(graphDag);
        MantisJobBuilder jobBuilder = makeMantisJob(graphDag, operandNodes);
        return (Config<T>) jobBuilder.buildJobConfig();
    }

    <OUT> MantisStreamImpl<OUT> updateGraph(MantisFunction mantisFn) {
        OperandNode<OUT> node = OperandNode.create(graph, mantisFn.getClass().getName() + "OUT");
        graph.putEdge(currNode, node, mantisFn);
        return new MantisStreamImpl<>(node, graph);
    }

    @Override
    public MantisStream<T> filter(FilterFunction<T> filterFn) {
        return updateGraph(filterFn);
    }

    @Override
    public <OUT> MantisStream<OUT> map(MapFunction<T, OUT> mapFn) {
        return updateGraph(mapFn);
    }

    @Override
    public <OUT> MantisStream<OUT> flatMap(FlatMapFunction<T, OUT> flatMapFn) {
        return updateGraph(flatMapFn);
    }

    @Override
    public MantisStream<T> materialize() {
        return materializeInternal();
    }

    private MantisStreamImpl<T> materializeInternal() {
        graph.putEdge(currNode, currNode, MantisFunction.empty());
        return new MantisStreamImpl<>(currNode, graph);
    }

    private <K> KeyedMantisStreamImpl<K, T> keyByInternal(KeyByFunction<K, T> keyFn) {
        OperandNode<T> node = OperandNode.create(graph, "keyByOut");
        graph.putEdge(currNode, node, keyFn);
        return new KeyedMantisStreamImpl<>(node, graph);
    }

    @Override
    public <K> KeyedMantisStream<K, T> keyBy(KeyByFunction<K, T> keyFn) {
        return this.materializeInternal()
            .keyByInternal(keyFn);
    }

    private MantisJobBuilder makeMantisJob(ImmutableValueGraph<OperandNode<?>, MantisFunction> graphDag, Iterable<OperandNode<?>> operandNodes) {
        MantisJobBuilder jobBuilder = new MantisJobBuilder();
        final AtomicReference<FunctionCombinator<?, ?>> composite = new AtomicReference<>(new FunctionCombinator<>(false));
        for (OperandNode<?> n : operandNodes) {
            Set<OperandNode<?>> successorsNodes = graphDag.successors(n);
            if (successorsNodes.size() == 0) {
                continue;
            }
            // remove self-loop
            Optional<MantisFunction> selfEdge = graphDag.edgeValue(n, n);
            Integer numSelfEdges = selfEdge.map(x -> 1).orElse(0);
            selfEdge.ifPresent(mantisFn -> {
                if (MantisFunction.empty().equals(mantisFn)) {
                    jobBuilder.addStage(composite.get().makeStage(), n.getCodec(), n.getKeyCodec());
                    composite.set(new FunctionCombinator<>(false));
                } else if (mantisFn instanceof WindowFunction) {
                    composite.set(composite.get().add(mantisFn));
                }
                // No other types of self-edges are possible
            });
            if (successorsNodes.size() - numSelfEdges > 1) {
                log.warn("Found multi-output node {} with nbrs {}. Not supported yet!", n, successorsNodes);
            }
            for (OperandNode<?> successorsNode : successorsNodes) {
                if (successorsNode == n) {
                    continue;
                }
                graphDag.edgeValue(n, successorsNode).ifPresent(mantisFn -> {
                    if (mantisFn instanceof SourceFunction) {
                        if (mantisFn instanceof ObservableSourceImpl) {
                            jobBuilder.addStage(((ObservableSourceImpl) mantisFn).getSource());
                        }
                    } else if (mantisFn instanceof KeyByFunction) {
                        // ensure that the existing composite is empty here
                        if (composite.get().size() > 0) {
                            log.warn("Unempty composite found for KeyByFunction {}", composite.get());
                        }
                        jobBuilder.addStage(makeGroupComputation((KeyByFunction) mantisFn), n.getCodec(), n.getKeyCodec());
                        composite.set(new FunctionCombinator<>(true));
                    } else if (mantisFn instanceof SinkFunction) {
                        // materialize all composite functions into a scalar stage
                        // it can't be a keyed stage because we didn't encounter a reduce function!
                        jobBuilder.addStage(composite.get().makeStage(), n.getCodec());
                        if (mantisFn instanceof ObservableSinkImpl) {
                            jobBuilder.addStage(((ObservableSinkImpl) mantisFn).getSink());
                        }
                    } else {
                        composite.set(composite.get().add(mantisFn));
                    }
                });
            }
        }
        return jobBuilder;
    }

    private <A, K> Computation makeGroupComputation(KeyByFunction<K, A> keyFn) {
        return new ToGroupComputation<A, K, A>() {
            @Override
            public void init(Context ctx) {
                keyFn.init();
            }

            @Override
            public Observable<MantisGroup<K, A>> call(Context ctx, Observable<A> obs) {
                return obs.map(e -> new MantisGroup<>(keyFn.getKey(e), e));
            }
        };
    }

    static <V, E> Iterable<V> topSortTraversal(ImmutableValueGraph<V, E> graphDag) {
        Set<V> nodes = graphDag.nodes();
        Map<V, AtomicInteger> inDegreeMap = nodes.stream()
            .collect(Collectors.toMap(x -> x, x ->
                new AtomicInteger(graphDag.inDegree(x) - (graphDag.hasEdgeConnecting(x, x) ? 1 : 0))));
        List<V> nodeOrder = new ArrayList<>();
        final Set<V> visited = new HashSet<>();
        List<V> starts = inDegreeMap.keySet().stream()
            .filter(x -> inDegreeMap.get(x).get() == 0)
            .collect(Collectors.toList());
        while (!starts.isEmpty()) {
            starts.forEach(x -> graphDag.successors(x).forEach(nbr -> {
                if (nbr != x) inDegreeMap.get(nbr).decrementAndGet();
            }));
            visited.addAll(starts);
            nodeOrder.addAll(starts);
            starts = starts.stream().flatMap(x -> graphDag.successors(x).stream())
                .filter(x -> !visited.contains(x) && inDegreeMap.get(x).get() == 0)
                .collect(Collectors.toList());
        }
        return nodeOrder;
    }
}
