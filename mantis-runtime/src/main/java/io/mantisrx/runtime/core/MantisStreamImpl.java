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
import io.mantisrx.common.codec.Codec;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.computation.Computation;
import io.mantisrx.runtime.computation.ToGroupComputation;
import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.FunctionCombinator;
import io.mantisrx.runtime.core.functions.KeyByFunction;
import io.mantisrx.runtime.core.functions.MantisFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.ReduceFunction;
import io.mantisrx.runtime.core.functions.WindowFunction;
import io.mantisrx.runtime.core.sinks.ObservableSinkImpl;
import io.mantisrx.runtime.core.sinks.SinkFunction;
import io.mantisrx.runtime.core.sources.ObservableSourceImpl;
import io.mantisrx.runtime.core.sources.SourceFunction;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava30.com.google.common.graph.ImmutableValueGraph;
import org.apache.flink.shaded.guava30.com.google.common.graph.MutableValueGraph;
import org.apache.flink.shaded.guava30.com.google.common.graph.ValueGraphBuilder;
import rx.Observable;

@Slf4j
public class MantisStreamImpl<T> implements MantisStream<T> {

    final OperandNode<T> currNode;
    final MantisGraph graph;
    final Iterable<ParameterDefinition<?>> params;

    public MantisStreamImpl(OperandNode<T> newNode, MantisGraph graph) {
        this(newNode, graph, ImmutableList.of());
    }

    public MantisStreamImpl(OperandNode<T> node, MantisGraph graph, Iterable<ParameterDefinition<?>> params) {
        this.currNode = node;
        this.graph = graph;
        this.params = params;
    }

    public static <T> MantisStream<T> init() {
        OperandNode<T> node = new OperandNode<>(0, "init");
        return new MantisStreamImpl<>(node, new MantisGraph().addNode(node));
    }

    @Override
    public <OUT> MantisStream<OUT> create() {
        return MantisStreamImpl.init();
    }

    public <OUT> MantisStream<OUT> source(SourceFunction<OUT> source) {
        // Preconditions.checkState(currNode == null, "Supports only 1 source which must be the first call");
        return updateGraph(source);

    }

    @Override
    public MantisStream<Void> sink(SinkFunction<T> sink) {
        return updateGraph(sink);
    }

    <OUT> MantisStream<OUT> updateGraph(MantisFunction<?, OUT> mantisFn) {
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

    @Override
    public MantisStream<T> parameters(ParameterDefinition<?>... params) {
        return new MantisStreamImpl<>(currNode, graph, Arrays.stream(params).collect(Collectors.toList()));
    }

    @Override
    public void execute() {
        // traverse the graph and build MantisJob
        // graph traversal using top-sort
        // return an instance of mantis job (or break it into two functions)
        ImmutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> graphDag = this.graph.immutable();
        Iterable<OperandNode<?>> operandNodes = topSortTraversal(graphDag);
        Job job = makeMantisJob(graphDag, operandNodes);
        LocalJobExecutorNetworked.execute(job);
    }

    private Job makeMantisJob(ImmutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> graphDag, Iterable<OperandNode<?>> operandNodes) {
        MantisJobBuilder jobBuilder = new MantisJobBuilder();
        final AtomicReference<FunctionCombinator<?, ?>> composite = new AtomicReference<>(new FunctionCombinator<>(false));
        for (OperandNode<?> n : operandNodes) {
            Set<OperandNode<?>> nbrs = graphDag.successors(n);
            if (nbrs.size() == 0) {
                continue;
            }
            // remove self-loop
            Optional<MantisFunction<?, ?>> selfEdge = graphDag.edgeValue(n, n);
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
            if (nbrs.size() - numSelfEdges > 1) {
                log.warn("Found multi-output node {} with nbrs {}. Not supported yet!", n, nbrs);
            }
            for (OperandNode<?> nbr : nbrs) {
                if (nbr == n) {
                    continue;
                }
                graphDag.edgeValue(n, nbr).ifPresent(mantisFn -> {
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
        jobBuilder.addParameters(params);
        return jobBuilder.buildJob();
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

    private Iterable<OperandNode<?>> topSortTraversal(ImmutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> graphDag) {
        Set<OperandNode<?>> nodes = graphDag.nodes();
        Map<OperandNode<?>, AtomicInteger> inDegreeMap = nodes.stream().collect(Collectors.toMap(x -> x, x -> new AtomicInteger(graphDag.inDegree(x) - (graphDag.hasEdgeConnecting(x, x) ? 1 : 0))));
        List<OperandNode<?>> nodeOrder = new ArrayList<>();
        final Set<OperandNode<?>> visited = new HashSet<>();
        while (true) {
            Set<OperandNode<?>> starts = inDegreeMap.keySet().stream()
                .filter(x -> !visited.contains(x) && inDegreeMap.get(x).get() == 0)
                .collect(Collectors.toSet());

            starts.forEach(x -> graphDag.successors(x).forEach(nbr -> inDegreeMap.get(nbr).decrementAndGet()));
            if (starts.isEmpty()) {
                break;
            }
            visited.addAll(starts);
            nodeOrder.addAll(starts);
        }
        return nodeOrder;
    }

    private static class KeyedMantisStreamImpl<K, T> implements KeyedMantisStream<K, T> {
        final OperandNode<T> currNode;
        final MantisGraph graph;

        public KeyedMantisStreamImpl(OperandNode<T> node, MantisGraph graph) {
            this.currNode = node;
            this.graph = graph;
        }

        <OUT> KeyedMantisStream<K, OUT> updateGraph(MantisFunction<?, OUT> mantisFn) {
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

    @Getter
    private static class OperandNode<T> {
        private final int nodeIdx;
        private final String description;
        private final Codec<T> codec;

        public OperandNode(int i, String description) {
            this.nodeIdx = i;
            this.description = description;
            this.codec = Codecs.javaSerializer();
        }

        public static <T> OperandNode<T> create(MantisGraph graph, String description) {
            return new OperandNode<>(graph.nodes().size(), description);
        }

        @Override
        public String toString() {
            return String.format("%d (%s)", nodeIdx, description);
        }

        public <K> Codec<K> getKeyCodec() {
            return Codecs.javaSerializer();
        }
    }

    private static class MantisGraph {
        private MutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> graph;

        MantisGraph() {
            this(ValueGraphBuilder.directed().allowsSelfLoops(true).build());
        }

        MantisGraph(MutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> graph) {
            this.graph = graph;
        }

        Set<OperandNode<?>> nodes() {
            return this.graph.nodes();
        }

        private MantisGraph addNode(OperandNode<?> node) {
            this.graph.addNode(node);
            return this;
        }

        private MantisGraph putEdge(OperandNode<?> from, OperandNode<?> to, MantisFunction<?, ?> edge) {
            this.graph.addNode(from);
            this.graph.addNode(to);
            this.graph.putEdgeValue(from, to, edge);
            return this;
        }

        public ImmutableValueGraph<OperandNode<?>, MantisFunction<?, ?>> immutable() {
            return ImmutableValueGraph.copyOf(this.graph);
        }
    }
}
