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
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.CompositeScalarComputation;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.core.functions.FilterFunction;
import io.mantisrx.runtime.core.functions.FlatMapFunction;
import io.mantisrx.runtime.core.functions.KeyByFunction;
import io.mantisrx.runtime.core.functions.MantisFunction;
import io.mantisrx.runtime.core.functions.MapFunction;
import io.mantisrx.runtime.core.functions.ReduceFunction;
import io.mantisrx.runtime.core.sources.SourceFunction;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.shaded.guava30.com.google.common.graph.MutableValueGraph;
import org.apache.flink.shaded.guava30.com.google.common.graph.ValueGraphBuilder;
import rx.Observable;

public class MantisStreamImpl<T> implements MantisStream<T> {

    final OperandNode<T> currNode;
    final MantisGraph graph;
    private MantisJob<?> mantisJob = new MantisJob<>();
    private CompositeScalarComputation<T, ?> scalarComputations;
    private ScalarToScalar<?, ?> stage;
    private SourceFunction<T> source;

    private <K> MantisStreamImpl(Object source, ScalarToGroup<T, K, T> stage, ScalarToGroup<T, K, T> stage1) {
    }

    public MantisStreamImpl(OperandNode<T> newNode, MantisGraph graph) {
        this.currNode = newNode;
        this.graph = graph;
    }

    public MantisStreamImpl(SourceFunction<T> source, ScalarToScalar<?, ?> stage, ScalarToScalar<?, ?> stage1) {

    }

    public static <T> MantisStream<T> init() {
        OperandNode<T> node = new OperandNode<>(0);
        return new MantisStreamImpl<>(node, new MantisGraph().addNode(node));
    }

    @Override
    public <OUT> MantisStream<OUT> create() {
        return MantisStreamImpl.init();
    }

    public MantisStream<T> source(SourceFunction<T> source) {
        // Preconditions.checkState(currNode == null, "Supports only 1 source which must be the first call");
        return updateGraph(source);

    }

    <OUT> MantisStream<OUT> updateGraph(MantisFunction<?, OUT> source) {
        OperandNode<OUT> node = OperandNode.create(graph);
        graph.putEdge(currNode, node, source);
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
        /*return new MantisStreamImpl<>(node, graph);
        getScalarComputations()
            .add(new ScalarComputation<T, OUT>() {
                @Override
                public void init(Context context) {
                    flatMapFn.init();
                }

                @Override
                public Observable<OUT> call(Context context, Observable<T> tObservable) {
                    final Collector<OUT> collector = new Collector<>();
                    tObservable.doOnNext(x -> flatMapFn.apply(x, collector)).subscribe();
                    return Observable.from(collector.iterable()).map(x -> x.orElse(null));
                }
            });
        return (MantisStream<OUT>) this;
         */
    }

    private CompositeScalarComputation<?, ?> getScalarComputations() {
        if (scalarComputations == null) {
            scalarComputations = new CompositeScalarComputation<>();
        }
        return scalarComputations;
    }

    @Override
    public MantisStream<T> materialize() {
        return materializeInternal();
    }

    private MantisStreamImpl<T> materializeInternal() {
        graph.putEdge(currNode, currNode, MantisFunction.empty());
        return new MantisStreamImpl<>(currNode, graph);
        /*ScalarComputation<?, ?> scalars = getScalarComputations();
        if (scalars != null) {
            this.stage = new ScalarToScalar<>(scalarComputations, new ScalarToScalar.Config<>(), Codecs.javaSerializer());
        }
        return this;
         */
    }

    private <K> KeyedMantisStreamImpl<K, T> keyByInternal(KeyByFunction<K, T> keyFn) {
        OperandNode<T> node = OperandNode.create(graph);
        graph.putEdge(currNode, node, keyFn);
        return new KeyedMantisStreamImpl<K, T>(node, graph);
    }

    @Override
    public <K> KeyedMantisStream<K, T> keyBy(KeyByFunction<K, T> keyFn) {
        return this.materializeInternal()
            .keyByInternal(keyFn);

        /*
        MantisStream<T> stream = materialize();

        ToGroupComputation<T, K, T> groupFn = new ToGroupComputation<T, K, T>() {
            @Override
            public void init(Context context) {
                keyFn.init();
            }

            @Override
            public Observable<MantisGroup<K, T>> call(Context ctx, Observable<T> rx) {
                return rx.map(i -> new MantisGroup<>(keyFn.getKey(i), i));
            }
        };
        ScalarToGroup.Config<T, K, T> stogConfig = new ScalarToGroup.Config<T, K, T>()
            .codec(Codecs.javaSerializer())
            .keyCodec(Codecs.javaSerializer());
        ScalarToGroup<T, K, T> stage = new ScalarToGroup<>(groupFn, stogConfig, Codecs.javaSerializer());
        return new KeyedMantisStreamImpl<>(source, stream, stage);
         */
    }

    public void execute() {
        // parse the graph
        // and build MantisJob
        // using a top-sort so that this part
        // won't change
        // return an instance of mantis job (or break it into two functions)

    }

    private static class KeyedMantisStreamImpl<K, T> implements KeyedMantisStream<K, T> {
        final OperandNode<T> currNode;
        final MantisGraph graph;

        public KeyedMantisStreamImpl(OperandNode<T> node, MantisGraph graph) {
            this.currNode = node;
            this.graph = graph;
        }

        <OUT> KeyedMantisStream<K, OUT> updateGraph(MantisFunction<?, OUT> source) {
            OperandNode<OUT> node = OperandNode.create(graph);
            graph.putEdge(currNode, node, source);
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
            this.graph.putEdge(currNode, currNode, new MantisFunction<Void, Void>() {
                public final WindowSpec windowSpec = spec;
            });
            return new KeyedMantisStreamImpl<>(currNode, graph);
        }

        @Override
        public <OUT> MantisStream<OUT> reduce(ReduceFunction<K, T, OUT> reduceFn) {
            OperandNode<OUT> node = OperandNode.create(graph);
            this.graph.putEdge(currNode, node, reduceFn);
            return new MantisStreamImpl<>(node, graph);
            /*
            MantisStreamImpl<OUT> outStream = (MantisStreamImpl<OUT>) new MantisStreamImpl<>(source, stage, stage);
            CompositeScalarComputation<?, ?> scalar = outStream.getScalarComputations();

            scalar.add(new GroupToScalarComputation<K, T, OUT>() {
                @Override
                public Observable<OUT> call(Context context, Observable<MantisGroup<K, T>> gobs) {
                    return gobs.groupBy(MantisGroup::getKeyValue)
                        .flatMap(go -> {
                            final Observable<Observable<MantisGroup<K, T>>> windowed;
                            if (spec == null) {
                                windowed = go.window(Integer.MAX_VALUE);
                            } else if (spec.type == WindowType.ELEMENT || spec.type == WindowType.ELEMENT_SLIDING) {
                                windowed = go.window(spec.numElements, spec.elementOffset);
                            } else {
                                windowed = go.window(spec.windowLength.toMillis(), spec.windowOffset.toMillis(), TimeUnit.MILLISECONDS);
                            }
                            return windowed.map(x -> x
                                .collect(() -> new ConcurrentLinkedQueue<T>(), (q, i) -> q.add(i.getValue()))
                                .map(y -> reduceFn.apply(go.getKey(), y)))
                                .flatMap(x -> x);
                            }
                        );
                }

                @Override
                public void init(Context context) {
                    reduceFn.init();
                }

            });
            return outStream;
             */
        }
    }

    public enum WindowType {
        TUMBLING,
        SLIDING,
        ELEMENT,
        ELEMENT_SLIDING
    }

    private static class WindowSpec {
        private final WindowType type;
        private int numElements;
        private int elementOffset;
        private Duration windowLength;
        private Duration windowOffset;

        public WindowSpec(WindowType type, Duration windowLength, Duration windowOffset) {
            this.type = type;
            this.windowLength = windowLength;
            this.windowOffset = windowOffset;
        }

        public WindowSpec(WindowType type, int numElements, int elementOffset) {
            this.type = type;
            this.numElements = numElements;
            this.elementOffset = elementOffset;
        }

        static WindowSpec timeTumbling(Duration windowLength) {
            return new WindowSpec(WindowType.TUMBLING, windowLength, windowLength);
        }

        static WindowSpec timeSliding(Duration windowLength, Duration windowOffset) {
            return new WindowSpec(WindowType.SLIDING, windowLength, windowOffset);
        }

        static WindowSpec countTumbling(int numElements) {
            return new WindowSpec(WindowType.ELEMENT, numElements, numElements);

        }
        static WindowSpec countSliding(int numElements, int elementOffset) {
            return new WindowSpec(WindowType.ELEMENT_SLIDING, numElements, elementOffset);
        }
    }

    private static class OperandNode<T> {
        private final int nodeIdx;
        private final MantisFunction<?, T> nodeFunction;

        public OperandNode(int i) {
            this.nodeIdx = i;
            this.nodeFunction = null;
        }

        public OperandNode(int i, MantisFunction<?, T> source) {
            this.nodeIdx = i;
            this.nodeFunction = source;
        }

        public static <T> OperandNode<T> create(MantisGraph graph) {
            return new OperandNode<>(graph.nodes().size() + 1);
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
    }
}
