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

import io.mantisrx.runtime.core.functions.MantisFunction;
import io.mantisrx.shaded.com.google.common.graph.ImmutableValueGraph;
import io.mantisrx.shaded.com.google.common.graph.MutableValueGraph;
import io.mantisrx.shaded.com.google.common.graph.ValueGraphBuilder;
import java.util.Set;

class MantisGraph {
    private final MutableValueGraph<OperandNode<?>, MantisFunction> graph;

    MantisGraph() {
        this(ValueGraphBuilder.directed().allowsSelfLoops(true).build());
    }

    MantisGraph(MutableValueGraph<OperandNode<?>, MantisFunction> graph) {
        this.graph = graph;
    }

    Set<OperandNode<?>> nodes() {
        return this.graph.nodes();
    }

    MantisGraph addNode(OperandNode<?> node) {
        this.graph.addNode(node);
        return this;
    }

    MantisGraph putEdge(OperandNode<?> from, OperandNode<?> to, MantisFunction edge) {
        this.graph.addNode(from);
        this.graph.addNode(to);
        this.graph.putEdgeValue(from, to, edge);
        return this;
    }

    public ImmutableValueGraph<OperandNode<?>, MantisFunction> immutable() {
        return ImmutableValueGraph.copyOf(this.graph);
    }
}
