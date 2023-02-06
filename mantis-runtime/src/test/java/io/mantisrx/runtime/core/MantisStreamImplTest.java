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

import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.graph.ImmutableValueGraph;
import io.mantisrx.shaded.com.google.common.graph.ValueGraphBuilder;
import java.util.Set;
import junit.framework.TestCase;
import org.junit.Test;

public class MantisStreamImplTest extends TestCase {

    @Test
    public void testGraphApi() {
        ImmutableValueGraph.Builder<String, Integer> graphBuilder = ValueGraphBuilder.directed().allowsSelfLoops(true).immutable();

        graphBuilder.putEdgeValue("a", "b", 10);
        graphBuilder.putEdgeValue("a", "a", 10);
        graphBuilder.putEdgeValue("b", "c", 20);
        graphBuilder.putEdgeValue("a", "d", 10);
        graphBuilder.putEdgeValue("d", "b", 10);

        ImmutableValueGraph<String, Integer> graph = graphBuilder.build();
        Set<String> anbrs = graph.successors("a");
        for (String a : graph.nodes()) {
            System.out.printf("node %s, adjnodes %s, nbrs %s, edges %s\n", a, graph.adjacentNodes(a), graph.successors(a), graph.incidentEdges(a));
            graph.successors(a).forEach(nbr -> System.out.printf("edge for %s -> %s ::: %s\n", a, nbr, graph.edgeValue(a, nbr)));
        }
        System.out.println("done!");
    }

    @Test
    public void testTopSort() {
        /**
         * For the following graph,
         *            c
         *            ^
         * a^ -> d -> b
         * |          ^
         * + -------- |
         */
        ImmutableValueGraph.Builder<String, Integer> graphBuilder = ValueGraphBuilder.directed().allowsSelfLoops(true).immutable();
        graphBuilder.putEdgeValue("a", "b", 10);
        graphBuilder.putEdgeValue("a", "a", 10);
        graphBuilder.putEdgeValue("a", "d", 10);
        graphBuilder.putEdgeValue("b", "c", 20);
        graphBuilder.putEdgeValue("d", "b", 10);

        ImmutableValueGraph<String, Integer> graph = graphBuilder.build();
        Iterable<String> nodes = MantisStreamImpl.topSortTraversal(graph);
        assertEquals(ImmutableList.of("a", "d", "b", "c"), nodes);
    }

}
