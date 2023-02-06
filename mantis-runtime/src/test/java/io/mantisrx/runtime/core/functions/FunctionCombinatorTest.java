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

import static org.junit.Assert.assertEquals;

import io.mantisrx.common.MantisGroup;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.core.WindowSpec;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import rx.Observable;

public class FunctionCombinatorTest {

    @Test
    public void testSimple() {
        FunctionCombinator<Integer, Integer> fns = new FunctionCombinator<Integer, Void>(false)
            .add((MapFunction<Integer, Long>) e -> e + 1L)
            .add((FilterFunction<Long>) e -> e % 2 == 0)
            .add((MapFunction<Long, Long>) e -> e * e)
            .add((MapFunction<Long, String>) e -> e + "1")
            .add((MapFunction<String, Integer>) Integer::parseInt);

        ScalarComputation<Integer, Integer> scalar = fns.makeScalarStage();
        ImmutableList<Integer> items = ImmutableList.of(0, 1, 2, 3, 4, 5, 6);
        Observable<Integer> result = scalar.call(new Context(), Observable.from(items));
        List<Integer> collect = new ArrayList<>();
        result.forEach(collect::add);
        assertEquals(ImmutableList.of(41, 161, 361), collect);
    }

    @Test
    public void testKeyedFunction() {
        FunctionCombinator<Integer, Integer> fns = new FunctionCombinator<Integer, Void>(true)
            .add((MapFunction<Integer, Long>) e -> e + 1L)
            .add((FilterFunction<Long>) e -> e % 2 == 0)
            .add((MapFunction<Long, Long>) e -> e * e)
            .add((MapFunction<Long, String>) e -> e + "1")
            .add((MapFunction<String, Integer>) Integer::parseInt);

        GroupToScalarComputation<String, Integer, Integer> scalar = fns.makeGroupToScalarStage();
        List<Integer> elems = ImmutableList.of(0, 1, 2, 3, 4, 5, 6);
        List<MantisGroup<String, Integer>> build = ImmutableList.<MantisGroup<String, Integer>>builder()
            .addAll(elems.stream().map(x -> new MantisGroup<>("k1", x)).collect(Collectors.toList()))
            .addAll(elems.stream().map(x -> new MantisGroup<>("k2", x + 10)).collect(Collectors.toList()))
            .build();
        Observable<Integer> result = scalar.call(new Context(), Observable.from(build));
        List<Integer> collect = new ArrayList<>();
        result.forEach(collect::add);
        assertEquals(ImmutableList.of(41, 161, 361, 1441, 1961, 2561), collect);
    }

    @Test
    public void testKeyedWindowWithReduce() {
        FunctionCombinator<Integer, Integer> fns = new FunctionCombinator<Integer, Void>(true)
            .add((MapFunction<Integer, Long>) e -> e + 1L)
            .add((FilterFunction<Long>) e -> e % 2 == 0)
            .add((MapFunction<Long, Long>) e -> e * e)
            .add((MapFunction<Long, String>) e -> e + "1")
            .add((MapFunction<String, Integer>) Integer::parseInt)
            .add(new WindowFunction<>(WindowSpec.count(2)))
            .add(new ReduceFunction<Integer, Integer>() {
                @Override
                public Integer initialValue() {
                    return 0;
                }

                @Override
                public Integer reduce(Integer acc, Integer elem) {
                    return acc + elem;
                }
            });
        GroupToScalarComputation<String, Integer, Integer> scalar = fns.makeGroupToScalarStage();
        List<Integer> elems = ImmutableList.of(0, 1, 2, 3, 4, 5, 6);
        List<MantisGroup<String, Integer>> build = ImmutableList.<MantisGroup<String, Integer>>builder()
            .addAll(elems.stream().map(x -> new MantisGroup<>("k1", x)).collect(Collectors.toList()))
            .addAll(elems.stream().map(x -> new MantisGroup<>("k2", x + 10)).collect(Collectors.toList()))
            .build();
        Observable<Integer> result = scalar.call(new Context(), Observable.from(build));
        List<Integer> collect = new ArrayList<>();
        result.forEach(collect::add);
        assertEquals(ImmutableList.of(202, 3402, 361, 2561), collect);

    }

}
