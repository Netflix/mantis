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

import io.mantisrx.common.MantisGroup;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.Computation;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.core.WindowSpec;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import rx.Observable;

public class FunctionCombinator<T, R> {

    @Getter
    private final boolean isKeyed;
    private final List<MantisFunction> functions;

    public FunctionCombinator(boolean isKeyed) {
        this(isKeyed, ImmutableList.of());
    }

    public FunctionCombinator(boolean isKeyed, List<MantisFunction> functions) {
        this.isKeyed = isKeyed;
        this.functions = functions;
    }

    public <IN, OUT> FunctionCombinator<T, OUT> add(MantisFunction fn) {
        ImmutableList<MantisFunction> functions = ImmutableList.<MantisFunction>builder().addAll(this.functions).add(fn).build();
        return new FunctionCombinator<>(this.isKeyed, functions);
    }

    public int size() {
        return functions.size();
    }

    @VisibleForTesting
    <U, V> ScalarComputation<U, V> makeScalarStage() {
        return new ScalarComputation<U, V>() {
            @Override
            public void init(Context context) {
                functions.forEach(MantisFunction::init);
            }

            @Override
            public Observable<V> call(Context context, Observable<U> uObservable) {
                Observable<?> current = uObservable;
                for (MantisFunction fn : functions) {
                    if (fn instanceof FilterFunction) {
                        current = current.filter(((FilterFunction) fn)::apply);
                    } else if (fn instanceof MapFunction) {
                        current = current.map(e -> ((MapFunction) fn).apply(e));
                    } else if (fn instanceof FlatMapFunction) {
                        current = current.flatMap(e -> Observable.from(((FlatMapFunction) fn).apply(e)));
                    }
                }
                return (Observable<V>) current;
            }
        };
    }

    @VisibleForTesting
    <K, U, V> GroupToScalarComputation<K, U, V> makeGroupToScalarStage() {
        return new GroupToScalarComputation<K, U, V>() {
            @Override
            public void init(Context context) {
                functions.forEach(MantisFunction::init);
            }

            @Override
            public Observable<V> call(Context context, Observable<MantisGroup<K, U>> gobs) {
                Observable<?> observable = gobs.groupBy(MantisGroup::getKeyValue).flatMap(gob -> {
                    Observable<?> current = gob.map(MantisGroup::getValue);
                    K key = gob.getKey();
                    for (MantisFunction fn : functions) {
                        if (fn instanceof FilterFunction) {
                            current = current.filter(((FilterFunction) fn)::apply);
                        } else if (fn instanceof MapFunction) {
                            current = current.map(x -> ((MapFunction) fn).apply(x));
                        } else if (fn instanceof FlatMapFunction) {
                            current = current.flatMap(x -> Observable.from(((FlatMapFunction) fn).apply(x)));
                        } else if (fn instanceof WindowFunction) {
                            current = handleWindows(current, (WindowFunction) fn);
                        } else if (fn instanceof ReduceFunction) {
                            ReduceFunction reduceFn = (ReduceFunction) fn;
                            current = ((Observable<Observable<?>>) current)
                                .map(obs -> obs.reduce(reduceFn.initialValue(), (acc, e) -> reduceFn.reduce(acc, e)))
                                .flatMap(x -> x)
                                .filter(x -> x != SimpleReduceFunction.EMPTY);
                        }
                    }
                    return current;
                });
                return (Observable<V>) observable;
            }
        };
    }

    private Observable<? extends Observable<?>> handleWindows(Observable<?> obs, WindowFunction<?> windowFn) {
        WindowSpec spec = windowFn.getSpec();
        switch (spec.getType()) {
            case ELEMENT:
            case ELEMENT_SLIDING:
                return obs.window(spec.getNumElements(), spec.getElementOffset());
            case TUMBLING:
            case SLIDING:
                return obs.window(spec.getWindowLength().toMillis(), spec.getWindowOffset().toMillis(), TimeUnit.MILLISECONDS);
        }
        throw new UnsupportedOperationException("Unknown WindowSpec must be one of " + Arrays.toString(WindowSpec.WindowType.values()));
    }

    public Computation makeStage() {
        if (size() == 0) {
            return null;
        }
        if (isKeyed) {
            return makeGroupToScalarStage();
        }
        return makeScalarStage();
    }
}
