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

package io.mantisrx.runtime.computation;

import io.mantisrx.runtime.Context;
import java.util.ArrayList;
import java.util.List;
import rx.Observable;

public class CompositeScalarComputation<T, R> implements ScalarComputation<T, R> {
    List<Computation> functions = new ArrayList<>();

    public void add(Computation scalarFn) {
        functions.add(scalarFn);
    }

    @Override
    public void init(Context context) {
        functions.forEach(fn -> fn.init(context));
    }

    @Override
    public Observable<R> call(Context context, Observable<T> tObservable) {
        Observable<?> current = tObservable;
        for (Computation fn : functions) {
            if (fn instanceof ScalarComputation) {
                current = (Observable<?>) ((ScalarComputation) fn).call(context, current);
            } else if (fn instanceof GroupToScalarComputation) {
                current = (Observable<?>) ((GroupToScalarComputation) fn).call(context, current);
            }
        }
        return (Observable<R>) current;
    }
}
