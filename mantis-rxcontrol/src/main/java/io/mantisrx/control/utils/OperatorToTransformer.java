/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.utils;

import rx.Observable;

/*
  Wraps an rx Operator into an rx Transformer
 */
public class OperatorToTransformer<T, R> implements Observable.Transformer<T, R> {

    private final Observable.Operator<R, T> op;

    public OperatorToTransformer(Observable.Operator<R, T> op) {
        this.op = op;
    }

    @Override
    public Observable<R> call(Observable<T> tObservable) {
        return tObservable.lift(op);
    }
}
