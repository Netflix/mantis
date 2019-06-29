/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.runtime.source;

import java.util.concurrent.TimeUnit;

import io.mantisrx.runtime.Context;
import rx.Observable;
import rx.functions.Func1;


public class Sources {

    private Sources() {}

    public static <T> Source<T> observable(final Observable<T> o) {
        return new Source<T>() {
            @Override
            public Observable<Observable<T>> call(Context context, Index t1) {
                return Observable.just(o);
            }
        };
    }

    public static <T> Source<T> observables(final Observable<Observable<T>> o) {
        return new Source<T>() {
            @Override
            public Observable<Observable<T>> call(Context context, Index t1) {
                return o;
            }
        };
    }

    public static Source<Integer> integerPerSecond() {
        return integers(0, 1);
    }

    public static Source<Integer> integerPerSecond(int initialDelay) {
        return integers(initialDelay, 1);
    }

    public static Source<Integer> integers(final int initialDelay, final long periodInSec) {
        return new Source<Integer>() {
            @Override
            public Observable<Observable<Integer>> call(Context t1, Index t2) {
                return Observable.just(Observable.interval(initialDelay, periodInSec, TimeUnit.SECONDS)
                        .map(new Func1<Long, Integer>() {
                            @Override
                            public Integer call(Long t1) {
                                return (int) (long) t1;
                            }
                        }));
            }
        };
    }
}
