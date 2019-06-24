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

package io.reactivx.mantis.operators;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.observables.GroupedObservable;


/**
 * copied from https://github.com/Netflix/Turbine/blob/2.x/turbine-core/src/main/java/com/netflix/turbine/internal/GroupedObservableUtils.java
 *
 * @author njoshi
 */

public class GroupedObservableUtils {

    // TODO can we do without this?
    public static <K, T> GroupedObservable<K, T> createGroupedObservable(K key, final Observable<T> o) {
        return GroupedObservable.create(key, new OnSubscribe<T>() {

            @Override
            public void call(Subscriber<? super T> s) {
                o.unsafeSubscribe(s);
            }
        });
    }
}


