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

package io.mantisrx.runtime;

import io.mantisrx.common.MantisGroup;
import io.reactivx.mantis.operators.GroupedObservableUtils;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.GroupedObservable;


public class Groups {

    private Groups() {}

    public static <K, T> Observable<GroupedObservable<K, T>> flatten(
            Observable<Observable<GroupedObservable<K, T>>> groups) {
        Observable<GroupedObservable<K, T>> flattenedGroups = Observable.merge(groups);
        return flattenedGroups
                //				// re-group by key
                .groupBy(GroupedObservable::getKey)

                // flatten, with merged group
                .flatMap(new Func1<GroupedObservable<K, GroupedObservable<K, T>>, Observable<GroupedObservable<K, T>>>() {
                    @Override
                    public Observable<GroupedObservable<K, T>> call(
                            GroupedObservable<K, GroupedObservable<K, T>> groups) {
                        return Observable.just(GroupedObservableUtils.createGroupedObservable(groups.getKey(),
                                Observable.merge(groups)));
                    }
                });
    }

    /**
     * Convert O O MantisGroup  to  O GroupedObservable
     *
     * @param <K> MantisGroup<K, T> groups keyValue
     *
     * @param <T> MantisGroup<K, T>> groups value
     *
     * @return Observable
     */

    public static <K, T> Observable<GroupedObservable<K, T>> flattenMantisGroupsToGroupedObservables(
            Observable<Observable<MantisGroup<K, T>>> groups) {
        Observable<MantisGroup<K, T>> flattenedGroups = Observable.merge(groups);
        return flattenedGroups.groupBy(MantisGroup::getKeyValue, MantisGroup::getValue);


    }
}
