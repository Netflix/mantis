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

import rx.Observable;
import rx.subjects.BehaviorSubject;


public class Index {

    private final int workerIndex;
    private final Observable<Integer> totalNumWorkersObservable;


    public Index(int offset, int total) {
        this.workerIndex = offset;
        this.totalNumWorkersObservable = BehaviorSubject.create(total);

    }

    public Index(int offset, final Observable<Integer> totalWorkerAtStageObservable) {
        this.workerIndex = offset;
        this.totalNumWorkersObservable = totalWorkerAtStageObservable;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getTotalNumWorkers() {
        return totalNumWorkersObservable.take(1).toBlocking().first();
    }

    public Observable<Integer> getTotalNumWorkersObservable() {
        return totalNumWorkersObservable;
    }

    @Override
    public String toString() {
        return "InputQuota [offset=" + workerIndex + ", total=" + getTotalNumWorkers() + "]";
    }
}
