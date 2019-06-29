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

package io.reactivex.mantis.remote.observable.slotting;

import io.mantisrx.common.network.WritableEndpoint;
import rx.functions.Action0;


public abstract class SlottingStrategy<T> {

    Action0 doAfterFirstConnectionAdded = new Action0() {
        @Override
        public void call() {}
    };
    Action0 doAfterLastConnectionRemoved = new Action0() {
        @Override
        public void call() {}
    };

    Action0 doOnEachConnectionAdded = new Action0() {
        @Override
        public void call() {}
    };

    Action0 doOnEachConnectionRemoved = new Action0() {
        @Override
        public void call() {}
    };

    public void registerDoOnEachConnectionRemoved(Action0 doOnEachConnectionRemoved) {
        this.doOnEachConnectionRemoved = doOnEachConnectionRemoved;
    }

    public void registerDoOnEachConnectionAdded(Action0 doOnEachConnectionAdded) {
        this.doOnEachConnectionAdded = doOnEachConnectionAdded;
    }

    public void registerDoAfterFirstConnectionAdded(Action0 doAfterFirstConnectionAdded) {
        this.doAfterFirstConnectionAdded = doAfterFirstConnectionAdded;
    }

    public void registerDoAfterLastConnectionRemoved(Action0 doAfterLastConnectionRemoved) {
        this.doAfterLastConnectionRemoved = doAfterLastConnectionRemoved;
    }

    public abstract boolean addConnection(WritableEndpoint<T> endpoint);

    public abstract boolean removeConnection(WritableEndpoint<T> endpoint);

    public abstract void writeOnSlot(byte[] keyBytes, T data);

    public abstract void completeAllConnections();

    public abstract void errorAllConnections(Throwable e);

}
