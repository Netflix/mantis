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

package io.reactivex.mantis.remote.observable;

import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import java.util.Map;
import rx.functions.Func1;


public abstract class ServeConfig<T, O> {

    SlottingStrategy<O> slottingStrategy;
    private String name;
    private Func1<Map<String, String>, Func1<T, Boolean>> filterFunction;
    private int maxWriteAttempts;

    public ServeConfig(String name,
                       SlottingStrategy<O> slottingStrategy, Func1<Map<String, String>,
            Func1<T, Boolean>> filterFunction, int maxWriteAttempts) {
        this.name = name;
        this.slottingStrategy = slottingStrategy;
        this.filterFunction = filterFunction;
        this.maxWriteAttempts = maxWriteAttempts;
    }

    public int getMaxWriteAttempts() {
        return maxWriteAttempts;
    }

    public String getName() {
        return name;
    }

    public SlottingStrategy<O> getSlottingStrategy() {
        return slottingStrategy;
    }

    public Func1<Map<String, String>, Func1<T, Boolean>> getFilterFunction() {
        return filterFunction;
    }
}
