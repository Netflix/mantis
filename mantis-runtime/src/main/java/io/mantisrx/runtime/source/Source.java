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

import java.util.Collections;
import java.util.List;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import rx.Observable;
import rx.functions.Func2;


public interface Source<T> extends Func2<Context, Index, Observable<Observable<T>>> {

    default List<ParameterDefinition<?>> getParameters() {
        return Collections.emptyList();
    }

    default void init(Context context, Index index) {

    }
}
