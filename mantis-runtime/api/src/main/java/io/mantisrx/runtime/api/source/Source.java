/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.runtime.api.source;

import java.util.Collections;
import java.util.List;

import io.mantisrx.runtime.api.Context;
import io.mantisrx.runtime.api.parameter.ParameterDefinition;
import org.reactivestreams.Publisher;


public interface Source<T> {

    /**
     * Create a data source connector
     * @param context Execution context for this job
     * @param index worker index and total number of workers in source stage
     * @return Publisher<Publisher<T>> A source can generate many streams of events which change dynamically as
     * emitted
     * by the outer Publisher, the inner Publisher<T> represents one such stream
     */
    Publisher<Publisher<T>> createDataSource(Context context, Index index);

    default List<ParameterDefinition<?>> getParameters() {
        return Collections.emptyList();
    }

    default void init(Context context, Index index) { }
}
