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

package io.mantisrx.runtime.executor;

import io.mantisrx.runtime.StageConfig;
import io.reactivex.mantis.remote.observable.RxMetrics;
import java.io.Closeable;
import rx.Observable;

/**
 * WorkerPublisher is an abstraction for a sink operator execution.
 * @param <T> type of the observable that's getting published
 */
public interface WorkerPublisher<T> extends Closeable {

    void start(StageConfig<?, T> stage, Observable<Observable<T>> observableToPublish);

    RxMetrics getMetrics();
}
