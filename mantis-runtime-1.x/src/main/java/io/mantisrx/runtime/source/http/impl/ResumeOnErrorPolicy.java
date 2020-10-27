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

package io.mantisrx.runtime.source.http.impl;

import rx.Observable;
import rx.functions.Func2;


/**
 * An implementation of this functional interface defines how to resume an {@link rx.Observable} when the
 * {@link rx.Observable}'s runs into an error. This is used by an {@link io.mantisrx.runtime.source.http.impl.OperatorResumeOnCompleted} instance.
 *
 * @param <T> The type of items in the returned new {@link rx.Observable}
 *
 * @see io.mantisrx.runtime.source.http.impl.OperatorResumeOnCompleted
 */
public interface ResumeOnErrorPolicy<T> extends Func2<Integer, Throwable, Observable<T>> {

    /**
     * Called when an {@link rx.Observable} needs to be to resumed upon error.
     *
     * @param attempts The number of the current attempt.
     * @param error    The error of the {@link rx.Observable} to be resumed.
     *
     * @return An {@link rx.Observable} that will be used to replaced the old completed one.
     * Return {@code null} if there should be no more attempt on resuming the old {@link rx.Observable}.
     */
    @Override
    Observable<T> call(Integer attempts, Throwable error);
}
