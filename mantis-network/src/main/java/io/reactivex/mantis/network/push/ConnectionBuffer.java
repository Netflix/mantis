/*
 * Copyright 2026 Netflix, Inc.
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
package io.reactivex.mantis.network.push;

import java.util.List;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.Scheduler;

/** Immutable configuration; each subscription owns its buffer and timer. */
final class ConnectionBuffer<T> implements Observable.Transformer<T, List<T>> {
    private final ServerConfig.ConnectionBufferMode mode;
    private final int intervalMs;
    private final Scheduler scheduler;

    ConnectionBuffer(ServerConfig.ConnectionBufferMode mode, int intervalMs, Scheduler scheduler) {
        this.mode = mode;
        this.intervalMs = intervalMs;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<List<T>> call(Observable<T> source) {
        if (mode == ServerConfig.ConnectionBufferMode.ACTIVITY) {
            return source.lift(new ActivityDrivenBuffer<>(intervalMs, TimeUnit.MILLISECONDS, scheduler));
        }
        return source.buffer(intervalMs, TimeUnit.MILLISECONDS, scheduler);
    }
}
