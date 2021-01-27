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

package io.mantisrx.runtime.scheduler;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Schedules work on the same fixed thread pool executor with 1 thread that is constructed once as part of creating
 * this Scheduler.
 */
public final class MantisRxSingleThreadScheduler extends Scheduler {
    private final Scheduler scheduler;

    public MantisRxSingleThreadScheduler(ThreadFactory threadFactory) {
        ExecutorService executorService = newFixedThreadPool(1, threadFactory);
        this.scheduler = Schedulers.from(executorService);
    }

    @Override
    public Worker createWorker() {
        return this.scheduler.createWorker();
    }
}
