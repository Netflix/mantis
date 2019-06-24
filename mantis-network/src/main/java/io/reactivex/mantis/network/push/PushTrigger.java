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

package io.reactivex.mantis.network.push;

import io.mantisrx.common.metrics.Metrics;
import rx.functions.Action1;


public class PushTrigger<T> {

    protected MonitoredQueue<T> buffer;
    protected Action1<MonitoredQueue<T>> doOnStart;
    protected Action1<MonitoredQueue<T>> doOnStop;
    protected Metrics metrics;

    public PushTrigger(Action1<MonitoredQueue<T>> doOnStart,
                       Action1<MonitoredQueue<T>> doOnStop,
                       Metrics metrics) {
        this.doOnStart = doOnStart;
        this.doOnStop = doOnStop;
        this.metrics = metrics;
    }

    public void setBuffer(MonitoredQueue<T> buffer) {
        this.buffer = buffer;
    }

    public void start() {
        doOnStart.call(buffer);
    }

    public void stop() {
        doOnStop.call(buffer);
    }

    public Metrics getMetrics() {
        return metrics;
    }
}
