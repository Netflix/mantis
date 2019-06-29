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

package io.mantisrx.common.network;

import rx.functions.Action0;
import rx.functions.Action1;


public class WorkerEndpoint extends Endpoint {

    private final int stage;
    private final int metricPort;
    private final int workerIndex;
    private final int workerNum;

    public WorkerEndpoint(String host, int port, int stage, int metricPort, int workerIndex, int workerNum) {
        super(host, port);
        this.stage = stage;
        this.metricPort = metricPort;
        this.workerIndex = workerIndex;
        this.workerNum = workerNum;
    }

    public WorkerEndpoint(String host, int port, String slotId, int stage, int metricPort, int workerIndex, int workerNum) {
        super(host, port, slotId);
        this.stage = stage;
        this.metricPort = metricPort;
        this.workerIndex = workerIndex;
        this.workerNum = workerNum;
    }

    public WorkerEndpoint(String host, int port, int stage, int metricPort, int workerIndex, int workerNum,
                          Action0 completedCallback, Action1<Throwable> errorCallback) {
        super(host, port, completedCallback, errorCallback);
        this.stage = stage;
        this.metricPort = metricPort;
        this.workerIndex = workerIndex;
        this.workerNum = workerNum;
    }

    public WorkerEndpoint(String host, int port, String slotId, int stage, int metricPort, int workerIndex, int workerNum,
                          Action0 completedCallback, Action1<Throwable> errorCallback) {
        super(host, port, slotId, completedCallback, errorCallback);
        this.stage = stage;
        this.metricPort = metricPort;
        this.workerIndex = workerIndex;
        this.workerNum = workerNum;
    }

    public int getStage() {
        return stage;
    }

    public int getMetricPort() {
        return metricPort;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WorkerEndpoint that = (WorkerEndpoint) o;

        if (stage != that.stage) return false;
        if (metricPort != that.metricPort) return false;
        if (workerIndex != that.workerIndex) return false;
        return workerNum == that.workerNum;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + stage;
        result = 31 * result + metricPort;
        result = 31 * result + workerIndex;
        result = 31 * result + workerNum;
        return result;
    }

    @Override
    public String toString() {
        return "WorkerEndpoint{" +
                "stage=" + stage +
                ", metricPort=" + metricPort +
                ", workerIndex=" + workerIndex +
                ", workerNum=" + workerNum +
                ", endpoint=" + super.toString() +
                '}';
    }
}
