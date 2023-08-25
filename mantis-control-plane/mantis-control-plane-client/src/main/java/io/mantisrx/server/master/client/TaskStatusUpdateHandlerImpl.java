/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master.client;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ExceptionUtils;

@Slf4j
public class TaskStatusUpdateHandlerImpl implements TaskStatusUpdateHandler {

    private final Counter failureCounter;
    private final Counter workerSentHeartbeats;
    private final MantisMasterGateway masterMonitor;

    TaskStatusUpdateHandlerImpl(MantisMasterGateway masterGateway) {
        final Metrics metrics = MetricsRegistry.getInstance().registerAndGet(new Metrics.Builder()
                .name("ReportStatusServiceHttpImpl")
                .addCounter("failureCounter")
                .addCounter("workerSentHeartbeats")
                .build());

        this.failureCounter = metrics.getCounter("failureCounter");
        this.workerSentHeartbeats = metrics.getCounter("workerSentHeartbeats");
        this.masterMonitor = masterGateway;
    }

    @Override
    public void onStatusUpdate(Status status) {
        log.info("onStatusUpdate for status: {}", status);
        masterMonitor
                .updateStatus(status)
                .whenComplete((ack, throwable) -> {
                    if (ack != null) {
                        workerSentHeartbeats.increment();
                    } else {
                        Throwable cleaned = ExceptionUtils.stripExecutionException(throwable);
                        failureCounter.increment();
                        log.error("Failed to send status update", cleaned);
                    }
                });
    }
}
