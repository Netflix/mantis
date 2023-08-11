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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.mantisrx.server.core.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ExceptionUtils;

@Slf4j
public class TaskStatusUpdateHandlerImpl implements TaskStatusUpdateHandler {

    private final MeterRegistry meterRegistry;
    private final Counter failureCounter;
    private final Counter workerSentHeartbeats;
    private final MantisMasterGateway masterMonitor;

    TaskStatusUpdateHandlerImpl(MantisMasterGateway masterGateway, MeterRegistry meterRegistry) {

        String groupName = "ReportStatusServiceHttpImpl";
        this.failureCounter = meterRegistry.counter(groupName + "_failureCounter");
        this.workerSentHeartbeats = meterRegistry.counter(groupName + "_workerSentHeartbeats");
        this.masterMonitor = masterGateway;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onStatusUpdate(Status status) {
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
