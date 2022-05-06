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
import java.net.SocketTimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;

@Slf4j
public class TaskStatusUpdateHandlerImpl implements TaskStatusUpdateHandler {

    private final Counter hbConnectionTimeoutCounter;
    private final Counter hbConnectionRequestTimeoutCounter;
    private final Counter hbSocketTimeoutCounter;
    private final Counter workerSentHeartbeats;
    private final MantisMasterGateway masterMonitor;

    TaskStatusUpdateHandlerImpl(MantisMasterGateway masterGateway) {
        final Metrics metrics = MetricsRegistry.getInstance().registerAndGet(new Metrics.Builder()
                .name("ReportStatusServiceHttpImpl")
                .addCounter("hbConnectionTimeoutCounter")
                .addCounter("hbConnectionRequestTimeoutCounter")
                .addCounter("hbSocketTimeoutCounter")
                .addCounter("workerSentHeartbeats")
                .build());

        this.hbConnectionTimeoutCounter = metrics.getCounter("hbConnectionTimeoutCounter");
        this.hbConnectionRequestTimeoutCounter = metrics.getCounter(
                "hbConnectionRequestTimeoutCounter");
        this.hbSocketTimeoutCounter = metrics.getCounter("hbSocketTimeoutCounter");
        this.workerSentHeartbeats = metrics.getCounter("workerSentHeartbeats");
        this.masterMonitor = masterGateway;
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
                        if (cleaned instanceof SocketTimeoutException) {
                            log.warn("SocketTimeoutException: Failed to send status update", cleaned);
                            hbSocketTimeoutCounter.increment();
                        } else if (cleaned instanceof ConnectionPoolTimeoutException) {
                            log.warn("ConnectionPoolTimeoutException: Failed to send status update", cleaned);
                            hbConnectionRequestTimeoutCounter.increment();
                        } else if (cleaned instanceof ConnectTimeoutException) {
                            log.warn("ConnectTimeoutException: Failed to send status update", cleaned);
                            hbConnectionTimeoutCounter.increment();
                        } else {
                            log.error("Failed to send status update", cleaned);
                        }
                    }
                });
    }
}
