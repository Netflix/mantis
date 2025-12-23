/*
 * Copyright 2025 Netflix, Inc.
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

package io.mantisrx.server.master.scheduler;

import io.mantisrx.common.Ack;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.CancelReservation;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.UpsertReservation;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationKey;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Scheduler implementation that uses the reservation-based scheduling system.
 *
 * This scheduler delegates scheduling decisions to the ReservationRegistryActor
 * which processes reservations in priority order per constraint group.
 * WorkerLaunched events are still routed via jobMessageRouter from ExecutorStateManagerActor.
 */
@Slf4j
@RequiredArgsConstructor
public class ResourceClusterReservationAwareScheduler implements MantisScheduler {

    private final ResourceCluster resourceCluster;

    // ==================== Reservation APIs ====================

    @Override
    public CompletableFuture<Ack> upsertReservation(UpsertReservation request) {
        ReservationKey key = request.getReservationKey();
        log.info("Upserting reservation for job {} stage {} with {} workers (priority={})",
            key.getJobId(),
            key.getStageNumber(),
            request.getAllocationRequests() != null ? request.getAllocationRequests().size() : 0,
            request.getPriority() != null ? request.getPriority().getType() : "UNKNOWN");
        return resourceCluster.upsertReservation(request);
    }

    @Override
    public CompletableFuture<Ack> cancelReservation(CancelReservation request) {
        ReservationKey key = request.getReservationKey();
        log.info("Cancelling reservation for job {} stage {}", key.getJobId(), key.getStageNumber());
        return resourceCluster.cancelReservation(request);
    }

    @Override
    public boolean schedulerHandlesAllocationRetries() {
        // Reservation registry handles retries until the worker is allocated (launched)
        return true;
    }

    // ==================== Legacy APIs ====================

    @Override
    public void scheduleWorkers(BatchScheduleRequest scheduleRequest) {
        throw new UnsupportedOperationException(
            "Use upsertReservation() instead of scheduleWorkers() with reservation-based scheduling");
    }

    @Override
    public void unscheduleJob(String jobId) {
        // For reservation-based scheduling, caller should use cancelReservation per stage
        log.warn("unscheduleJob({}) called - use cancelReservation() per stage instead", jobId);
    }

    @Override
    public void unscheduleWorker(WorkerId workerId, Optional<String> hostname) {
        throw new UnsupportedOperationException(
            "unscheduleWorker not supported - use cancelReservation() for pending workers");
    }

    @Override
    public void unscheduleAndTerminateWorker(WorkerId workerId, Optional<String> hostname) {
        log.info("Terminating worker {}", workerId);
        resourceCluster.terminateWorker(workerId)
            .whenComplete((ack, ex) -> {
                if (ex != null) {
                    log.warn("Failed to request worker termination {}", workerId, ex);
                }
            });
    }

    @Override
    public void updateWorkerSchedulingReadyTime(WorkerId workerId, long when) {
        throw new UnsupportedOperationException(
            "Reservation registry handles retry timing internally");
    }

    @Override
    public void initializeRunningWorker(ScheduleRequest scheduleRequest, String hostname, String hostID) {
        // Still needed for leader switch recovery
        log.debug("[Noop] Initializing running worker {}", scheduleRequest.getWorkerId());
//        resourceCluster.initializeTaskExecutor(TaskExecutorID.of(hostID), scheduleRequest.getWorkerId())
//            .whenComplete((ack, ex) -> {
//                if (ex != null) {
//                    log.warn("Failed to initialize running worker {}", scheduleRequest.getWorkerId(), ex);
//                }
//            });
    }
}

