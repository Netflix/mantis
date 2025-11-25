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

package io.mantisrx.server.master.resourcecluster.proto;

import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.resourcecluster.TaskExecutorAllocationRequest;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;

/**
 * Protocol classes for the reservation-based scheduling system.
 * These classes are shared between ResourceClusterActor, ReservationRegistryActor,
 * MantisScheduler, and JobActor.
 */
public final class MantisResourceClusterReservationProto {

    private MantisResourceClusterReservationProto() {}

    /**
     * Unique identifier for a reservation (job + stage).
     */
    @Value
    @Builder
    public static class ReservationKey {
        String jobId;
        int stageNumber;
    }

    /**
     * Priority for ordering reservations within constraint groups.
     * Ordering: REPLACE < SCALE < NEW_JOB (REPLACE processed first).
     */
    @Value
    @Builder
    public static class ReservationPriority implements Comparable<ReservationPriority> {
        public enum PriorityType {
            REPLACE,  // Worker replacement due to failure (highest priority)
            SCALE,    // Scale-up request for existing job
            NEW_JOB   // New job submission (lowest priority)
        }

        PriorityType type;
        int tier;       // Job tier (lower = higher priority within same type)
        long timestamp; // FIFO ordering within same priority and tier

        @Override
        public int compareTo(ReservationPriority other) {
            // 1. First, compare by PriorityType (REPLACE < SCALE < NEW_JOB)
            // Enums compareTo method uses their ordinal (declaration order)
            int typeComparison = this.type.compareTo(other.type);
            if (typeComparison != 0) {
                return typeComparison;
            }

            // 2. If types are equal, compare by tier (ascending)
            int tierComparison = Integer.compare(this.tier, other.tier);
            if (tierComparison != 0) {
                return tierComparison;
            }

            // 3. If types and tiers are equal, compare by timestamp (ascending)
            return Long.compare(this.timestamp, other.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReservationPriority priority = (ReservationPriority) o;
            return tier == priority.tier &&
                timestamp == priority.timestamp &&
                type == priority.type;
        }

        /**
         * Generates a hash code based on type, tier, and timestamp.
         */
        @Override
        public int hashCode() {
            return Objects.hash(type, tier, timestamp);
        }
    }

    /**
     * Full reservation data including scheduling constraints and allocation requests.
     */
    @Value
    @Builder(toBuilder = true)
    public static class Reservation {
        ReservationKey key;
        SchedulingConstraints schedulingConstraints;
        String canonicalConstraintKey;
        Set<WorkerId> requestedWorkers;
        Set<TaskExecutorAllocationRequest> allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;

        public boolean hasSameShape(Reservation other) {
            return other != null
                && Objects.equals(key, other.key)
                && Objects.equals(canonicalConstraintKey, other.canonicalConstraintKey)
                && Objects.equals(requestedWorkers, other.requestedWorkers)
                && stageTargetSize == other.stageTargetSize;
        }

        public int getRequestedWorkersCount() {
            return requestedWorkers != null ? requestedWorkers.size() : 0;
        }

        public static Reservation fromUpsertReservation(UpsertReservation upsert, String canonicalConstraintKey) {
            return Reservation.builder()
                .key(upsert.getReservationKey())
                .schedulingConstraints(upsert.getSchedulingConstraints())
                .canonicalConstraintKey(canonicalConstraintKey)
                .requestedWorkers(upsert.getAllocationRequests() != null ?
                    upsert.getAllocationRequests().stream()
                        .map(TaskExecutorAllocationRequest::getWorkerId)
                        .collect(Collectors.toSet())
                    : Collections.emptySet())
                .allocationRequests(upsert.getAllocationRequests() != null ?
                    upsert.getAllocationRequests() : Collections.emptySet())
                .stageTargetSize(upsert.getStageTargetSize())
                .priority(upsert.getPriority())
                .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Reservation that = (Reservation) o;
            return stageTargetSize == that.stageTargetSize
                && Objects.equals(key, that.key)
                && Objects.equals(canonicalConstraintKey, that.canonicalConstraintKey)
                && Objects.equals(priority, that.priority)
                && Objects.equals(requestedWorkers, that.requestedWorkers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, canonicalConstraintKey, priority, stageTargetSize, requestedWorkers);
        }
    }

    /**
     * Message to insert/update a reservation.
     */
    @Value
    @Builder
    public static class UpsertReservation {
        ReservationKey reservationKey;
        SchedulingConstraints schedulingConstraints;
        Set<TaskExecutorAllocationRequest> allocationRequests;
        int stageTargetSize;
        ReservationPriority priority;
    }

    /**
     * Message to cancel a reservation.
     */
    @Value
    @Builder
    public static class CancelReservation {
        ReservationKey reservationKey;
    }

    /**
     * Response to reservation cancellation.
     */
    @Value
    public static class CancelReservationAck {
        ReservationKey reservationKey;
        boolean cancelled;
    }

    /**
     * Marker message to indicate registry is ready to process reservations.
     */
    public enum MarkReady {
        INSTANCE
    }
}

