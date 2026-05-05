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

package io.mantisrx.master.jobcluster.job;

import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.server.master.domain.Costs;

/**
 * Two-phase wrapper for a worker addition that has been applied to in-memory job metadata
 * but not yet durably persisted. Lets the caller commit the addition (after a successful
 * {@code MantisJobStore.storeNewWorker}) or roll it back (after the store call throws),
 * keeping the in-memory {@link MantisJobMetadataImpl} consistent across partial failures
 * during a scale-up loop.
 *
 * <p>Usage:
 * <pre>{@code
 * PendingWorkerAddition pending = prepareWorker(...);
 * try {
 *     jobStore.storeNewWorker(pending.metadata());
 *     pending.commit();
 *     // ... use pending.metadata() ...
 * } catch (Exception e) {
 *     pending.rollback();
 *     // ... handle/log ...
 * }
 * }</pre>
 *
 * <p>{@link #commit()} and a successful {@link #rollback()} are mutually exclusive and idempotent:
 * once one completes, the other becomes a no-op. If rollback itself throws because the underlying
 * metadata is already corrupt, this wrapper remains unfinished so callers can surface that failure.
 */
final class PendingWorkerAddition {

    private final MantisJobMetadataImpl jobMetaData;
    private final int stageNum;
    private final int workerIndex;
    private final int workerNumber;
    private final IMantisWorkerMetadata metadata;
    private final Costs previousJobCosts;

    private boolean committed;
    private boolean rolledBack;

    PendingWorkerAddition(
            MantisJobMetadataImpl jobMetaData,
            int stageNum,
            int workerIndex,
            int workerNumber,
            IMantisWorkerMetadata metadata,
            Costs previousJobCosts) {
        this.jobMetaData = jobMetaData;
        this.stageNum = stageNum;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.metadata = metadata;
        this.previousJobCosts = previousJobCosts;
    }

    /** The worker metadata to pass to {@code MantisJobStore.storeNewWorker}. */
    IMantisWorkerMetadata metadata() {
        return metadata;
    }

    /**
     * Marks this addition as durable. Subsequent {@link #rollback()} calls become no-ops.
     */
    synchronized void commit() {
        if (rolledBack) {
            return;
        }
        committed = true;
    }

    /**
     * Reverses the in-memory mutations performed when this pending addition was created:
     * removes the worker from the stage's index/number maps and the job-level
     * workerNumber→stage map, and restores the job-level Costs to the snapshot taken
     * before the addition. Idempotent after a successful rollback; no-op if {@link #commit()}
     * has already been called. If rollback itself throws, this wrapper remains unfinished so
     * callers can surface the corruption instead of silently finalizing it.
     */
    synchronized void rollback() {
        if (committed || rolledBack) {
            return;
        }
        jobMetaData.unsafeRemoveWorkerMetadata(stageNum, workerIndex, workerNumber);
        jobMetaData.setJobCosts(previousJobCosts);
        rolledBack = true;
    }
}
