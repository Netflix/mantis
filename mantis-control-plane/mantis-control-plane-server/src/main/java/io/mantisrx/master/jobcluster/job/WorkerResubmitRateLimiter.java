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

package io.mantisrx.master.jobcluster.job;

import io.mantisrx.server.core.domain.WorkerId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Value;


/**
 * This class is not ThreadSafe. It is expected to be invoked by the JobActor
 * (which should guarantee no concurrent invocations.
 */
/* package */class WorkerResubmitRateLimiter {

    private final Map<String, ResubmitRecord> resubmitRecords = new HashMap<>();

    private final Duration expireResubmitDelay;

    private final List<Duration> resubmitIntervals;

    WorkerResubmitRateLimiter(JobSettings jobSettings) {
        this.expireResubmitDelay = jobSettings.getWorkerResubmitExpiry();
        this.resubmitIntervals = jobSettings.getWorkerResubmitIntervals();
    }

    /**
     * Called periodically by Job Actor to purge old records.
     *
     * @param currentTime
     */
    public void expireResubmitRecords(long currentTime) {

        Iterator<ResubmitRecord> it = resubmitRecords.values().iterator();
        while (it.hasNext()) {
            ResubmitRecord record = it.next();
            if (record.getResubmitAt() - getDelay(record.getDelayedIdx()).toMillis() < (currentTime - this.expireResubmitDelay.toMillis())) {
                it.remove();
            }
        }
    }

    /**
     * Given a resubmit record pick the next delay in the array of delay in seconds configured.
     *
     * @param resubmitRecord
     *
     * @return
     */
    int evalNextDelay(@Nullable final ResubmitRecord resubmitRecord) {
        if (resubmitRecord == null) {
            return -1;
        } else {
            return Math.min(resubmitRecord.delayedIdx + 1, resubmitIntervals.size() - 1);
        }
    }

    /**
     * Used for testing.
     *
     * @param workerId
     * @param currentTime
     *
     * @return
     */
    long getWorkerResubmitTime(final WorkerId workerId, final int stageNum, final long currentTime) {
        String workerKey = generateWorkerIndexStageKey(workerId, stageNum);
        final ResubmitRecord prevResubmitRecord = resubmitRecords.get(workerKey);
        int delayIdx = evalNextDelay(prevResubmitRecord);
        Duration delay = getDelay(delayIdx);
        long resubmitAt = currentTime +  delay.toMillis();
        final ResubmitRecord currResubmitRecord = new ResubmitRecord(workerKey, resubmitAt, delayIdx);
        resubmitRecords.put(workerKey, currResubmitRecord);
        return resubmitAt;
    }

    private Duration getDelay(int idx) {
        if (idx < 0) {
            return Duration.ZERO;
        }
        return resubmitIntervals.get(idx);
    }

    /**
     * Get the worker resubmit time for the given worker.
     *
     * @param workerId
     *
     * @return
     */
    public long getWorkerResubmitTime(final WorkerId workerId, final int stageNum) {
        return getWorkerResubmitTime(workerId, stageNum, System.currentTimeMillis());
    }

    /**
     * Appends stage number and worker index.
     * @param workerId
     * @param stageNum
     * @return
     */
    String generateWorkerIndexStageKey(WorkerId workerId, int stageNum) {
        return stageNum + "_" + workerId.getWorkerIndex();
    }

    /**
     * clears the resubmit cache.
     */
    void shutdown() {
        resubmitRecords.clear();
    }

    /**
     * Returns the list of resubmit records.
     * @return
     */
    List<ResubmitRecord> getResubmitRecords() {
        return new ArrayList<>(resubmitRecords.values());
    }

    /**
     * Tracks information about a worker resubmit.
     */
    @Value
    static class ResubmitRecord {

        String workerKey;
        long resubmitAt;
        int delayedIdx;
    }
}
