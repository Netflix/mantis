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

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.config.ConfigurationProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is not ThreadSafe. It is expected to be invoked by the JobActor
 * (which should guarantee no concurrent invocations.
 */
/* package */class WorkerResubmitRateLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerResubmitRateLimiter.class);
    private static final String DEFAULT_WORKER_RESUBMIT_INTERVAL_SECS_STR = "5:10:20";

    private final Map<String, ResubmitRecord> resubmitRecords = new HashMap<>();

    private static final long DEFAULT_EXPIRE_RESUBMIT_DELAY_SECS = 300;
    private static final long DEFAULT_EXPIRE_RESUBMIT_DELAY_EXECUTION_INTERVAL_SECS = 120;
    private static final long DEFAULT_RESUBMISSION_INTERVAL_SECS = 10;

    private final long expireResubmitDelaySecs;

    private final long[] resubmitIntervalSecs;

    /**
     * Constructor for this class.
     * @param workerResubmitIntervalSecs
     * @param expireResubmitDelaySecs
     */
    WorkerResubmitRateLimiter(String workerResubmitIntervalSecs, long expireResubmitDelaySecs) {


        Preconditions.checkArg(expireResubmitDelaySecs > 0, "Expire "
                + "Resubmit Delay cannot be 0 or less");
        if (workerResubmitIntervalSecs == null || workerResubmitIntervalSecs.isEmpty())
            workerResubmitIntervalSecs = DEFAULT_WORKER_RESUBMIT_INTERVAL_SECS_STR;
        StringTokenizer tokenizer = new StringTokenizer(workerResubmitIntervalSecs, ":");
        if (tokenizer.countTokens() == 0) {
            this.resubmitIntervalSecs = new long[2];
            this.resubmitIntervalSecs[0] = 0L;
            this.resubmitIntervalSecs[1] = DEFAULT_RESUBMISSION_INTERVAL_SECS;
        } else {
            this.resubmitIntervalSecs = new long[tokenizer.countTokens() + 1];
            this.resubmitIntervalSecs[0] = 0L;
            for (int i = 1; i < this.resubmitIntervalSecs.length; i++) {
                final String s = tokenizer.nextToken();
                try {
                    this.resubmitIntervalSecs[i] = Long.parseLong(s);
                } catch (NumberFormatException e) {
                    LOGGER.warn("Invalid number for resubmit interval " + s + ": using default "
                            + DEFAULT_RESUBMISSION_INTERVAL_SECS);
                    this.resubmitIntervalSecs[i] = DEFAULT_RESUBMISSION_INTERVAL_SECS;
                }
            }
        }
        this.expireResubmitDelaySecs = expireResubmitDelaySecs;

    }

    /**
     * Default constructor.
     */
    WorkerResubmitRateLimiter() {
        this(ConfigurationProvider.getConfig().getWorkerResubmitIntervalSecs(),
                ConfigurationProvider.getConfig().getExpireWorkerResubmitDelaySecs());
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
            if (record.getResubmitAt() - record.getDelayedBy() < (currentTime - this.expireResubmitDelaySecs * 1000))
                it.remove();
        }
    }

    /**
     * Given a resubmit record pick the next delay in the array of delay in seconds configured.
     *
     * @param resubmitRecord
     *
     * @return
     */
    long evalDelay(final ResubmitRecord resubmitRecord) {
        long delay = resubmitIntervalSecs[0];
        if (resubmitRecord != null) {
            long prevDelay = resubmitRecord.getDelayedBy();
            int index = 0;
            for (; index < resubmitIntervalSecs.length; index++)
                if (prevDelay <= resubmitIntervalSecs[index])
                    break;
            index++;
            if (index >= resubmitIntervalSecs.length)
                index = resubmitIntervalSecs.length - 1;
            delay = resubmitIntervalSecs[index];
        }
        return delay;
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
        long delay = evalDelay(prevResubmitRecord);
        long resubmitAt = currentTime + delay * 1000;
        final ResubmitRecord currResubmitRecord = new ResubmitRecord(workerKey, resubmitAt, delay);
        resubmitRecords.put(workerKey, currResubmitRecord);
        return resubmitAt;
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
        Map<String, ResubmitRecord> copy = new HashMap<>(resubmitRecords.size());
        List<ResubmitRecord> resubmitRecordList = resubmitRecords.values().stream().collect(Collectors.toList());
        return resubmitRecordList;
    }

    long getExpireResubmitDelaySecs() {
        return expireResubmitDelaySecs;
    }


    public long[] getResubmitIntervalSecs() {
        return resubmitIntervalSecs;
    }

    /**
     * Tracks information about a worker resubmit.
     */
    static final class ResubmitRecord {

        private final String workerKey;
        private final long resubmitAt;
        private final long delayedBy;

        private ResubmitRecord(String workerKey, long resubmitAt, long delayedBy) {
            this.workerKey = workerKey;
            this.resubmitAt = resubmitAt;
            this.delayedBy = delayedBy;
        }

        public long getDelayedBy() {
            return delayedBy;
        }

        public String getWorkerKey() {
            return this.workerKey;
        }

        public long getResubmitAt() {
            return resubmitAt;
        }
    }
}
