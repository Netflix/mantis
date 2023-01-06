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

import static org.junit.Assert.*;

import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import org.junit.Test;

public class WorkerResubmitRateLimiterTest {

    private static final JobSettings jobSettings =
        JobSettings
            .builder()
            .workerResubmitIntervals(ImmutableList.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(15)))
            .workerResubmitExpiry(Duration.ofSeconds(5))
            .build();


    @Test
    public void addWorkerTest() {
        WorkerResubmitRateLimiter wrrl = new WorkerResubmitRateLimiter(jobSettings);
        int stageNum = 1;
        long currTime = System.currentTimeMillis();
        WorkerId workerId = new WorkerId("TestJob-1", 0, 1);
        long resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 5000, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 10000, resubmitTime);


        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 15000, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 15000, resubmitTime);

    }

    @Test
    public void addMultipleWorkerTest() {
        WorkerResubmitRateLimiter wrrl = new WorkerResubmitRateLimiter(jobSettings);
        int stageNum = 1;
        long currTime = System.currentTimeMillis();
        WorkerId workerId = new WorkerId("TestJob-1", 0, 1);
        WorkerId workerId2 = new WorkerId("TestJob-1", 1, 2);
        long resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 5000, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId2, stageNum, currTime);
        assertEquals(currTime, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 10000, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId2, stageNum, currTime);
        assertEquals(currTime + 5000, resubmitTime);


        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 15000, resubmitTime);

        resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);
        assertEquals(currTime + 15000, resubmitTime);

    }

    @Test
    public void expireOldEntryTest() {

        WorkerResubmitRateLimiter wrrl = new WorkerResubmitRateLimiter(jobSettings);
        int stageNum = 1;
        long currTime = System.currentTimeMillis();
        WorkerId workerId = new WorkerId("TestJob-1", 0, 1);
        WorkerId workerId2 = new WorkerId("TestJob-1", 1, 2);

        long resubmitTime = wrrl.getWorkerResubmitTime(workerId, stageNum, currTime);

        List<WorkerResubmitRateLimiter.ResubmitRecord> resubmitRecords = wrrl.getResubmitRecords();
        assertTrue(resubmitRecords.size() == 1);


        currTime += 4_000;
        resubmitTime = wrrl.getWorkerResubmitTime(workerId2, stageNum, currTime);
        resubmitRecords = wrrl.getResubmitRecords();

        assertEquals(2, resubmitRecords.size());


        // Move time now to 6 seconds which is greater than expiry time of 5
        currTime += 2000;

        // This should expire worker id 1 but not 2
        wrrl.expireResubmitRecords(currTime);
        resubmitRecords = wrrl.getResubmitRecords();

        assertEquals(1, resubmitRecords.size());

        assertEquals(stageNum + "_" + workerId2.getWorkerIndex(), resubmitRecords.get(0).getWorkerKey());







    }


}
