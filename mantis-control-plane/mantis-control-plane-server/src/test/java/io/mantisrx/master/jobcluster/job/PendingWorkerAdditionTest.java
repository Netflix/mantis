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

import static io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl.MANTIS_SYSTEM_ALLOCATED_NUM_PORTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.events.AuditEventSubscriberLoggingImpl;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventPublisherImpl;
import io.mantisrx.master.events.StatusEventSubscriberLoggingImpl;
import io.mantisrx.master.events.WorkerEventSubscriberLoggingImpl;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.domain.Costs;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.ConcurrentMap;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link PendingWorkerAddition}'s rollback / commit semantics.
 *
 * <p>These tests construct a real {@link MantisJobMetadataImpl} with one stage and one
 * pre-existing worker, simulate the in-memory mutations that {@code prepareWorker} would
 * have applied, wrap them in a {@code PendingWorkerAddition}, then assert that
 * {@link PendingWorkerAddition#rollback()} fully reverts those mutations and that
 * {@link PendingWorkerAddition#commit()} disables further rollback.
 */
public class PendingWorkerAdditionTest {

    private static final String CLUSTER = "PendingWorkerAdditionTestCluster";
    private static final int STAGE_NUM = 1;
    private static final int EXISTING_WORKER_INDEX = 0;
    private static final int EXISTING_WORKER_NUMBER = 1;
    private static final int NEW_WORKER_INDEX = 1;
    private static final int NEW_WORKER_NUMBER = 2;

    private LifecycleEventPublisher eventPublisher;
    private MantisJobMetadataImpl jobMetaData;
    private Costs preAdditionCosts;

    @Before
    public void setUp() throws Exception {
        eventPublisher = new LifecycleEventPublisherImpl(
                new AuditEventSubscriberLoggingImpl(),
                new StatusEventSubscriberLoggingImpl(),
                new WorkerEventSubscriberLoggingImpl());

        JobDefinition jobDefn = JobTestHelper.generateJobDefinition(CLUSTER, makeSchedulingInfo());
        JobId jobId = new JobId(CLUSTER, 1);
        jobMetaData = new MantisJobMetadataImpl.Builder()
                .withJobId(jobId)
                .withSubmittedAt(Instant.now())
                .withJobState(io.mantisrx.master.jobcluster.job.JobState.Accepted)
                .withNextWorkerNumToUse(EXISTING_WORKER_NUMBER + 1)
                .withJobDefinition(jobDefn)
                .build();

        // Add the stage and a pre-existing worker so we have a meaningful baseline state to compare against.
        StageSchedulingInfo stage = jobDefn.getSchedulingInfo().getStages().get(STAGE_NUM);
        IMantisStageMetadata stageMeta = new MantisStageMetadataImpl.Builder()
                .withJobId(jobId)
                .withStageNum(STAGE_NUM)
                .withNumStages(1)
                .withMachineDefinition(stage.getMachineDefinition())
                .withNumWorkers(stage.getNumberOfInstances())
                .withHardConstraints(stage.getHardConstraints())
                .withSoftConstraints(stage.getSoftConstraints())
                .withScalingPolicy(stage.getScalingPolicy())
                .isScalable(stage.getScalable())
                .build();
        jobMetaData.addJobStageIfAbsent(stageMeta);
        jobMetaData.addWorkerMetadata(STAGE_NUM, makeWorker(jobId, EXISTING_WORKER_INDEX, EXISTING_WORKER_NUMBER));

        // Snapshot baseline costs (set before the simulated pending addition).
        preAdditionCosts = new Costs(42.0);
        jobMetaData.setJobCosts(preAdditionCosts);
    }

    @Test
    public void rollback_removesFromBothStageMaps_andRestoresCosts() {
        PendingWorkerAddition pending = simulatePreparedAddition(new Costs(99.0));

        assertTrue("new worker should be in stage maps after addition",
                stageContainsWorker(NEW_WORKER_NUMBER));
        assertEquals("workerNumberToStageMap should map new worker number",
                Integer.valueOf(STAGE_NUM),
                jobMetaData.getWorkerNumberToStageMap().get(NEW_WORKER_NUMBER));
        assertEquals(new Costs(99.0), jobMetaData.getJobCosts());
        assertEquals("stage size should reflect both workers", 2, stageWorkerCount());

        pending.rollback();

        assertFalse("new worker must be absent from stage maps after rollback",
                stageContainsWorker(NEW_WORKER_NUMBER));
        assertFalse("workerNumberToStageMap must not contain the rolled-back worker number",
                jobMetaData.getWorkerNumberToStageMap().containsKey(NEW_WORKER_NUMBER));
        assertEquals("Costs must be restored to the pre-addition snapshot",
                preAdditionCosts, jobMetaData.getJobCosts());
        assertTrue("pre-existing worker must be untouched",
                stageContainsWorker(EXISTING_WORKER_NUMBER));
        assertEquals("stage size should be back to 1 after rollback", 1, stageWorkerCount());
    }

    @Test
    public void rollback_isIdempotent() {
        PendingWorkerAddition pending = simulatePreparedAddition(new Costs(99.0));
        pending.rollback();
        // Second rollback must be a no-op even if internal state would otherwise be inconsistent.
        pending.rollback();
        assertFalse(stageContainsWorker(NEW_WORKER_NUMBER));
        assertEquals(preAdditionCosts, jobMetaData.getJobCosts());
        assertEquals(1, stageWorkerCount());
    }

    @Test
    public void commit_thenRollback_isNoop() {
        Costs postAdditionCosts = new Costs(99.0);
        PendingWorkerAddition pending = simulatePreparedAddition(postAdditionCosts);
        pending.commit();
        // After commit, rollback() must NOT undo anything: the worker stays in metadata,
        // costs remain at the post-addition value (i.e. what storeNewWorker successfully durably stored).
        pending.rollback();
        assertTrue("committed worker must remain in stage maps after attempted rollback",
                stageContainsWorker(NEW_WORKER_NUMBER));
        assertEquals("workerNumberToStageMap must still map committed worker",
                Integer.valueOf(STAGE_NUM),
                jobMetaData.getWorkerNumberToStageMap().get(NEW_WORKER_NUMBER));
        assertEquals("post-addition costs must be preserved after commit",
                postAdditionCosts, jobMetaData.getJobCosts());
        assertEquals(2, stageWorkerCount());
    }

    @Test
    public void rollback_corruptionThrows_andDoesNotFinalizeWrapper() throws Exception {
        Costs postAdditionCosts = new Costs(99.0);
        PendingWorkerAddition pending = simulatePreparedAddition(postAdditionCosts);
        MantisStageMetadataImpl stageMeta =
                (MantisStageMetadataImpl) jobMetaData.getStageMetadata(STAGE_NUM).get();

        corruptStageIndexEntry(
                stageMeta,
                NEW_WORKER_INDEX,
                makeWorker(jobMetaData.getJobId(), NEW_WORKER_INDEX, NEW_WORKER_NUMBER));

        try {
            pending.rollback();
            fail("expected rollback corruption to throw");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(jobMetaData.getJobId().getId()));
            assertTrue(expected.getMessage().contains("stage " + STAGE_NUM));
        }

        assertEquals("failed rollback must leave the pending worker in the stage index map",
                NEW_WORKER_NUMBER,
                stageMeta.getWorkerByIndex(NEW_WORKER_INDEX).getMetadata().getWorkerNumber());
        assertTrue("failed rollback must leave the pending worker in stage worker metadata",
                stageContainsWorker(NEW_WORKER_NUMBER));
        assertEquals("failed rollback must leave the job-level worker mapping untouched",
                Integer.valueOf(STAGE_NUM),
                jobMetaData.getWorkerNumberToStageMap().get(NEW_WORKER_NUMBER));
        assertEquals("failed rollback must not restore costs",
                postAdditionCosts, jobMetaData.getJobCosts());

        try {
            pending.rollback();
            fail("expected second rollback attempt to retry and throw again");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(jobMetaData.getJobId().getId()));
        }
    }

    private boolean stageContainsWorker(int workerNumber) {
        return jobMetaData.getStageMetadata(STAGE_NUM).get().getAllWorkers().stream()
                .anyMatch(w -> w.getMetadata().getWorkerNumber() == workerNumber);
    }

    private int stageWorkerCount() {
        return jobMetaData.getStageMetadata(STAGE_NUM).get().getAllWorkers().size();
    }

    private void corruptStageIndexEntry(MantisStageMetadataImpl stageMeta, int workerIndex, JobWorker worker)
            throws Exception {
        stageIndexMap(stageMeta).put(workerIndex, worker);
    }

    /**
     * Mimics what {@code JobActor.prepareWorker} does: snapshots the current Costs,
     * adds a new worker to the stage, sets a new (post-addition) Costs, and returns the wrapper.
     */
    private PendingWorkerAddition simulatePreparedAddition(Costs postAdditionCosts) {
        Costs previousJobCosts = jobMetaData.getJobCosts();

        JobWorker newWorker = makeWorker(jobMetaData.getJobId(), NEW_WORKER_INDEX, NEW_WORKER_NUMBER);
        assertTrue("addWorkerMetadata must succeed in test fixture",
                jobMetaData.addWorkerMetadata(STAGE_NUM, newWorker));
        jobMetaData.setJobCosts(postAdditionCosts);

        return new PendingWorkerAddition(
                jobMetaData,
                STAGE_NUM,
                NEW_WORKER_INDEX,
                NEW_WORKER_NUMBER,
                newWorker.getMetadata(),
                previousJobCosts);
    }

    private JobWorker makeWorker(JobId jobId, int workerIndex, int workerNumber) {
        return new JobWorker.Builder()
                .withJobId(jobId)
                .withWorkerIndex(workerIndex)
                .withWorkerNumber(workerNumber)
                .withNumberOfPorts(1 + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                .withWorkerPorts(new WorkerPorts(ImmutableList.of(9091, 9092, 9093, 9094, 9095)))
                .withStageNum(STAGE_NUM)
                .withLifecycleEventsPublisher(eventPublisher)
                .build();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<Integer, JobWorker> stageIndexMap(MantisStageMetadataImpl stageMeta)
            throws Exception {
        Field field = MantisStageMetadataImpl.class.getDeclaredField("workerByIndexMetadataSet");
        field.setAccessible(true);
        return (ConcurrentMap<Integer, JobWorker>) field.get(stageMeta);
    }

    private static SchedulingInfo makeSchedulingInfo() {
        return new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerStageWithConstraints(
                        1,
                        new MachineDefinition(1.0, 1.0, 1.0, 3),
                        Lists.newArrayList(),
                        Lists.newArrayList())
                .build();
    }
}
