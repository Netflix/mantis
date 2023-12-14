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

package io.mantisrx.master.jobcluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.actor.AbstractActor;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.common.Label;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.JobClusterActor.JobInfo;
import io.mantisrx.master.jobcluster.JobClusterActor.JobManager;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.JobTestHelper;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobManagerTest {

    private static MantisJobStore jobStore;
    private static AbstractActor.ActorContext context;
    private static MantisSchedulerFactory schedulerFactory;
    private static LifecycleEventPublisher publisher;
    private final CostsCalculator costsCalculator = CostsCalculator.noop();
    private static JobDefinition jobDefinition;

    @BeforeClass
    public static void setup() {
        jobStore = mock(MantisJobStore.class);
        context = mock(AbstractActor.ActorContext.class);
        schedulerFactory = mock(MantisSchedulerFactory.class);
        publisher = mock(LifecycleEventPublisher.class);
        jobDefinition = mock(JobDefinition.class);
        when(jobDefinition.getLabels()).thenReturn(ImmutableList.of(new Label("label1", "value1")));

        JobTestHelper.createDirsIfRequired();
        TestHelpers.setupMasterConfig();

    }

    @Test
    public void acceptedToActive() {

        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertEquals(1, jm.acceptedJobsCount());

        assertTrue(jm.markJobStarted(jInfo1));

        assertEquals(0, jm.acceptedJobsCount());
        assertEquals(1, jm.activeJobsCount());

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo1));
    }

    @Test
    public void acceptedToCompleted() throws IOException {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertEquals(1, jm.acceptedJobsCount());

        assertTrue(jm.getCompletedJobsList(100, null).size() == 0);

        assertTrue(
            jm.markCompleted(jId1, System.currentTimeMillis(), JobState.Completed).isPresent());

        assertEquals(0, jm.acceptedJobsCount());
        assertEquals(1, jm.getCompletedJobsList(100, null).size());
        assertEquals(0, jm.activeJobsCount());

        assertFalse(jm.getAllNonTerminalJobsList().contains(jInfo1));

        assertTrue(jm.getCompletedJobsList(100, null).size() == 1);

        JobClusterDefinitionImpl.CompletedJob completedJob = jm.getCompletedJobsList(100, null).get(0);

        assertEquals(jId1.getId(), completedJob.getJobId());

    }

    @Test
    public void acceptedToTerminating() {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo1));

        assertEquals(1, jm.acceptedJobsCount());

        assertTrue(jm.markJobTerminating(jInfo1, JobState.Terminating_abnormal));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo1));

        assertEquals(0, jm.acceptedJobsCount());
        assertEquals(0, jm.activeJobsCount());
        Optional<JobInfo> j1 = jm.getJobInfoForNonTerminalJob(jId1);
        assertTrue(j1.isPresent());
        assertEquals(jId1, j1.get().jobId);

    }

    @Test
    public void terminatingToActiveIsIgnored() {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        jm.markJobAccepted(jInfo1);

        assertEquals(1, jm.acceptedJobsCount());

        Optional<JobInfo> jInfo1Op = jm.getJobInfoForNonTerminalJob(jId1);

        assertTrue(jInfo1Op.isPresent());

        assertTrue(jm.markJobTerminating(jInfo1Op.get(), JobState.Terminating_abnormal));

        jInfo1Op = jm.getJobInfoForNonTerminalJob(jId1);
        assertTrue(jInfo1Op.isPresent());
        assertFalse(jm.markJobStarted(jInfo1Op.get()));


    }

    @Test
    public void activeToAcceptedFails() {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertEquals(1, jm.acceptedJobsCount());

        assertTrue(jm.markJobTerminating(jInfo1, JobState.Terminating_abnormal));

        assertFalse(jm.markJobAccepted(jInfo1));


    }

    @Test
    public void testGetAcceptedJobList() {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, null, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        JobId jId2 = new JobId("name", 2);
        JobInfo jInfo2 = new JobInfo(jId2, null, 0, null, JobState.Accepted, "nj");
        assertTrue(jm.markJobAccepted(jInfo2));

        List<JobInfo> acceptedJobList = jm.getAcceptedJobsList();

        assertEquals(2, acceptedJobList.size());

        assertTrue(
            jId1.equals(acceptedJobList.get(0).jobId) || jId1.equals(acceptedJobList.get(1).jobId));

        assertTrue(
            jId2.equals(acceptedJobList.get(0).jobId) || jId2.equals(acceptedJobList.get(1).jobId));

        try {
            acceptedJobList.remove(0);
            fail();
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetActiveJobList() {
        JobClusterActor.JobManager jm = new JobManager("name", context, schedulerFactory, publisher,
            jobStore, costsCalculator);
        JobId jId1 = new JobId("name", 1);
        JobInfo jInfo1 = new JobInfo(jId1, null, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));
        assertTrue(jm.markJobStarted(jInfo1));

        JobId jId2 = new JobId("name", 2);
        JobInfo jInfo2 = new JobInfo(jId2, null, 0, null, JobState.Accepted, "nj");
        assertTrue(jm.markJobAccepted(jInfo2));
        assertTrue(jm.markJobStarted(jInfo2));
        List<JobInfo> acceptedJobList = jm.getAcceptedJobsList();

        assertEquals(0, acceptedJobList.size());

        List<JobInfo> activeJobList = jm.getActiveJobsList();
        assertEquals(2, jm.getActiveJobsList().size());

        assertTrue(
            jId1.equals(activeJobList.get(0).jobId) || jId1.equals(activeJobList.get(1).jobId));

        assertTrue(
            jId2.equals(activeJobList.get(0).jobId) || jId2.equals(activeJobList.get(1).jobId));

        try {
            activeJobList.remove(0);
            fail();
        } catch (Exception e) {

        }
    }

    @Test
    public void testPurgeOldJobs() throws IOException {
        final String clusterName = "testPurgeOldJobs";
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        JobClusterActor.JobManager jm = new JobManager(clusterName, context, schedulerFactory,
            publisher, jobStoreMock, costsCalculator);

        JobId jId1 = new JobId(clusterName, 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo1));

        JobId jId2 = new JobId(clusterName, 2);
        JobInfo jInfo2 = new JobInfo(jId2, jobDefinition, 1, null, JobState.Accepted, "nj");
        assertTrue(jm.markJobAccepted(jInfo2));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo2));

        assertTrue(jm.getAllNonTerminalJobsList().size() == 2);

        assertEquals(jInfo1, jm.getAllNonTerminalJobsList().get(1));

        assertEquals(jInfo2, jm.getAllNonTerminalJobsList().get(0));

        jm.markJobTerminating(jInfo1, JobState.Terminating_abnormal);

        Instant completionInstant = Instant.now().minusSeconds(5);
        jm.markCompleted(jId1, completionInstant.toEpochMilli(), JobState.Completed);

        assertEquals(1, jm.getCompletedJobsList(100, null).size());
        assertEquals(jId1.getId(), jm.getCompletedJobsList(100, null).get(0).getJobId());

        List<CompletedJob> completedJobs = jm.getCompletedJobsList(100, null);
        when(jobStoreMock.loadCompletedJobsForCluster(clusterName, 100, null))
            .thenReturn(completedJobs);
        jm.onJobClusterDeletion();
        assertEquals(0, jm.getCompletedJobsList(100, null).size());

        try {
            verify(jobStoreMock, times(1)).deleteCompletedJobsForCluster(clusterName);
            verify(jobStoreMock, times(1)).deleteJob(jId1.getId());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }


    }

    @Test
    public void testJobListSortedCorrectly() throws IOException {
        String clusterName = "testJobListSortedCorrectly";
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        JobClusterActor.JobManager jm = new JobManager(clusterName, context, schedulerFactory,
            publisher, jobStoreMock, costsCalculator);

        JobId jId1 = new JobId(clusterName, 1);
        JobInfo jInfo1 = new JobInfo(jId1, jobDefinition, 0, null, JobState.Accepted, "nj");

        assertTrue(jm.markJobAccepted(jInfo1));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo1));

        JobId jId2 = new JobId(clusterName, 2);
        JobInfo jInfo2 = new JobInfo(jId2, jobDefinition, 1, null, JobState.Accepted, "nj");
        assertTrue(jm.markJobAccepted(jInfo2));

        assertTrue(jm.getAllNonTerminalJobsList().contains(jInfo2));

        assertTrue(jm.getAllNonTerminalJobsList().size() == 2);

        assertEquals(jInfo1, jm.getAllNonTerminalJobsList().get(1));

        assertEquals(jInfo2, jm.getAllNonTerminalJobsList().get(0));

        jm.markJobTerminating(jInfo1, JobState.Terminating_abnormal);

        Instant completionInstant = Instant.now().minusSeconds(5);
        jm.markCompleted(jId1, completionInstant.toEpochMilli(), JobState.Completed);

        assertEquals(1, jm.getCompletedJobsList(100, null).size());
        assertEquals(jId1.getId(), jm.getCompletedJobsList(100, null).get(0).getJobId());

        jm.markJobTerminating(jInfo1, JobState.Terminating_abnormal);

        completionInstant = Instant.now().minusSeconds(2);
        jm.markCompleted(jId2, completionInstant.toEpochMilli(), JobState.Completed);

        assertEquals(2, jm.getCompletedJobsList(100, null).size());
        assertEquals(jId2.getId(), jm.getCompletedJobsList(100, null).get(0).getJobId());
        assertEquals(jId1.getId(), jm.getCompletedJobsList(100, null).get(1).getJobId());


    }


}
