/*
 * Copyright 2023 Netflix, Inc.
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.master.jobcluster.JobClusterActor.LabelCache;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class CompletedJobStoreTest {
    private static final String clusterName = "clusterName";
    private final LabelCache labelCache = new LabelCache();
    private final MantisJobStore jobStore = mock(MantisJobStore.class);

    private final CompletedJob job1 = mock(CompletedJob.class);
    private final IMantisJobMetadata metadata1 = mock(IMantisJobMetadata.class);
    private final CompletedJob job2 = mock(CompletedJob.class);
    private final IMantisJobMetadata metadata2 = mock(IMantisJobMetadata.class);
    private final CompletedJob job3 = mock(CompletedJob.class);
    private final IMantisJobMetadata metadata3 = mock(IMantisJobMetadata.class);
    private final IMantisJobMetadata metadata4 = mock(IMantisJobMetadata.class);

    private final CompletedJobStore completedJobStore =
        new CompletedJobStore(clusterName, labelCache, jobStore, 1);

    @Before
    public void setup() throws IOException {
        when(job1.getJobId()).thenReturn("clusterName-1");
        when(job1.getSubmittedAt()).thenReturn(3L);
        when(job2.getJobId()).thenReturn("clusterName-2");
        when(job2.getSubmittedAt()).thenReturn(2L);
        when(job3.getJobId()).thenReturn("clusterName-3");
        when(job3.getSubmittedAt()).thenReturn(1L);

        when(metadata4.getJobId()).thenReturn(JobId.fromId("clusterName-4").get());
        JobDefinition jobDefinition = mock(JobDefinition.class);
        when(jobDefinition.getVersion()).thenReturn("1.0.0");
        when(metadata4.getJobDefinition()).thenReturn(jobDefinition);
        when(metadata4.getSubmittedAtInstant()).thenReturn(Instant.ofEpochMilli(4L));
        when(metadata4.getEndedAtInstant()).thenReturn(Optional.of(Instant.ofEpochMilli(5L)));
        when(metadata4.getUser()).thenReturn("user");
        when(metadata4.getLabels()).thenReturn(ImmutableList.of());

        when(jobStore.getArchivedJob("clusterName-1"))
            .thenReturn(Optional.of(metadata1));
        when(jobStore.getArchivedJob("clusterName-2"))
            .thenReturn(Optional.of(metadata2));
        when(jobStore.getArchivedJob("clusterName-3"))
            .thenReturn(Optional.of(metadata3));
        when(jobStore.getArchivedJob("clusterName-4"))
            .thenReturn(Optional.of(metadata4));

        when(jobStore.loadCompletedJobsForCluster(clusterName, 1, null))
            .thenReturn(ImmutableList.of(job1));

        when(jobStore.loadCompletedJobsForCluster(clusterName, 1, JobId.fromId("clusterName-1").get()))
            .thenReturn(ImmutableList.of(job2));

        when(jobStore.loadCompletedJobsForCluster(clusterName, 1, JobId.fromId("clusterName-2").get()))
            .thenReturn(ImmutableList.of(job3));
    }

    @Test
    public void testInitializationOfCompletedJobStore() throws IOException {
        completedJobStore.initialize();

        List<CompletedJob> jobs = completedJobStore.getCompletedJobs(1);
        assertEquals(1, jobs.size());
        assertEquals(job1, jobs.get(0));
    }

    @Test
    public void testLazyLoadingOfNewPages() throws IOException {
        completedJobStore.initialize();

        List<CompletedJob> jobs = completedJobStore.getCompletedJobs(1);
        assertEquals(1, jobs.size());
        assertEquals(job1, jobs.get(0));

        jobs = completedJobStore.getCompletedJobs(2);
        assertEquals(2, jobs.size());
        assertEquals(job1, jobs.get(0));
        assertEquals(job2, jobs.get(1));

        jobs = completedJobStore.getCompletedJobs(3);
        assertEquals(3, jobs.size());
        assertEquals(job1, jobs.get(0));
        assertEquals(job2, jobs.get(1));
        assertEquals(job3, jobs.get(2));
    }

    @Test
    public void testWhenJobGetsCompleted() throws IOException {
        completedJobStore.initialize();

        completedJobStore.onJobCompletion(metadata4);
        List<CompletedJob> completedJobs =
            completedJobStore.getCompletedJobs(1);
        assertEquals(1, completedJobs.size());
        assertEquals("clusterName-4", completedJobs.get(0).getJobId());
        assertTrue(completedJobStore.getJobMetadata(JobId.fromId("clusterName-4").get()).isPresent());
    }

    @Test
    public void testWhenJobIsNotThere() throws IOException {
        completedJobStore.initialize();
        assertTrue(completedJobStore.getJobMetadata(JobId.fromId("clusterName-4").get()).isPresent());
    }

    @Test
    public void testJobClusterDeletion() throws IOException {
        completedJobStore.initialize();
        when(jobStore.loadCompletedJobsForCluster(clusterName, 100, null))
            .thenReturn(ImmutableList.of(job1, job2, job3));
        when(jobStore.loadCompletedJobsForCluster(clusterName, 100, JobId.fromId("clusterName-3").get()))
            .thenReturn(ImmutableList.of());
        completedJobStore.onJobClusterDeletion();
    }
}
