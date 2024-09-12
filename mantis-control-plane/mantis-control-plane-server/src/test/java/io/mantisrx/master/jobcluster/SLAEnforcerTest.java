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

import io.mantisrx.master.jobcluster.JobClusterActor.JobInfo;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.shaded.com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.joda.time.Instant;
import org.junit.Test;

import static org.junit.Assert.*;

public class SLAEnforcerTest {

    @Test
	public void testSorting() {
        Instant now = Instant.now();
        List<JobInfo> jobList = Lists.newArrayList(
                new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
                new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
                new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null),
                new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null)
        );

        int min =1;
        int max =1;
        SLA sla = new SLA(min,max, null, null);
        SLAEnforcer slaEnforcer = new SLAEnforcer(sla);

        Set<JobInfo> sortJobsByIdDesc = slaEnforcer.sortJobsByIdDesc(jobList);
        String [] expectedOrder = {"cname-1","cname-2","cname-3","cname-4"};
        JobInfo [] jobIdArray = sortJobsByIdDesc.toArray(new JobInfo[sortJobsByIdDesc.size()]);

        for(int i=0; i< jobIdArray.length; i++) {
            System.out.println("[" + i + "] ->" + jobIdArray[i]);
            assertEquals(expectedOrder[i],(jobIdArray[i].jobId.getId()));
        }
    }

	@Test
	public void slaValidationTest() {
		int min = 5;
		int max = 2;
		try {
			SLA sla = new SLA(5, 2, null, null);
			fail();
		} catch(Exception e) {}
	}
	@Test
	public void slaMinInvalidArgTest() {
		int min = 2;
		int max = 0;

		try {
		    SLA sla = new SLA(min,max, null, null);

		    SLAEnforcer slaEnf = new SLAEnforcer(sla);
			slaEnf.enforceSLAMin(-1, 0);
			fail();
		} catch(Exception e) {}


	}
	@Test
	public void slaMinDefaultsTest() {
		SLA sla = new SLA(0,0, null, null);
		SLAEnforcer slaEnf = new SLAEnforcer(sla);
		assertEquals(0, slaEnf.enforceSLAMin(2, 0));
		try {
			slaEnf = new SLAEnforcer(null);
			assertEquals(0, slaEnf.enforceSLAMin(2, 0));
		} catch(Exception e) {
			fail();
		}
	}

	@Test
	public void slaMinTest() {



		int min = 2;
		int max = 10;

		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(sla);
		// min is 2 and active jobs count is 2 no need to launch any jobs
		assertEquals(0, slaEnf.enforceSLAMin(2, 0));
		// min is 2 and active jobs is 1 and launched jobs is 1 no need to launch any more jobs
		assertEquals(0, slaEnf.enforceSLAMin(1, 1));

		// min is 2, active = 1, launched = 0, therefore launch 1 job
		assertEquals(1, slaEnf.enforceSLAMin(1, 0));

	}
	@Test
	public void slaMaxDefaultsTest() {
		Instant now = Instant.now();

		int min = 0;
		int max = 0;
		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(null);

		List<JobInfo> jobList = Lists.newArrayList(
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null)
				);
	// sla not set nothing to enforce
		try {
			List<JobId> jobsToDelete = slaEnf.enforceSLAMax(jobList);
			assertTrue(jobsToDelete.isEmpty());
		} catch(Exception e) {
			fail();
		}

		slaEnf = new SLAEnforcer(sla);

		jobList = Lists.newArrayList(
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null)
				);
	// sla max is 0 nothing to enforce
		List<JobId>  jobsToDelete = slaEnf.enforceSLAMax(jobList);
		assertTrue(jobsToDelete.isEmpty());



	}

	@Test
	public void slaMaxTest() {
		Instant now = Instant.now();

		int min = 0;
		int max = 2;
		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(sla);

		List<JobInfo> jobList = Lists.newArrayList(
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null)
				);
		// 2 active and 2 accepted jobs, sla met at job id 2, hence delete job 1
		List<JobId> jobsToDelete = slaEnf.enforceSLAMax(jobList);
		assertEquals(1, jobsToDelete.size());
		assertEquals("cname-1", jobsToDelete.get(0).getId());


	}

	@Test
	public void slaMaxTest2() {
		Instant now = Instant.now();

		int min = 0;
		int max = 2;
		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(sla);

		List<JobInfo> jobList = Lists.newArrayList(
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null)
				);
		// 3 active and 1 accepted jobs, terminate job 2
		List<JobId> jobsToDelete = slaEnf.enforceSLAMax(jobList);
		assertEquals(2, jobsToDelete.size());

		boolean job1Found = false;
		boolean job2Found = false;

		for(JobId jId : jobsToDelete) {
		    if(jId.getId().equals("cname-1")) {
		        job1Found = true;
            } else if(jId.getId().equals("cname-2")) {
		        job2Found = true;
            }
        }

		assertTrue(job1Found && job2Found);

	}

	@Test
	public void slaMaxTest3() {
		Instant now = Instant.now();

		int min = 0;
		int max = 2;
		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(sla);

		List<JobInfo> jobList = Lists.newArrayList(
                                        new JobInfo(new JobId("cname", 5), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
                                        new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 6), null, now.getMillis(), null, JobState.Launched, null)
				);
		// 2 active and 4 accepted jobs, terminate jobs 3,2,1
		List<JobId> jobsToDelete = slaEnf.enforceSLAMax(jobList);
		assertEquals(3, jobsToDelete.size());
		assertTrue(jobsToDelete.contains(new JobId("cname",1)));
		assertTrue(jobsToDelete.contains(new JobId("cname",2)));
        assertTrue(jobsToDelete.contains(new JobId("cname",3)));
	}

	@Test
	public void slaMaxTest4() {
		Instant now = Instant.now();

		int min = 0;
		int max = 2;
		SLA sla = new SLA(min,max, null, null);

		SLAEnforcer slaEnf = new SLAEnforcer(sla);

		List<JobInfo> jobList = Lists.newArrayList(
                                        new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Accepted, null),
                                        new JobInfo(new JobId("cname", 6), null, now.getMillis(), null, JobState.Launched, null),
										new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 5), null, now.getMillis(), null, JobState.Accepted, null),
										new JobInfo(new JobId("cname", 7), null, now.getMillis(), null, JobState.Launched, null)
				);
		// 3 active and 4 accepted jobs, terminate jobs 1 & 2 & 3 & 4 & 5
		List<JobId> jobsToDelete = slaEnf.enforceSLAMax(jobList);
		assertEquals(5, jobsToDelete.size());
		assertTrue(jobsToDelete.contains(new JobId("cname",1)));
		assertTrue(jobsToDelete.contains(new JobId("cname",2)));
        assertTrue(jobsToDelete.contains(new JobId("cname",3)));
		assertTrue(jobsToDelete.contains(new JobId("cname",4)));
        assertTrue(jobsToDelete.contains(new JobId("cname",5)));
	}

    @Test
    public void slaMaxShouldLimitAcceptedJobsToOneIfEnabled() {
        Instant now = Instant.now();
        int min = 0;
        int max = 2;
        SLA sla = new SLA(min,max, null, null);

        SLAEnforcer slaEnf = new SLAEnforcer(sla);

        List<JobInfo> jobList = Lists.newArrayList(
            new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Launched, null),
            new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Launched, null),
            new JobInfo(new JobId("cname", 5), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 6), null, now.getMillis(), null, JobState.Accepted, null)
        );

        // We expect the two launched jobs and the latest accepted job to remain
        Set<JobId> jobsToDelete = new HashSet<>(slaEnf.enforceSLAMax(jobList, 1));
        assertTrue(jobsToDelete.contains(jobList.get(0).jobId));
        assertTrue(jobsToDelete.contains(jobList.get(2).jobId));
        assertTrue(jobsToDelete.contains(jobList.get(4).jobId));

        assertFalse(jobsToDelete.contains(jobList.get(1).jobId));
        assertFalse(jobsToDelete.contains(jobList.get(3).jobId));
        assertFalse(jobsToDelete.contains(jobList.get(5).jobId));
    }

    @Test
    public void sla11ShouldNotLetJobsPileUpIfEnabled() {
        Instant now = Instant.now();
        int min = 1;
        int max = 1;
        SLA sla = new SLA(min,max, null, null);

        SLAEnforcer slaEnf = new SLAEnforcer(sla);

        List<JobInfo> jobList = Lists.newArrayList(
            new JobInfo(new JobId("cname", 1), null, now.getMillis(), null, JobState.Launched, null),
            new JobInfo(new JobId("cname", 2), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 3), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 4), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 5), null, now.getMillis(), null, JobState.Accepted, null),
            new JobInfo(new JobId("cname", 6), null, now.getMillis(), null, JobState.Accepted, null)
        );

        // We expect the launched job and the latest accepted job
        Set<JobId> jobsToDelete = new HashSet<>(slaEnf.enforceSLAMax(jobList, 1));
        assertFalse(jobsToDelete.contains(jobList.get(0).jobId));
        assertFalse(jobsToDelete.contains(jobList.get(5).jobId));

        assertTrue(jobsToDelete.contains(jobList.get(1).jobId));
        assertTrue(jobsToDelete.contains(jobList.get(2).jobId));
        assertTrue(jobsToDelete.contains(jobList.get(3).jobId));
        assertTrue(jobsToDelete.contains(jobList.get(4).jobId));
    }


}
