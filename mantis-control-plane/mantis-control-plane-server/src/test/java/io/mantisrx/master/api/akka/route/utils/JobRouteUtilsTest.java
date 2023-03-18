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

package io.mantisrx.master.api.akka.route.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class JobRouteUtilsTest {
    @Test
    public void testListJobRequest() {
        Map<String, List<String>> params = new HashMap<>();
        params.put(JobRouteUtils.QUERY_PARAM_LIMIT, Arrays.asList("10"));
        params.put(JobRouteUtils.QUERY_PARAM_JOB_STATE, Arrays.asList("Active"));
        params.put(JobRouteUtils.QUERY_PARAM_STAGE_NUM, Arrays.asList("1"));
        params.put(JobRouteUtils.QUERY_PARAM_WORKER_INDEX, Arrays.asList("11"));
        params.put(JobRouteUtils.QUERY_PARAM_WORKER_NUM, Arrays.asList("233"));
        params.put(JobRouteUtils.QUERY_PARAM_WORKER_STATE, Arrays.asList("Terminal"));
        params.put(JobRouteUtils.QUERY_PARAM_ACTIVE_ONLY, Arrays.asList("False"));
        params.put(JobRouteUtils.QUERY_PARAM_LABELS_QUERY, Arrays.asList("lab1=v1,lab3=v3"));
        params.put(JobRouteUtils.QUERY_PARAM_LABELS_OPERAND, Arrays.asList("and"));

        JobClusterManagerProto.ListJobsRequest listJobsRequest = JobRouteUtils.createListJobsRequest(params, Optional.of(".*abc.*"), true);
        assertEquals(10, listJobsRequest.getCriteria().getLimit().get().intValue());
        assertEquals(JobState.MetaState.Active, listJobsRequest.getCriteria().getJobState().get());
        assertEquals(1, listJobsRequest.getCriteria().getStageNumberList().get(0).intValue());
        assertEquals(11, listJobsRequest.getCriteria().getWorkerIndexList().get(0).intValue());
        assertEquals(233, listJobsRequest.getCriteria().getWorkerNumberList().get(0).intValue());
        assertEquals(1, listJobsRequest.getCriteria().getWorkerStateList().size());
        assertEquals(WorkerState.MetaState.Terminal, listJobsRequest.getCriteria().getWorkerStateList().get(0));
        assertEquals(false, listJobsRequest.getCriteria().getActiveOnly().get());
        assertEquals(2, listJobsRequest.getCriteria().getMatchingLabels().size());
        assertEquals("lab1", listJobsRequest.getCriteria().getMatchingLabels().get(0).getName());
        assertEquals("v1", listJobsRequest.getCriteria().getMatchingLabels().get(0).getValue());
        assertEquals("lab3", listJobsRequest.getCriteria().getMatchingLabels().get(1).getName());
        assertEquals("v3", listJobsRequest.getCriteria().getMatchingLabels().get(1).getValue());
        assertEquals("and", listJobsRequest.getCriteria().getLabelsOperand().get());
        assertEquals(".*abc.*", listJobsRequest.getCriteria().getMatchingRegex().get());
    }

    @Test
    public void testListJobRequestDefaults() {
        JobClusterManagerProto.ListJobsRequest listJobsRequest2 = JobRouteUtils.createListJobsRequest(new HashMap<>(), Optional.empty(), true);
        assertEquals(false, listJobsRequest2.getCriteria().getLimit().isPresent());
        assertEquals(false, listJobsRequest2.getCriteria().getJobState().isPresent());
        assertEquals(0, listJobsRequest2.getCriteria().getStageNumberList().size());
        assertEquals(0, listJobsRequest2.getCriteria().getWorkerIndexList().size());
        assertEquals(0, listJobsRequest2.getCriteria().getWorkerNumberList().size());
        assertEquals(0, listJobsRequest2.getCriteria().getWorkerStateList().size());
        assertEquals(true, listJobsRequest2.getCriteria().getActiveOnly().get());
        assertEquals(0, listJobsRequest2.getCriteria().getMatchingLabels().size());
        assertEquals(false, listJobsRequest2.getCriteria().getLabelsOperand().isPresent());
        assertEquals(false, listJobsRequest2.getCriteria().getMatchingRegex().isPresent());
    }
}
