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

import static org.junit.Assert.*;

import io.mantisrx.common.Label;
import io.mantisrx.server.master.domain.JobId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class LabelCacheTest {

    @Test
    public void addLabelTest() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        List<Label> labelList = new ArrayList<>();
        Label label1 = new Label("l1","v1");
        labelList.add(label1);
        labelCache.addJobIdToLabelCache(jId,labelList);

        assertTrue(labelCache.labelJobIdMap.containsKey(label1));

        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId));
    }

    @Test
    public void addLabelTest2() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        JobId jId2 = new JobId("addLabelTest",2);
        List<Label> labelList = new ArrayList<>();
        Label label1 = new Label("l1","v1");
        labelList.add(label1);
        labelCache.addJobIdToLabelCache(jId,labelList);
        labelCache.addJobIdToLabelCache(jId2,labelList);

        assertTrue(labelCache.labelJobIdMap.containsKey(label1));

        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId));
        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId2));
    }

    @Test
    public void removeLabelTest() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        List<Label> labelList = new ArrayList<>();
        Label label1 = new Label("l1","v1");
        labelList.add(label1);
        labelCache.addJobIdToLabelCache(jId,labelList);

        assertTrue(labelCache.labelJobIdMap.containsKey(label1));

        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId));


        labelCache.removeJobIdFromLabelCache(jId);

        assertFalse(labelCache.jobIdToLabelMap.containsKey(jId));
        // label has no jobs associated with it remove label entry
        assertFalse(labelCache.labelJobIdMap.containsKey(label1));
    }

    @Test
    public void removeLabelTest2() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        JobId jId2 = new JobId("addLabelTest",2);
        List<Label> labelList = new ArrayList<>();
        Label label1 = new Label("l1","v1");
        labelList.add(label1);
        labelCache.addJobIdToLabelCache(jId,labelList);
        labelCache.addJobIdToLabelCache(jId2,labelList);

        assertTrue(labelCache.labelJobIdMap.containsKey(label1));

        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId));
        assertTrue(labelCache.jobIdToLabelMap.containsKey(jId2));

        labelCache.removeJobIdFromLabelCache(jId);

        assertFalse(labelCache.jobIdToLabelMap.containsKey(jId));
        // label still has 1 job associated with it label entry should still exist
        assertTrue(labelCache.labelJobIdMap.containsKey(label1));

    }

    @Test
    public void matchingLabelsAndTest() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        JobId jId2 = new JobId("addLabelTest",2);
        JobId jId3 = new JobId("addLabelTest",3);
        JobId jId4 = new JobId("addLabelTest",4);

        List<Label> sourceList = new ArrayList<>();

        List<Label> sourceMREList = new ArrayList<>();
        List<Label> sourceKafkaList = new ArrayList<>();


        Label jobTypeLabel = new Label("_mantis.jobType","source");
        Label originMRELabel = new Label("_mantis.dataOrigin","mre");
        Label originKafkaLabel = new Label("_mantis.dataOrigin","kafka");

        sourceMREList.add(jobTypeLabel);
        sourceMREList.add(originMRELabel);

        sourceKafkaList.add(jobTypeLabel);
        sourceKafkaList.add(originKafkaLabel);

        sourceList.add(jobTypeLabel);

        labelCache.addJobIdToLabelCache(jId,sourceMREList);
        labelCache.addJobIdToLabelCache(jId2,sourceMREList);
        labelCache.addJobIdToLabelCache(jId3,sourceMREList);

        Set<JobId> jobIdsMatchingLabels = labelCache.getJobIdsMatchingLabels(sourceKafkaList, true);

        System.out.println("matchset " + jobIdsMatchingLabels);
        assertEquals(0, jobIdsMatchingLabels.size());

        labelCache.addJobIdToLabelCache(jId4,sourceKafkaList);

        jobIdsMatchingLabels = labelCache.getJobIdsMatchingLabels(sourceKafkaList, true);

        System.out.println("matchset " + jobIdsMatchingLabels);
        assertEquals(1, jobIdsMatchingLabels.size());

        assertTrue( jobIdsMatchingLabels.contains(jId4));

        jobIdsMatchingLabels = labelCache.getJobIdsMatchingLabels(sourceList, true);

        System.out.println("matchset " + jobIdsMatchingLabels);
        assertEquals(4, jobIdsMatchingLabels.size());

        //assertTrue( jobIdsMatchingLabels.contains(jId4));

    }

    @Test
    public void matchingLabelsOrTest() {
        JobClusterActor.LabelCache labelCache = new JobClusterActor.LabelCache();
        JobId jId = new JobId("addLabelTest",1);
        JobId jId2 = new JobId("addLabelTest",2);
        List<Label> labelList1 = new ArrayList<>();
        List<Label> labelList2 = new ArrayList<>();

        Label label1 = new Label("l1","v1");
        Label label2 = new Label("l2","v2");

        labelList1.add(label1);


        labelList2.add(label2);


        labelCache.addJobIdToLabelCache(jId,labelList1);
        labelCache.addJobIdToLabelCache(jId2,labelList2);

        List<Label> labelListAll = new ArrayList<>();
        labelListAll.addAll(labelList1);
        labelListAll.addAll(labelList2);

        Set<JobId> jobIdsMatchingLabels = labelCache.getJobIdsMatchingLabels(labelListAll, false);

        assertEquals(2, jobIdsMatchingLabels.size());
        boolean foundJob1 = false;
        boolean foundJob2 = false;
        for(JobId jobId : jobIdsMatchingLabels) {
            if(jobId.equals(jId)) {
                foundJob1 = true;
            } else if(jobId.equals(jId2)) {
                foundJob2 = true;
            }
        }

        assertTrue(foundJob1 && foundJob2);

    }



}
