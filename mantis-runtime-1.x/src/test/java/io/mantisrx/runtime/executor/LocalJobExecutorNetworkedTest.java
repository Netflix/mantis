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

package io.mantisrx.runtime.executor;

import java.util.Iterator;

import io.mantisrx.runtime.MachineDefinitions;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import junit.framework.Assert;
import org.junit.Test;


public class LocalJobExecutorNetworkedTest {

    @Test
    public void testSingleStage() {

        TestJobSingleStage provider = new TestJobSingleStage();
        LocalJobExecutorNetworked.execute(provider.getJobInstance());

        Iterator<Integer> iter = provider.getItemsWritten().iterator();

        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        Assert.assertEquals(10, count);
    }

    @Test
    public void testSingleStageMultiWorker() {

        TestJobSingleStage provider = new TestJobSingleStage();
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
                .numberOfStages(1)
                .multiWorkerStage(2, MachineDefinitions.micro())
                .build();

        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        Assert.assertEquals(20, count);

    }

    @Test
    public void testTwoStage() {
        TestJob provider = new TestJob();
        LocalJobExecutorNetworked.execute(provider.getJobInstance());

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());
        Assert.assertEquals(16, iter.next().intValue());
    }

    @Test
    public void testThreeStage() {
        TestJobThreeStage provider = new TestJobThreeStage(); // 1 instance per stage
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
                .numberOfStages(3)
                .singleWorkerStage(MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .build();
        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(16, iter.next().intValue());
        Assert.assertEquals(256, iter.next().intValue());
    }

    @Test
    public void testThreeStageTopology1_2_1() {
        TestJobThreeStage provider = new TestJobThreeStage(); // 1,2,1 topology
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
                .numberOfStages(3)
                .singleWorkerStage(MachineDefinitions.micro())
                .multiWorkerStage(2, MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .build();
        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(16, iter.next().intValue());
        Assert.assertEquals(256, iter.next().intValue());
    }

    @Test
    public void testThreeStageTopology2_1_1() {
        TestJobThreeStage provider = new TestJobThreeStage(); // 2,1,1 topology
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
                .numberOfStages(3)
                .multiWorkerStage(2, MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .build();
        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        // with two source instances, should have double the expected
        // input
        Assert.assertEquals(10, provider.getItemsWritten().size());
    }

    @Test
    public void testThreeStageTopology2_2_1() {
        // Note, fails due to onComplete timing issue
        TestJobThreeStage provider = new TestJobThreeStage(); // 2,1,1 topology
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
                .numberOfStages(3)
                .multiWorkerStage(2, MachineDefinitions.micro())
                .multiWorkerStage(2, MachineDefinitions.micro())
                .singleWorkerStage(MachineDefinitions.micro())
                .build();
        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        // with two source instances, should have double the expected
        // input
        Assert.assertEquals(10, provider.getItemsWritten().size());
    }
}
