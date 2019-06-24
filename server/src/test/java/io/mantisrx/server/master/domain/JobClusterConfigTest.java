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

package io.mantisrx.server.master.domain;

import com.google.common.collect.Lists;

import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;


public class JobClusterConfigTest {
    private static final SchedulingInfo DEFAULT_SCHED_INFO = new SchedulingInfo.Builder().numberOfStages(1).singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2), Lists.newArrayList(), Lists.newArrayList()).build();

    @Test
    public void happyTest() {
        String name = "happyTest";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withArtifactName("myart")

                .withSchedulingInfo(DEFAULT_SCHED_INFO)
                .withVersion("0.0.1")

                .build();
        try {
            final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                    .withJobClusterConfig(clusterConfig)
                    .withName(name)
                    .withUser("nj")
                    .withParameters(Lists.newArrayList())
                    .withIsReadyForJobMaster(true)
                    .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                    .withMigrationConfig(WorkerMigrationConfig.DEFAULT)

                    .build();
        } catch(Exception e) {
            fail();
        }
    }
    @Test(expected = Exception.class)
    public void noSchedInfoFails() {
        String name = "noSchedInfoFails";

        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withArtifactName("myart")

                .withSchedulingInfo(null)
                .withVersion("0.0.1")
                .build();
        final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(name)
                .withParameters(Lists.newArrayList())
                .withUser("nj")
                .withIsReadyForJobMaster(true)
                .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .build();
    }

    @Test(expected = Exception.class)
    public void noArtifactNameFails() {
        String name = "noArtifactNameFails";

        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withArtifactName(null)

                .withSchedulingInfo(DEFAULT_SCHED_INFO)
                .withVersion("0.0.1")
                .build();
        final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(name)
                .withUser("nj")
                .withParameters(Lists.newArrayList())
                .withIsReadyForJobMaster(true)
                .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .build();
    }
    @Test
    public void noVersionAutogenerate() {
        String name = "noArtifactNameFails";

        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withArtifactName("myart")

                .withSchedulingInfo(DEFAULT_SCHED_INFO)
                .build();
        final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(clusterConfig)
                .withName(name)
                .withUser("nj")
                .withParameters(Lists.newArrayList())
                .withIsReadyForJobMaster(true)
                .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)
                .build();

        assertTrue(clusterConfig.getVersion() != null);
    }

    @Test
    public void jobClusterDefnTest() {
        String name = "jobClusterDefnTest";
        JobClusterConfig clusterConfig = new JobClusterConfig.Builder()
                .withArtifactName("myart")

                .withSchedulingInfo(DEFAULT_SCHED_INFO)
                .withVersion("0.0.1")
                .build();
        try {
        // null cluster config is not allowed
            final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                .withJobClusterConfig(null)
                .withName(name)
                .withUser("nj")
                .withParameters(Lists.newArrayList())
                .withIsReadyForJobMaster(true)
                .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                .withMigrationConfig(WorkerMigrationConfig.DEFAULT)

                .build();
            fail();
        } catch(Exception e) {
            
        }
        
        try {
            //  cluster name is not specified
                final JobClusterDefinitionImpl fakeJobCluster = new JobClusterDefinitionImpl.Builder()
                    .withJobClusterConfig(clusterConfig)
                    .withUser("nj")
                    .withIsReadyForJobMaster(true)
                    .withOwner(new JobOwner("Nick", "Mantis", "desc", "nma@netflix.com", "repo"))
                    .withMigrationConfig(WorkerMigrationConfig.DEFAULT)

                    .build();
                fail();
            } catch(Exception e) {
                
            }


    }


}
