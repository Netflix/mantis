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

package com.netflix.mantis.master.scheduler;

import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.core.scheduler.SchedulingConstraints;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.StaticPropertiesConfigurationFactory;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import java.util.Collections;
import java.util.Properties;

public class TestHelpers {
    public static ScheduleRequest createFakeScheduleRequest(final WorkerId workerId,
                                                            final int stageNum,
                                                            final int numStages,
                                                            final MachineDefinition machineDefinition) {
        try {
        	JobDefinition jobDefinition = new JobDefinition.Builder()
                .withJobJarUrl("http://jar")
                .withArtifactName("jar")
                .withSchedulingInfo(new SchedulingInfo(Collections.singletonMap(0,
                    StageSchedulingInfo.builder()
                        .numberOfInstances(1)
                        .machineDefinition(machineDefinition)
                        .hardConstraints(Collections.emptyList()).softConstraints(Collections.emptyList())
                        .build())
                ))
                .withJobSla(new JobSla(0, 0, null, MantisJobDurationType.Perpetual, null))
                .build();

        	IMantisJobMetadata mantisJobMetadata = new MantisJobMetadataImpl.Builder()
                .withJobId(JobId.fromId(workerId.getJobId()).get())
                .withJobDefinition(jobDefinition)
                .build();

            return new ScheduleRequest(
                    workerId,
                    stageNum,
                    new JobMetadata(mantisJobMetadata.getJobId().getId(),
                            mantisJobMetadata.getJobJarUrl(),
                            mantisJobMetadata.getJobDefinition().getVersion(),
                            mantisJobMetadata.getTotalStages(),
                            mantisJobMetadata.getUser(),
                            mantisJobMetadata.getSchedulingInfo(),
                            mantisJobMetadata.getParameters(),
                            mantisJobMetadata.getSubscriptionTimeoutSecs(),
                            0,
                            mantisJobMetadata.getMinRuntimeSecs()
                    ),
                    mantisJobMetadata.getSla().get().getDurationType(),
                    SchedulingConstraints.of(machineDefinition),
                    0
            );
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void setupMasterConfig() {
        final Properties props = new Properties();

        props.setProperty("mantis.master.consoleport", "8080");
        props.setProperty("mantis.master.apiport", "7070");
        props.setProperty("mantis.master.metrics.port", "7102");
        props.setProperty("mantis.master.apiportv2", "7075");
        props.setProperty("mantis.master.schedInfoPort", "7076");
        props.setProperty("mantis.master.workqueuelength", "100");
        props.setProperty("mantis.master.storageProvider", "io.mantisrx.server.master.store.KeyValueStorageProvider.NoopStorageProvider");
        props.setProperty("mantis.master.resourceClusterStorageProvider", "io.mantisrx.master.resourcecluster.resourceprovider.InMemoryOnlyResourceClusterStorageProvider");
        props.setProperty("mantis.master.resourceClusterProvider", "io.mantisrx.master.resourcecluster.resourceprovider.NoopResourceClusterProvider");
        props.setProperty("mantis.master.api.status.path", "api/postjobstatus");
        props.setProperty("mantis.master.mesos.failover.timeout.ms", "1000.0");
        props.setProperty("mantis.worker.executor.name", "Mantis Worker Executor");
        props.setProperty("mantis.localmode", "true");
        props.setProperty("mantis.zookeeper.connectionTimeMs", "1000");
        props.setProperty("mantis.zookeeper.connection.retrySleepMs", "100");
        props.setProperty("mantis.zookeeper.connection.retryCount", "3");
        props.setProperty("mantis.zookeeper.connectString", "ec2-50-19-255-1.compute-1.amazonaws.com:2181,ec2-54-235-159-245.compute-1.amazonaws.com:2181,ec2-50-19-255-97.compute-1.amazonaws.com:2181,ec2-184-73-152-248.compute-1.amazonaws.com:2181,ec2-50-17-247-179.compute-1.amazonaws.com:2181");
        props.setProperty("mantis.zookeeper.root", "/mantis/master");
        props.setProperty("mantis.zookeeper.leader.election.path", "/hosts");
        props.setProperty("mantis.zookeeper.leader.announcement.path", "/leader");
        props.setProperty("mesos.master.location", "127.0.0.1:5050");
        props.setProperty("mesos.worker.executorscript", "startup.sh");
        props.setProperty("mesos.worker.installDir", "/tmp/mantisWorkerInstall");
        props.setProperty("mantis.master.framework.name", "MantisFramework");
        props.setProperty("mesos.worker.timeoutSecondsToReportStart", "5");
        props.setProperty("mesos.lease.offer.expiry.secs", "1");
        props.setProperty("mantis.master.stage.assignment.refresh.interval.ms","-1");
        props.setProperty("mantis.master.api.cache.ttl.milliseconds","0");
        props.setProperty("mantis.scheduler.enable-batch","true");

        ConfigurationProvider.initialize(new StaticPropertiesConfigurationFactory(props));
    }

//    public static MantisSchedulerFenzoImpl createMantisScheduler(final VMResourceManager vmResourceManager,
//                                             final JobMessageRouter jobMessageRouter,
//                                             final WorkerRegistry workerRegistry,
//                                             final AgentClustersAutoScaler agentClustersAutoScaler) {
//        final ClusterResourceMetricReporter metricReporterMock = mock(ClusterResourceMetricReporter.class);
//
//        final SchedulingResultHandler schedulingResultHandler =
//                new SchedulingResultHandler(vmResourceManager, jobMessageRouter, workerRegistry);
//        TestHelpers.setupMasterConfig();
//        final MantisSchedulerFenzoImpl mantisScheduler = new MantisSchedulerFenzoImpl(vmResourceManager,
//                schedulingResultHandler,
//                metricReporterMock,
//                agentClustersAutoScaler,
//                workerRegistry);
//        mantisScheduler.start();
//        return mantisScheduler;
//    }
}
