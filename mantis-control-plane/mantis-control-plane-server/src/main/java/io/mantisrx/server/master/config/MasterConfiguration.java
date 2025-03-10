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

package io.mantisrx.server.master.config;

import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.resourcecluster.AvailableTaskExecutorMutatorHook;
import io.mantisrx.master.scheduler.FitnessCalculator;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.shaded.com.google.common.base.Splitter;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Map;
import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;


public interface MasterConfiguration extends CoreConfiguration {

    @Config("mantis.master.consoleport")
    int getConsolePort();

    @Config("mantis.master.apiport")
    int getApiPort();

    @Config("mantis.master.schedInfoPort")
    int getSchedInfoPort();

    @Config("mantis.master.apiportv2")
    int getApiPortV2();

    @Config("mantis.master.metrics.port")
    int getMasterMetricsPort();

    @Config("mantis.master.api.status.path")
    String getApiStatusUri();

    @Config("mantis.master.storageProvider")
    IKeyValueStore getStorageProvider();

    @Config("mantis.master.resourceClusterProvider")
    String getResourceClusterProvider();

    @Config("mantis.master.host")
    @DefaultNull
    String getMasterHost();

    @Config("mantis.master.ip")
    @DefaultNull
    String getMasterIP();

    @Config("mantis.worker.timeoutSecondsToReportStart")
    @Default("10")
    int getTimeoutSecondsToReportStart();

    @Config("mantis.master.leader.mismatch.retry.count")
    @Default("5")
    int getMasterLeaderMismatchRetryCount();

    @Config("master.shutdown.curator.service.enabled")
    @Default("true")
    boolean getShutdownCuratorServiceEnabled();

    @Config("mantis.leader.elector.factory")
    @Default("io.mantisrx.server.core.master.LocalLeaderFactory")
    String getLeaderElectorFactory();

    @Config("mantis.master.api.route.ask.timeout.millis")
    @Default("1000")
    long getMasterApiAskTimeoutMs();

    @Config("mantis.master.api.route.ask.longOperation.timeout.millis")
    @Default("2500")
    long getMasterApiLongOperationAskTimeoutMs();

    @Config("mantis.worker.machine.definition.maxCpuCores")
    @Default("8")
    int getWorkerMachineDefinitionMaxCpuCores();

    @Config("mantis.worker.machine.definition.maxMemoryMB")
    @Default("28000")
    int getWorkerMachineDefinitionMaxMemoryMB();

    @Config("mantis.worker.machine.definition.maxNetworkMbps")
    @Default("1024")
    int getWorkerMachineDefinitionMaxNetworkMbps();

    @Config("mantis.master.max.workers.per.stage")
    @Default("1500")
    int getMaxWorkersPerStage();

    @Config("mantis.master.worker.jvm.memory.scale.back.percent")
    @Default("10")
    int getWorkerJvmMemoryScaleBackPercentage();

    @Config("mantis.master.active.slave.attribute.name")
    @Default("NETFLIX_AUTO_SCALE_GROUP")
    String getActiveSlaveAttributeName();

    @Config("mantis.master.slave.cluster.attribute.name")
    @Default("CLUSTER_NAME")
    String getSlaveClusterAttributeName();

    @Config("mantis.master.agent.fitness.cluster.weight")
    @Default("0.2")
    double getPreferredClusterFitnessWeight();

    @Config("mantis.master.agent.fitness.durationtype.weight")
    @Default("0.5")
    double getDurationTypeFitnessWeight();

    @Config("mantis.master.agent.fitness.binpacking.weight")
    @Default("0.3")
    double getBinPackingFitnessWeight();

    // Threshold value compared should make sense with the 3 fitness weights above that aggregates the weighted results from
    // individual fitness calculators.
    @Config("mantis.master.agent.fitness.goodenough.threshold")
    @Default("0.63")
    double getFitnessGoodEnoughThreshold();

    @Config("mantis.master.framework.name")
    @Default("MantisFramework")
    String getMantisFrameworkName();

    @Config("mantis.master.framework.user")
    @Default("")
    String getMantisFrameworkUserName();

    @Config("mantis.worker.executor.name")
    @Default("Mantis Worker Executor")
    String getWorkerExecutorName();

    // Sleep interval between consecutive scheduler iterations
    @Config("mantis.master.scheduler.iteration.interval.millis")
    @Default("50")
    long getSchedulerIterationIntervalMillis();

    @Config("mantis.master.scheduler.disable.slave.duration.secs")
    @Default("60")
    long getDisableSlaveDurationSecs();

    // Sleep interval between consecutive scheduler retries
    @Config("mantis.master.scheduler.retry-interval.millis")
    @Default("60000") // 1 minute
    int getSchedulerIntervalBetweenRetriesInMs();

    @Config("mantis.master.scheduler.fitnessCalculator.class")
    @Default("io.mantisrx.master.scheduler.CpuWeightedFitnessCalculator")
    FitnessCalculator getFitnessCalculator();

    @Config("mantis.master.scheduler.availabletaskexecutormutatorhook.class")
    @Default("io.mantisrx.master.resourcecluster.PassthroughAvailableTaskExecutorMutatorHook")
    AvailableTaskExecutorMutatorHook getAvailableTaskExecutorMutatorHook();

    default Duration getSchedulerIntervalBetweenRetries() {
        return Duration.ofMillis(getSchedulerIntervalBetweenRetriesInMs());
    }

    @Config("mantis.master.scheduler.max-retries")
    @Default("60")
    int getSchedulerMaxRetries();

    @Config("mantis.zookeeper.leader.election.path")
    String getLeaderElectionPath();

    @Config("mantis.worker.heartbeat.intervalv2.secs")
    @Default("20")
    long getDefaultWorkerHeartbeatIntervalSecs();

    //todo: fix the property name, ideally to mantis.worker.timeout.secs
    @Config("mantis.worker.heartbeat.interval.secs")
    @Default("60")
    long getDefaultWorkerTimeoutSecs();

    @Config("mantis.worker.heartbeat.interval.init.secs")
    @Default("180")
    long getWorkerInitTimeoutSecs();

    @Config("mantis.worker.heartbeat.receipts.min.threshold.percent")
    @Default("55")
    double getHeartbeatReceiptsMinThresholdPercentage();

    @Config("mantis.master.stage.assignment.refresh.interval.ms")
    @Default("1000")
    long getStageAssignmentRefreshIntervalMs();

    @Config("mantis.worker.heartbeat.termination.enabled")
    @Default("true")
    boolean isHeartbeatTerminationEnabled();

    @Config("mantis.worker.heartbeat.processing.enabled")
    @Default("true")
    boolean isHeartbeatProcessingEnabled();

    @Config("mantis.interval.move.workers.disabled.vms.millis")
    @Default("60000")
    long getIntervalMoveWorkersOnDisabledVMsMillis();

    @Config("mantis.jobs.max.jars.per.named.job")
    @Default("10")
    int getMaximumNumberOfJarsPerJobName();

    @Config("mantis.worker.resubmissions.maximum")
    @Default("100")
    int getMaximumResubmissionsPerWorker();

    @Config("mantis.worker.resubmission.interval.secs")
    @Default("5:10:20")
    String getWorkerResubmitIntervalSecs();

    @Config("mantis.worker.expire.resubmit.delay.secs")
    @Default("300")
    long getExpireWorkerResubmitDelaySecs();

    @Config("mantis.worker.expire.resubmit.execution.interval.secs")
    @Default("120")
    long getExpireResubmitDelayExecutionIntervalSecs();

    @Config("mantis.master.purge.frequency.secs")
    @Default("1200")
    long getCompletedJobPurgeFrequencySeqs();

    @Config("mantis.master.purge.size")
    @Default("50")
    int getMaxJobsToPurge();


    @Config("mantis.worker.state.launched.timeout.millis")
    @Default("7000")
    long getWorkerInLaunchedStateTimeoutMillis();

    @Config("mantis.master.store.worker.writes.batch.size")
    @Default("100")
    int getWorkerWriteBatchSize();

    @Config("mantis.master.ephemeral.job.unsubscribed.timeout.secs")
    @Default("300")
    long getEphemeralJobUnsubscribedTimeoutSecs();

    @Config("mantis.master.init.timeout.secs")
    @Default("240")
    long getMasterInitTimeoutSecs();

    @Config("mantis.master.terminated.job.to.delete.delay.hours")
    @Default("360")
        // 15 days * 24 hours
    long getTerminatedJobToDeleteDelayHours();

    @Config("mantis.master.max.archived.jobs.to.cache")
    @Default("1000")
    int getMaxArchivedJobsToCache();

    @Config("mantis.agent.cluster.autoscale.by.attribute.name")
    @Default("CLUSTER_NAME")
    String getAutoscaleByAttributeName();

    @Config("mantis.agent.cluster.autoscaler.map.hostname.attribute.name")
    @Default("EC2_INSTANCE_ID")
    String getAutoScalerMapHostnameAttributeName();

    @Config("mantis.agent.cluster.autoscaler.shortfall.evaluation.disabled")
    @Default("false")
    boolean getDisableShortfallEvaluation();

    @Config("mantis.scheduling.info.observable.heartbeat.interval.secs")
    @Default("120")
    long getSchedulingInfoObservableHeartbeatIntervalSecs();

    @Config("mantis.job.master.scheduling.info.cores")
    @Default("2.0")
    double getJobMasterCores();

    @Config("mantis.job.master.scheduling.info.memoryMB")
    @Default("4096.0")
    double getJobMasterMemoryMB();

    @Config("mantis.job.master.scheduling.info.networkMbps")
    @Default("128.0")
    double getJobMasterNetworkMbps();

    @Config("mantis.job.master.scheduling.info.diskMB")
    @Default("100.0")
    double getJobMasterDiskMB();

    @Config("mantis.master.api.cache.ttl.milliseconds")
    @Default("250")
    int getApiCacheTtlMilliseconds();

    @Config("mantis.master.api.cache.size.max")
    @Default("50")
    int getApiCacheMaxSize();

    @Config("mantis.master.api.cache.size.min")
    @Default("5")
    int getApiCacheMinSize();

    @Config("mantis.agent.heartbeat.interval.ms")
    @Default("300000") // 5 minutes
    int getHeartbeatIntervalInMs();

    /**
     * Config value for each {@link io.mantisrx.master.resourcecluster.ResourceClusterScalerActor}'s timer to trigger
     * check on current cluster usage.
     */
    @Config("mantis.job.master.resource.cluster.scaler.interval.secs")
    @Default("60")
    int getScalerTriggerThresholdInSecs();

    /**
     * Config value for each {@link io.mantisrx.master.resourcecluster.ResourceClusterScalerActor}'s timer to refresh
     * its cached scale rules.
     */
    @Config("mantis.job.master.resource.cluster.scaler.ruleset.refresh.secs")
    @Default("180")
    int getScalerRuleSetRefreshThresholdInSecs();

    @Config("mantis.agent.assignment.interval.ms")
    @Default("60000") // 1 minute
    int getAssignmentIntervalInMs();

    @Config("mantis.agent.assignment.scheduler.lease.ms")
    @Default("100")
    int getSchedulerLeaseExpirationDurationInMs();

    @Config("mantis.job.costsCalculator.class")
    @Default("io.mantisrx.master.jobcluster.job.NoopCostsCalculator")
    CostsCalculator getJobCostsCalculator();

    @Config("mantis.job.worker.max.artifacts.to.cache")
    @Default("5")
    int getMaxJobArtifactsToCache();

    @Config("mantis.artifactCaching.jobClusters")
    @Default("")
    String getJobClustersWithArtifactCachingEnabled();

    @Config("mantis.artifactCaching.enabled")
    @Default("true")
    boolean isJobArtifactCachingEnabled();

    // rate limit actions on resource cluster actor to control backlog.
    @Config("mantis.master.resource.cluster.actions.permitsPerSecond")
    @Default("5000")
    int getResourceClusterActionsPermitsPerSecond();

    @Config("mantis.scheduler.enable-batch")
    @Default("false")
    boolean isBatchSchedulingEnabled();

    // Example: "jdk:17"
    @Config("mantis.scheduler.schedulingConstraints")
    @Default("")
    String getSchedulingConstraintsString();

    @Config("mantis.sla.headroomForAcceptedJobs")
    @Default("3")
    int getSlaMaxHeadroomForAccepted();

    @Config("mantis.scheduler.handlesAllocationRetries")
    @Default("true")
    boolean getSchedulerHandlesAllocationRetries();

    default Duration getHeartbeatInterval() {
        return Duration.ofMillis(getHeartbeatIntervalInMs());
    }

    default Duration getMaxAssignmentThreshold() {
        return Duration.ofMillis(getAssignmentIntervalInMs());
    }

    default Map<String, String> getSchedulingConstraints() { return getSchedulingConstraintsString().isEmpty() ? ImmutableMap.of() : Splitter.on(",").withKeyValueSeparator(':').split(getSchedulingConstraintsString());}
}
