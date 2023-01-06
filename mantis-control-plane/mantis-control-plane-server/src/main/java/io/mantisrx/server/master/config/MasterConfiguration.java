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

import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.master.store.KeyValueStore;
import java.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;


public interface MasterConfiguration extends CoreConfiguration {

    @Config("mantis.master.storageProvider")
    KeyValueStore getStorageProvider();

    @Config("mantis.master.resourceClusterStorageProvider")
    ResourceClusterStorageProvider getResourceClusterStorageProvider();

    @Config("mantis.master.resourceClusterProvider")
    String getResourceClusterProvider();


    // ------------------------------------------------------------------------
    //  Apache Mesos related configurations
    // ------------------------------------------------------------------------


    @Config("mantis.master.active.slave.attribute.name")
    @Default("NETFLIX_AUTO_SCALE_GROUP")
    String getActiveSlaveAttributeName();

    @Config("mantis.master.slave.cluster.attribute.name")
    @Default("CLUSTER_NAME")
    String getSlaveClusterAttributeName();

    @Config("mantis.master.mesos.failover.timeout.secs")
    @Default("604800.0")
        // 604800 secs = 1 week
    double getMesosFailoverTimeOutSecs();

    // Sleep interval between consecutive scheduler iterations
    @Config("mantis.master.scheduler.iteration.interval.millis")
    @Default("50")
    long getSchedulerIterationIntervalMillis();

    // Sleep interval between consecutive scheduler retries
    @Config("mantis.master.scheduler.retry-interval.millis")
    @Default("60000") // 1 minute
    int getSchedulerIntervalBetweenRetriesInMs();

    default Duration getSchedulerIntervalBetweenRetries() {
        return Duration.ofMillis(getSchedulerIntervalBetweenRetriesInMs());
    }

    @Config("mantis.master.scheduler.max-retries")
    @Default("10")
    int getSchedulerMaxRetries();

    @Config("mantis.worker.heartbeat.interval.secs")
    @Default("60")
    long getWorkerTimeoutSecs();

    @Config("mantis.worker.heartbeat.interval.init.secs")
    @Default("180")
    long getWorkerInitTimeoutSecs();

    @Config("mantis.worker.heartbeat.receipts.min.threshold.percent")
    @Default("55")
    double getHeartbeatReceiptsMinThresholdPercentage();

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

    @Config("mantis.master.purge.frequency.secs")
    @Default("1200")
    long getCompletedJobPurgeFrequencySeqs();

    @Config("mantis.master.purge.size")
    @Default("50")
    int getMaxJobsToPurge();


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

    @Config("mantis.master.api.cache.ttl.milliseconds")
    @Default("250")
    int getApiCacheTtlMilliseconds();

    @Config("mantis.master.api.cache.size.max")
    @Default("50")
    int getApiCacheMaxSize();

    @Config("mantis.master.api.cache.size.min")
    @Default("5")
    int getApiCacheMinSize();

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

}
