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

package io.mantisrx.publish.internal.discovery;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.mantis.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.mantisapi.MantisApiClient;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.mantis.discovery.proto.AppJobClustersMap.DEFAULT_APP_KEY;


public class MantisJobDiscoveryCachingImpl implements MantisJobDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(MantisJobDiscoveryCachingImpl.class);
    private final MantisApiClient mantisApiClient;
    private final MrePublishConfiguration mrePublishConfiguration;
    private final ConcurrentMap<String, AtomicLong> lastFetchTimeMs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Optional<JobDiscoveryInfo>> jobClusterDiscoveryInfoMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AppJobClustersMap> appJobClusterMapping = new ConcurrentHashMap<>();
    private final Counter jobDiscoveryRefreshSuccess;
    private final Counter jobDiscoveryRefreshFailed;
    private final Counter jobClusterMappingRefreshSuccess;
    private final Counter jobClusterMappingRefreshFailed;

    public MantisJobDiscoveryCachingImpl(MrePublishConfiguration mrePublishConfiguration,
                                         Registry registry,
                                         MantisApiClient mantisApiClient) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.mantisApiClient = mantisApiClient;
        this.jobDiscoveryRefreshSuccess = SpectatorUtils.buildAndRegisterCounter(registry, "jobDiscoveryRefreshSuccess");
        this.jobDiscoveryRefreshFailed = SpectatorUtils.buildAndRegisterCounter(registry, "jobDiscoveryRefreshFailed");
        this.jobClusterMappingRefreshSuccess = SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterMappingRefreshSuccess");
        this.jobClusterMappingRefreshFailed = SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterMappingRefreshFailed");
    }


    void refreshDiscoveryInfo(String jobClusterName) {
        CompletableFuture<JobDiscoveryInfo> jobDiscoveryInfoF = mantisApiClient.jobDiscoveryInfo(jobClusterName);
        if (jobClusterDiscoveryInfoMap.containsKey(jobClusterName)) {
            // we retrieved discovery info for this job cluster before, allow this refresh to finish async
            jobDiscoveryInfoF.whenCompleteAsync((jdi, t) -> {
                if (jdi != null) {
                    jobClusterDiscoveryInfoMap.put(jobClusterName, Optional.ofNullable(jdi));
                    jobDiscoveryRefreshSuccess.increment();
                } else {
                    // on failure to refresh job discovery info, continue to serve previous job discovery info
                    logger.info("failed to refresh job discovery info, will serve old job discovery info");
                    jobDiscoveryRefreshFailed.increment();
                }
            });
        } else {
            // we haven't seen discovery info for this job cluster before, block till we have a response
            try {
                // synchronously await Job Discovery info first time we get a job discovery request for a job cluster
                JobDiscoveryInfo jobDiscoveryInfo = jobDiscoveryInfoF.get(1, TimeUnit.SECONDS);
                jobClusterDiscoveryInfoMap.put(jobClusterName, Optional.ofNullable(jobDiscoveryInfo));
                jobDiscoveryRefreshSuccess.increment();
            } catch (InterruptedException e) {
                logger.error("interrupted on job discovery fetch {}", jobClusterName, e);
                jobDiscoveryRefreshFailed.increment();
            } catch (ExecutionException e) {
                jobDiscoveryRefreshFailed.increment();
                if (e.getCause() instanceof NonRetryableException) {
                    logger.error("non retryable exception on job discovery fetch {}, update cache to avoid blocking refresh in future", jobClusterName, e.getCause());
                    jobClusterDiscoveryInfoMap.put(jobClusterName, Optional.empty());
                } else {
                    logger.error("caught exception on job discovery fetch {}", jobClusterName, e.getCause());
                }
            } catch (TimeoutException e) {
                jobDiscoveryRefreshFailed.increment();
                logger.error("timed out on job discovery fetch {}", jobClusterName, e);
            }
        }
    }

    private boolean shouldRefreshWorkers(String jobCluster) {
        lastFetchTimeMs.putIfAbsent(jobCluster, new AtomicLong(0));
        long lastFetchMs = lastFetchTimeMs.get(jobCluster).get();
        return (System.currentTimeMillis() - lastFetchMs) > (mrePublishConfiguration.jobDiscoveryRefreshIntervalSec() * 1000);
    }

    @Override
    public Optional<JobDiscoveryInfo> getCurrentJobWorkers(String jobClusterName) {
        if (shouldRefreshWorkers(jobClusterName)) {
            refreshDiscoveryInfo(jobClusterName);
            lastFetchTimeMs.get(jobClusterName).set(System.currentTimeMillis());
        }
        return jobClusterDiscoveryInfoMap.getOrDefault(jobClusterName, Optional.empty());
    }

    private boolean shouldRefreshJobClusterMapping(String appName) {
        lastFetchTimeMs.putIfAbsent(appName, new AtomicLong(0));
        long lastFetchMs = lastFetchTimeMs.get(appName).get();
        return (System.currentTimeMillis() - lastFetchMs) > (mrePublishConfiguration.jobClusterMappingRefreshIntervalSec() * 1000);
    }


    void refreshJobClusterMapping(String app) {
        CompletableFuture<AppJobClustersMap> jobClusterMappingF = mantisApiClient.getJobClusterMapping(Optional.ofNullable(app));
        AppJobClustersMap cachedMapping = appJobClusterMapping.get(app);
        if (cachedMapping != null) {
            // we retrieved job cluster mapping info for this app before, allow this refresh to finish async
            jobClusterMappingF.whenCompleteAsync((mapping, t) -> {
                if (mapping != null) {
                    long recvTimestamp = mapping.getTimestamp();
                    if (recvTimestamp >= cachedMapping.getTimestamp()) {
                        appJobClusterMapping.put(app, mapping);
                        jobClusterMappingRefreshSuccess.increment();
                    } else {
                        logger.info("ignoring job cluster mapping refresh with older timestamp {} than cached {}", recvTimestamp, cachedMapping.getTimestamp());
                        jobClusterMappingRefreshFailed.increment();
                    }
                } else {
                    // on failure to refresh job discovery info, continue to serve previous job discovery info
                    logger.info("failed to refresh job cluster mapping info, will serve old job cluster mapping");
                    jobClusterMappingRefreshFailed.increment();
                }
            });

        } else {
            // we haven't seen job cluster mapping for this app before, synchronously await job cluster mapping info
            try {
                AppJobClustersMap appJobClustersMap = jobClusterMappingF.get(1, TimeUnit.SECONDS);
                appJobClusterMapping.put(app, appJobClustersMap);
                jobClusterMappingRefreshSuccess.increment();
            } catch (Exception e) {
                logger.error("exception getting job cluster mapping {}", app, e);
                jobClusterMappingRefreshFailed.increment();
            }
        }
    }

    private String appWithFallback(String app) {
        return (app == null) ? DEFAULT_APP_KEY : app;
    }

    @Override
    public Optional<AppJobClustersMap> getJobClusterMappings(String app) {
        String appName = appWithFallback(app);
        if (shouldRefreshJobClusterMapping(appName)) {
            refreshJobClusterMapping(appName);
            lastFetchTimeMs.get(appName).set(System.currentTimeMillis());
        }
        return Optional.ofNullable(appJobClusterMapping.get(appName));
    }
}
