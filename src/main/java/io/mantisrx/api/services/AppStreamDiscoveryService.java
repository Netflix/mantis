/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.services;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.proto.AppDiscoveryMap;
import io.mantisrx.client.MantisClient;
import io.mantisrx.discovery.proto.AppJobClustersMap;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.vavr.control.Either;
import io.vavr.control.Option;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Scheduler;

@Slf4j
public class AppStreamDiscoveryService {

    private final MantisClient mantisClient;
    private final Scheduler scheduler;

    private final AppStreamStore appStreamStore;

    public AppStreamDiscoveryService(
        MantisClient mantisClient,
        Scheduler scheduler,
        AppStreamStore appStreamStore) {
        Preconditions.checkArgument(mantisClient != null);
        Preconditions.checkArgument(appStreamStore != null);
        Preconditions.checkArgument(scheduler != null);
        this.mantisClient = mantisClient;
        this.scheduler = scheduler;
        this.appStreamStore = appStreamStore;

        Counter appJobClusterMappingNullCount = SpectatorUtils.newCounter(
            "appJobClusterMappingNull", "mantisapi");
        Counter appJobClusterMappingRequestCount = SpectatorUtils.newCounter(
            "appJobClusterMappingRequest", "mantisapi", "app", "unknown");
        Counter appJobClusterMappingFailCount = SpectatorUtils.newCounter(
            "appJobClusterMappingFail", "mantisapi");
    }


    public Either<String, AppDiscoveryMap> getAppDiscoveryMap(List<String> appNames) {
        try {

            AppJobClustersMap appJobClusters = getAppJobClustersMap(appNames);

            //
            // Lookup discovery info per stream and build mapping
            //

            AppDiscoveryMap adm = new AppDiscoveryMap(appJobClusters.getVersion(), appJobClusters.getTimestamp());

            for (String app : appJobClusters.getMappings().keySet()) {
                for (String stream : appJobClusters.getMappings().get(app).keySet()) {
                    String jobCluster = appJobClusters.getMappings().get(app).get(stream);
                    Option<JobSchedulingInfo> jobSchedulingInfo = getJobDiscoveryInfo(jobCluster);
                    jobSchedulingInfo.map(jsi -> {
                        adm.addMapping(app, stream, jsi);
                        return jsi;
                    });
                }
            }
            return Either.right(adm);
        } catch (Exception ex) {
            log.error(ex.getMessage());
            return Either.left(ex.getMessage());
        }
    }

    public AppJobClustersMap getAppJobClustersMap(List<String> appNames) throws IOException  {
        return appStreamStore.getJobClusterMappings(appNames);
    }

    private Option<JobSchedulingInfo> getJobDiscoveryInfo(String jobCluster) {
        JobDiscoveryService jdim = JobDiscoveryService.getInstance(mantisClient, scheduler);
        return jdim
                .jobDiscoveryInfoStream(jdim.key(JobDiscoveryService.LookupType.JOB_CLUSTER, jobCluster))
                .map(Option::of)
                .take(1)
                .timeout(2, TimeUnit.SECONDS, Observable.just(Option.none()))
                .doOnError((t) -> {
                    log.warn("Timed out looking up job discovery info for cluster: " + jobCluster + ".");
                })
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .toSingle()
                .toBlocking()
                .value();
    }
}
