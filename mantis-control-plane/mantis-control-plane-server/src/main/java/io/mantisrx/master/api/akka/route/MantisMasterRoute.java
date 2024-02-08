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

package io.mantisrx.master.api.akka.route;

import akka.actor.ActorSystem;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import io.mantisrx.master.api.akka.route.handlers.ResourceClusterRouteHandler;
import io.mantisrx.master.api.akka.route.v0.JobClusterRoute;
import io.mantisrx.master.api.akka.route.v0.JobDiscoveryRoute;
import io.mantisrx.master.api.akka.route.v0.JobRoute;
import io.mantisrx.master.api.akka.route.v0.JobStatusRoute;
import io.mantisrx.master.api.akka.route.v0.MasterDescriptionRoute;
import io.mantisrx.master.api.akka.route.v1.AdminMasterRoute;
import io.mantisrx.master.api.akka.route.v1.JobArtifactsRoute;
import io.mantisrx.master.api.akka.route.v1.JobClustersRoute;
import io.mantisrx.master.api.akka.route.v1.JobDiscoveryStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobStatusStreamRoute;
import io.mantisrx.master.api.akka.route.v1.JobsRoute;
import io.mantisrx.master.api.akka.route.v1.LastSubmittedJobIdStreamRoute;
import io.mantisrx.master.api.akka.route.v1.ResourceClustersLeaderExclusiveRoute;
import io.mantisrx.master.api.akka.route.v1.ResourceClustersNonLeaderRedirectRoute;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;

public class MantisMasterRoute extends AllDirectives {

    private final LeaderRedirectionFilter leaderRedirectionFilter;


    private final JobClusterRoute v0JobClusterRoute;
    private final JobRoute v0JobRoute;
    private final JobDiscoveryRoute v0JobDiscoveryRoute;
    private final JobStatusRoute v0JobStatusRoute;
    private final MasterDescriptionRoute v0MasterDescriptionRoute;

    private final JobClustersRoute v1JobClusterRoute;
    private final JobsRoute v1JobsRoute;
    private final JobArtifactsRoute v1JobArtifactsRoute;
    private final AdminMasterRoute v1MasterRoute;
    private final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute;
    private final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute;
    private final JobStatusStreamRoute v1JobStatusStreamRoute;
    private final ResourceClustersNonLeaderRedirectRoute resourceClustersNonLeaderRedirectRoute;
    private final ResourceClustersLeaderExclusiveRoute resourceClustersLeaderExclusiveRoute;

    public MantisMasterRoute(
        final ActorSystem actorSystem,
        final LeaderRedirectionFilter leaderRedirectionFilter,
        final MasterDescriptionRoute v0MasterDescriptionRoute,
        final JobClusterRoute v0JobClusterRoute,
        final JobRoute v0JobRoute,
        final JobDiscoveryRoute v0JobDiscoveryRoute,
        final JobStatusRoute v0JobStatusRoute,
        final JobClustersRoute v1JobClusterRoute,
        final JobsRoute v1JobsRoute,
        final JobArtifactsRoute v1JobArtifactsRoute,
        final AdminMasterRoute v1MasterRoute,
        final JobDiscoveryStreamRoute v1JobDiscoveryStreamRoute,
        final LastSubmittedJobIdStreamRoute v1LastSubmittedJobIdStreamRoute,
        final JobStatusStreamRoute v1JobStatusStreamRoute,
        final ResourceClusters resourceClusters,
        final ResourceClusterRouteHandler resourceClusterRouteHandler) {
        this.leaderRedirectionFilter = leaderRedirectionFilter;
        this.v0MasterDescriptionRoute = v0MasterDescriptionRoute;
        this.v0JobClusterRoute = v0JobClusterRoute;
        this.v0JobRoute = v0JobRoute;
        this.v0JobDiscoveryRoute = v0JobDiscoveryRoute;
        this.v0JobStatusRoute = v0JobStatusRoute;

        this.v1JobClusterRoute = v1JobClusterRoute;
        this.v1JobsRoute = v1JobsRoute;
        this.v1JobArtifactsRoute = v1JobArtifactsRoute;
        this.v1MasterRoute = v1MasterRoute;
        this.v1JobDiscoveryStreamRoute = v1JobDiscoveryStreamRoute;
        this.v1LastSubmittedJobIdStreamRoute = v1LastSubmittedJobIdStreamRoute;
        this.v1JobStatusStreamRoute = v1JobStatusStreamRoute;
        this.resourceClustersNonLeaderRedirectRoute = new ResourceClustersNonLeaderRedirectRoute(
            resourceClusters, resourceClusterRouteHandler, actorSystem);
        this.resourceClustersLeaderExclusiveRoute = new ResourceClustersLeaderExclusiveRoute(resourceClusters);
    }

    public Route createRoute() {
        return concat(
                v0MasterDescriptionRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v0JobStatusRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v0JobRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v0JobClusterRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v0JobDiscoveryRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1JobClusterRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1JobsRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1JobArtifactsRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1MasterRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1JobDiscoveryStreamRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1LastSubmittedJobIdStreamRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                v1JobStatusStreamRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                resourceClustersNonLeaderRedirectRoute.createRoute(leaderRedirectionFilter::redirectIfNotLeader),
                resourceClustersLeaderExclusiveRoute.createRoute(leaderRedirectionFilter::rejectIfNotLeader)
        );
    }
}
