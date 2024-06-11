/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.master.api.akka.route.v1;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import com.netflix.mantis.master.scheduler.TestHelpers;
import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import io.mantisrx.server.master.resourcecluster.TaskExecutorTaskCancelledException;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class ResourceClustersLeaderExclusiveRouteTest extends JUnitRouteTest {
    private final ResourceClusters resourceClusters = mock(ResourceClusters.class);
    private final JsonSerializer serializer = new JsonSerializer();

    private final TestRoute testRoute =
        testRoute(new ResourceClustersLeaderExclusiveRoute(resourceClusters)
            .createRoute(route -> route));

    @BeforeClass
    public static void init() {
        TestHelpers.setupMasterConfig();
    }

    @Test
    public void testGetTaskExecutorStateWithCancelledWorker() throws IOException {
        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        TaskExecutorTaskCancelledException err = new TaskExecutorTaskCancelledException(
            "mock err", WorkerId.fromId("late-sine-function-tutorial-1-worker-0-1").get());
        when(resourceCluster.heartBeatFromTaskExecutor(ArgumentMatchers.any()))
            .thenReturn(
                CompletableFutures.exceptionallyCompletedFuture(err));
        when(resourceClusters.getClusterFor(ClusterID.of("myCluster"))).thenReturn(resourceCluster);

        TaskExecutorHeartbeat heartbeat = new TaskExecutorHeartbeat(
            TaskExecutorID.of("myExecutor"),
            ClusterID.of("myCluster"),
            TaskExecutorReport.available()
        );
        String encoded = serializer.toJson(heartbeat);

        testRoute.run(
            HttpRequest.POST("/api/v1/resourceClusters/myCluster/actions/heartBeatFromTaskExecutor")
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, encoded)))
            .assertStatusCode(StatusCodes.NOT_ACCEPTABLE)
            .assertEntity(serializer.toJson(err));
    }
}
