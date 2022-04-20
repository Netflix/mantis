/*
 * Copyright 2022 Netflix, Inc.
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

import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ResourceCluster;
import io.mantisrx.server.master.resourcecluster.ResourceCluster.TaskExecutorStatus;
import io.mantisrx.server.master.resourcecluster.ResourceClusters;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class ResourceClusterReadRouteTest extends JUnitRouteTest {
    private final ResourceClusters resourceClusters = mock(ResourceClusters.class);
    private final TestRoute testRoute =
        testRoute(new ResourceClustersReadRoute(resourceClusters).createRoute(route -> route));

    @Test
    public void testGetTaskExecutorState() {
        TaskExecutorRegistration registration = new TaskExecutorRegistration(
            TaskExecutorID.of("myExecutor"),
            ClusterID.of("myCluster"),
            "taskExecutorAddress",
            "hostName",
            new WorkerPorts(1, 2, 3, 4, 5),
            new MachineDefinition(1, 1, 1, 1, 1));
        TaskExecutorStatus status =
            new TaskExecutorStatus(registration, true, true, true, null, Instant.now());
        ResourceCluster resourceCluster = mock(ResourceCluster.class);
        when(resourceCluster.getTaskExecutorState(TaskExecutorID.of("myExecutor")))
            .thenReturn(CompletableFuture.completedFuture(status));
        when(resourceClusters.getClusterFor(ClusterID.of("myCluster"))).thenReturn(resourceCluster);

        testRoute.run(HttpRequest.GET("/api/v1/resourceClusters/myCluster/taskExecutors/myExecutor/getTaskExecutorState"))
            .assertStatusCode(200);
    }
}
