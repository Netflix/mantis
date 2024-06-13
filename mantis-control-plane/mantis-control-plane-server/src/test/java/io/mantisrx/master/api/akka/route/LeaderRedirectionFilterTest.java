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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.LocalMasterMonitor;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.master.LeaderRedirectionFilter;
import io.mantisrx.server.master.LeadershipManagerLocalImpl;
import org.junit.Test;

public class LeaderRedirectionFilterTest extends AllDirectives {

    @Test
    public void testRouteUnchangedIfLeader() {
        // Become leader and make Master monitor return the localhost master, filter should return input Route
        final MasterDescription fakeMasterDesc = new MasterDescription(
            "localhost",
            "127.0.0.1", 8100,
            8100 + 2,
            8100 + 4,
            "api/postjobstatus",
            8100 + 6,
            System.currentTimeMillis());
        MasterMonitor masterMonitor = new LocalMasterMonitor(fakeMasterDesc);
        io.mantisrx.server.core.ILeadershipManager leadershipManager = new LeadershipManagerLocalImpl(fakeMasterDesc);
        leadershipManager.becomeLeader();
        LeaderRedirectionFilter filter = new LeaderRedirectionFilter(masterMonitor, leadershipManager);
        Route testRoute = route(path("test", () -> complete("done")));
        Route route = filter.redirectIfNotLeader(testRoute);
        // leader is not ready by default
        assertNotEquals(testRoute, route);
        // mark leader ready
        leadershipManager.setLeaderReady();
        Route route2 = filter.redirectIfNotLeader(testRoute);
        // leader is not ready by default
        assertEquals(testRoute, route2);
    }

    @Test
    public void testRouteChangesIfNotLeader() {
        final MasterDescription fakeMasterDesc = new MasterDescription(
            "localhost",
            "127.0.0.1", 8100,
            8100 + 2,
            8100 + 4,
            "api/postjobstatus",
            8100 + 6,
            System.currentTimeMillis());
        MasterMonitor masterMonitor = new LocalMasterMonitor(fakeMasterDesc);
        ILeadershipManager leadershipManager = new LeadershipManagerLocalImpl(fakeMasterDesc);
        // Stop being leader, the filter should redirect so the returned Route is different from the input Route
        leadershipManager.stopBeingLeader();
        LeaderRedirectionFilter filter = new LeaderRedirectionFilter(masterMonitor, leadershipManager);
        Route testRoute = route(path("test", () -> complete("done")));
        Route route = filter.redirectIfNotLeader(testRoute);
        // filter should return input Route if we are current leader
        assertNotEquals(testRoute, route);
    }
}
