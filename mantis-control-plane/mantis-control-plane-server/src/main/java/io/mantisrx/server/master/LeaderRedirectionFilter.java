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

package io.mantisrx.server.master;

import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeaderRedirectionFilter extends AllDirectives {

    public static final Logger logger = LoggerFactory.getLogger(LeaderRedirectionFilter.class);
    private final MasterMonitor masterMonitor;
    private final ILeadershipManager leadershipManager;
    private final Counter api503MasterNotReady;
    private final Counter apiRedirectsToLeader;

    public LeaderRedirectionFilter(final MasterMonitor masterMonitor, final ILeadershipManager leadershipManager) {
        this.masterMonitor = masterMonitor;
        this.leadershipManager = leadershipManager;
        Metrics m = new Metrics.Builder()
                .id("LeaderRedirectionFilter")
                .addCounter("api503MasterNotReady")
                .addCounter("apiRedirectsToLeader")
                .build();
        this.api503MasterNotReady = m.getCounter("api503MasterNotReady");
        this.apiRedirectsToLeader = m.getCounter("apiRedirectsToLeader");
    }

    private boolean isLocalHost(MasterDescription master) {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            for (InetAddress addr : InetAddress.getAllByName(master.getHostname())) {
                if (addr.equals(localHost)) {
                    return true;
                }
            }
        } catch (UnknownHostException e) {
            //logger.warn("Failed to compare if given master {} is local host: {}", master, e);
            return false;
        }

        return false;
    }

    public Route redirectIfNotLeader(final Route leaderRoute) {
        MasterDescription latestMaster = masterMonitor.getLatestMaster();
        if (leadershipManager.isLeader() || isLocalHost(latestMaster)) {
            if (leadershipManager.isReady()) {
                return leaderRoute;
            } else {
                return extractUri(uri -> {
                    logger.info("leader is not ready, returning 503 for {}", uri);
                    api503MasterNotReady.increment();
                    return complete(StatusCodes.SERVICE_UNAVAILABLE, "Mantis master awaiting to be ready");
                });
            }
        } else {
            String hostname = latestMaster.getHostname();
            int apiPort = latestMaster.getApiPort();
            return extractUri(uri -> {
                Uri redirectUri = uri.host(hostname).port(apiPort);
                apiRedirectsToLeader.increment();
                logger.info("redirecting request {} to leader", redirectUri.toString());
                return redirect(redirectUri, StatusCodes.FOUND);
            });
        }
    }

    public Route rejectIfNotLeader(final Route leaderRoute) {
        MasterDescription latestMaster = masterMonitor.getLatestMaster();
        if (!leadershipManager.isLeader() && !isLocalHost(latestMaster)) {
            return extractUri(uri -> {
                logger.info("not leader, returning 500 for {}", uri);
                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "this node is not leader");
            });
        } else {
            if (leadershipManager.isReady()) {
                return leaderRoute;
            } else {
                return extractUri(uri -> {
                    logger.info("leader is not ready, returning 503 for {}", uri);
                    api503MasterNotReady.increment();
                    return complete(StatusCodes.SERVICE_UNAVAILABLE, "Mantis master awaiting to be ready");
                });
            }
        }
    }
}
