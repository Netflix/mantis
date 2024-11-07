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

package io.mantisrx.server.core;

import io.mantisrx.common.metrics.MetricsPublisher;
import org.skife.config.Config;
import org.skife.config.Default;


/**
 * The configurations declared in this interface should be the ones that are shared by both worker and master (and
 * potentially other sub-projects).
 */
public interface CoreConfiguration {

    @Config("mantis.zookeeper.connectionTimeMs")
    @Default("10000")
    int getZkConnectionTimeoutMs();

    @Config("mantis.zookeeper.connection.retrySleepMs")
    @Default("500")
    int getZkConnectionRetrySleepMs();

    @Config("mantis.zookeeper.connection.retryCount")
    @Default("5")
    int getZkConnectionMaxRetries();

    @Config("mantis.zookeeper.connectString")
    @Default("localhost:2181")
    String getZkConnectionString();

    @Config("mantis.zookeeper.leader.announcement.path")
    @Default("/leader")
    String getLeaderAnnouncementPath();

    @Config("mantis.zookeeper.root")
    String getZkRoot();

    @Config("mantis.localmode")
    @Default("true")
    boolean isLocalMode();

    @Config("mantis.metricsPublisher.class")
    @Default("io.mantisrx.common.metrics.MetricsPublisherNoOp")
    MetricsPublisher getMetricsPublisher();

    @Config("mantis.metricsPublisher.publishFrequencyInSeconds")
    @Default("15")
    int getMetricsPublisherFrequencyInSeconds();

    @Config("mantis.asyncHttpClient.maxConnectionsPerHost")
    @Default("2")
    int getAsyncHttpClientMaxConnectionsPerHost();

    @Config("mantis.asyncHttpClient.connectionTimeoutMs")
    @Default("90000")
    int getAsyncHttpClientConnectionTimeoutMs();

    @Config("mantis.asyncHttpClient.requestTimeoutMs")
    @Default("90000")
    int getAsyncHttpClientRequestTimeoutMs();

    @Config("mantis.asyncHttpClient.readTimeoutMs")
    @Default("90000")
    int getAsyncHttpClientReadTimeoutMs();

    @Config("mantis.asyncHttpClient.followRedirect")
    @Default("true")
    boolean getAsyncHttpClientFollowRedirect();

    @Config("mantis.leader.monitor.factory")
    @Default("io.mantisrx.server.core.master.LocalLeaderFactory")
    String getLeaderMonitorFactoryName();
}
