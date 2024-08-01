/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.runtime.loader.config;

import io.mantisrx.common.metrics.MetricsPublisher;
import io.mantisrx.server.core.ILeaderMonitorFactory;
import io.mantisrx.server.core.utils.ConfigUtils;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.File;
import java.net.URI;
import lombok.Builder;
import lombok.Data;

/**
 * This is a workaround solution to bridge the worker configuration to a serializable format between worker process
 * entry and RuntimeTask implementations.
 */
@Data
@Builder
public class WorkerConfigurationWritable implements WorkerConfiguration {

    int zkConnectionTimeoutMs;
    int zkConnectionRetrySleepMs;
    int zkConnectionMaxRetries;
    String zkConnectionString;
    String leaderAnnouncementPath;
    String zkRoot;
    boolean isLocalMode;
    int metricsPublisherFrequencyInSeconds;
    String taskExecutorId;

    String clusterId;

    int metricsPort;
    int debugPort;
    int consolePort;
    int customPort;
    int sinkPort;
    int heartbeatInternalInMs;
    int tolerableConsecutiveHeartbeatFailures;
    int heartbeatTimeoutMs;
    long heartbeatRetryInitialDelayMs;
    long heartbeatRetryMaxDelayMs;
    long registrationRetryInitialDelayMillis;
    double registrationRetryMultiplier;
    double registrationRetryRandomizationFactor;
    int registrationRetryMaxAttempts;
    String externalAddress;
    String externalPortRange;
    String bindAddress;
    Integer bindPort;

    URI blobStoreArtifactDir;
    File localStorageDir;
    Double cpuCores;
    Double memoryInMB;
    Double diskInMB;
    double networkBandwidthInMB;
    String taskExecutorAttributesStr;
    int asyncHttpClientMaxConnectionsPerHost;
    int asyncHttpClientConnectionTimeoutMs;
    int asyncHttpClientRequestTimeoutMs;
    int asyncHttpClientReadTimeoutMs;
    boolean asyncHttpClientFollowRedirect;
    String leaderMonitorFactory;
    String metricsCollectorClass;
    String jobAutoscalerManagerClassName;

    @JsonIgnore
    MetricsPublisher metricsPublisher;

    @JsonIgnore
    MetricsCollector metricsCollector;

    @Override
    public int getZkConnectionTimeoutMs() {
        return this.zkConnectionTimeoutMs;
    }

    @Override
    public int getZkConnectionRetrySleepMs() {
        return this.zkConnectionRetrySleepMs;
    }

    @Override
    public int getZkConnectionMaxRetries() {
        return this.zkConnectionMaxRetries;
    }

    @Override
    public String getZkConnectionString() {
        return this.zkConnectionString;
    }

    @Override
    public String getLeaderAnnouncementPath() {
        return this.leaderAnnouncementPath;
    }

    @Override
    public String getZkRoot() {
        return this.zkRoot;
    }

    @Override
    public boolean isLocalMode() {
        return this.isLocalMode;
    }

    @Override
    public MetricsPublisher getMetricsPublisher() {
        return this.metricsPublisher;
    }

    @Override
    public int getMetricsPublisherFrequencyInSeconds() {
        return this.metricsPublisherFrequencyInSeconds;
    }

    @Override
    public boolean getAsyncHttpClientFollowRedirect() {
        return this.asyncHttpClientFollowRedirect;
    }

    @Override
    public String getLeaderMonitorFactoryName() {return this.leaderMonitorFactory;}

    public ILeaderMonitorFactory getLeaderMonitorFactoryImpl() {
        return ConfigUtils.createInstance(this.leaderMonitorFactory, ILeaderMonitorFactory.class);
    }

    @Override
    public String getTaskExecutorId() {
        return this.taskExecutorId;
    }

    @Override
    public String getClusterId() {
        return this.clusterId;
    }

    @Override
    public int getMetricsPort() {
        return this.metricsPort;
    }

    @Override
    public int getDebugPort() {
        return this.debugPort;
    }

    @Override
    public int getConsolePort() {
        return this.consolePort;
    }

    @Override
    public int getCustomPort() {
        return this.customPort;
    }

    @Override
    public int getSinkPort() {
        return this.sinkPort;
    }

    @Override
    public String getMetricsCollectorClassName() {
        return this.metricsCollectorClass;
    }

    @Override
    public int heartbeatInternalInMs() {
        return this.heartbeatInternalInMs;
    }

    @Override
    public int getTolerableConsecutiveHeartbeatFailures() {
        return this.tolerableConsecutiveHeartbeatFailures;
    }

    @Override
    public int heartbeatTimeoutMs() {
        return this.heartbeatTimeoutMs;
    }

    @Override
    public long heartbeatRetryInitialDelayMs() {
        return this.heartbeatRetryInitialDelayMs;
    }

    @Override
    public long heartbeatRetryMaxDelayMs() {
        return this.heartbeatRetryMaxDelayMs;
    }

    @Override
    public long registrationRetryInitialDelayMillis() {
        return this.registrationRetryInitialDelayMillis;
    }

    @Override
    public double registrationRetryMultiplier() {
        return this.registrationRetryMultiplier;
    }

    @Override
    public double registrationRetryRandomizationFactor() {
        return this.registrationRetryRandomizationFactor;
    }

    @Override
    public int registrationRetryMaxAttempts() {
        return this.registrationRetryMaxAttempts;
    }

    @Override
    public String getExternalAddress() {
        return this.externalAddress;
    }

    @Override
    public String getExternalPortRange() {
        return this.externalPortRange;
    }

    @Override
    public String getBindAddress() {
        return this.bindAddress;
    }

    @Override
    public Integer getBindPort() {
        return this.bindPort;
    }

    @Override
    public MetricsCollector getUsageSupplier() {
        return this.metricsCollector;
    }

    @Override
    public URI getBlobStoreArtifactDir() {
        return this.blobStoreArtifactDir;
    }

    @Override
    public File getLocalStorageDir() {
        return this.localStorageDir;
    }

    @Override
    public Double getCpuCores() {
        return this.cpuCores;
    }

    @Override
    public Double getMemoryInMB() {
        return this.memoryInMB;
    }

    @Override
    public Double getDiskInMB() {
        return this.diskInMB;
    }

    @Override
    public double getNetworkBandwidthInMB() {
        return this.networkBandwidthInMB;
    }

    @Override
    public String taskExecutorAttributes() {
        return this.taskExecutorAttributesStr;
    }

    @Override
    public File getRegistrationStoreDir() {
        return null;
    }
}
