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

package io.mantisrx.runtime.loader.config;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.server.core.MetricsCoercer;
import io.mantisrx.server.master.client.config.PluginCoercible;
import java.io.IOException;
import java.util.Properties;
import org.skife.config.ConfigurationObjectFactory;

public class WorkerConfigurationUtils {
    public static <T extends WorkerConfiguration> T frmProperties(Properties properties, Class<T> tClass) {
        ConfigurationObjectFactory configurationObjectFactory = new ConfigurationObjectFactory(
            properties);
        configurationObjectFactory.addCoercible(new MetricsCoercer(properties));
        configurationObjectFactory.addCoercible(new PluginCoercible<>(MetricsCollector.class, properties));
        return configurationObjectFactory.build(tClass);
    }

    public static <T extends WorkerConfiguration> WorkerConfigurationWritable toWritable(T configSource) {
        return WorkerConfigurationWritable.builder()
            .bindAddress(configSource.getBindAddress())
            .bindPort(configSource.getBindPort())
            .blobStoreArtifactDir(configSource.getBlobStoreArtifactDir())
            .consolePort(configSource.getConsolePort())
            .clusterId(configSource.getClusterId())
            .customPort(configSource.getCustomPort())
            .debugPort(configSource.getDebugPort())
            .externalAddress(configSource.getExternalAddress())
            .externalPortRange(configSource.getExternalPortRange())
            .heartbeatInternalInMs(configSource.heartbeatInternalInMs())
            .heartbeatTimeoutMs(configSource.heartbeatTimeoutMs())
            .isLocalMode(configSource.isLocalMode())
            .leaderAnnouncementPath(configSource.getLeaderAnnouncementPath())
            .localStorageDir(configSource.getLocalStorageDir())
            .metricsCollector(configSource.getUsageSupplier())
            .metricsCollectorClass(configSource.getMetricsCollectorClassName())
            .jobAutoscalerManagerClassName(configSource.getJobAutoscalerManagerClassName())
            .metricsPort(configSource.getMetricsPort())
            .metricsPublisher(configSource.getMetricsPublisher())
            .metricsPublisherFrequencyInSeconds(configSource.getMetricsPublisherFrequencyInSeconds())
            .cpuCores(configSource.getCpuCores())
            .memoryInMB(configSource.getMemoryInMB())
            .diskInMB(configSource.getDiskInMB())
            .networkBandwidthInMB(configSource.getNetworkBandwidthInMB())
            .sinkPort(configSource.getSinkPort())
            .taskExecutorId(configSource.getTaskExecutorId())
            .taskExecutorAttributesStr(configSource.taskExecutorAttributes())
            .zkConnectionMaxRetries(configSource.getZkConnectionMaxRetries())
            .zkConnectionTimeoutMs(configSource.getZkConnectionTimeoutMs())
            .zkConnectionString(configSource.getZkConnectionString())
            .zkConnectionRetrySleepMs(configSource.getZkConnectionRetrySleepMs())
            .zkRoot(configSource.getZkRoot())
            .leaderMonitorFactory(configSource.getLeaderMonitorFactoryName())
            .build();
    }

    public static WorkerConfigurationWritable stringToWorkerConfiguration(String sourceStr) throws IOException {
        JsonSerializer ser = new JsonSerializer();
        return ser.fromJSON(sourceStr, WorkerConfigurationWritable.class);
    }
}
