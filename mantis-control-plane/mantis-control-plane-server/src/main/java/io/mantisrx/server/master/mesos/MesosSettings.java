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

package io.mantisrx.server.master.mesos;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class MesosSettings {
    String masterLocation;

    Duration schedulerDriverInitTimeout;
    int schedulerDriverInitMaxAttempts;
    Duration workerTimeoutToReportStart;
    String workerExecutorScript;
    String workerInstallDir;
    String workerExecutorName;
    int workerJvmMemoryScaleBackPercent;

    boolean schedulerSlaveFilteringEnabled;
    String schedulerSlaveFilteringAttrName;
    String schedulerBalancedHostAttrName;
    Duration schedulerLeaseOfferExpiry;
    Duration schedulerIterationInterval;
    boolean schedulerDisableShortfallEvaluation;
    String schedulerAutoScalerMapHostnameAttributeName;
    String schedulerActiveVmGroupAttributeName;

    String frameworkName;
    String frameworkUser;
    Duration frameworkFailoverTimeout;

    Duration reconcilerInterval;
    AgentSettings agentSettings;

    public static MesosSettings fromConfig(Config config) {
        return MesosSettings.builder()
            .masterLocation(config.getString("masterLocation"))
            .schedulerDriverInitTimeout(config.getDuration("schedulerDriver.initTimeout"))
            .schedulerDriverInitMaxAttempts(config.getInt("schedulerDriver.maxAttempts"))

            .workerTimeoutToReportStart(config.getDuration("worker.timeoutToReportStart"))
            .workerExecutorScript(config.getString("worker.executorScript"))
            .workerInstallDir(config.getString("worker.installDir"))
            .workerExecutorName(config.getString("worker.executorName"))
            .workerJvmMemoryScaleBackPercent(config.getInt("worker.jvmMemoryScaleBackPercent"))

            .schedulerSlaveFilteringEnabled(config.getBoolean("scheduler.slaveFiltering.enabled"))
            .schedulerSlaveFilteringAttrName(config.getString("scheduler.slaveFiltering.attributeName"))
            .schedulerLeaseOfferExpiry(config.getDuration("scheduler.leaseOfferExpiry"))
            .schedulerBalancedHostAttrName(config.getString("scheduler.balancedHostAttrName"))
            .schedulerIterationInterval(config.getDuration("scheduler.epoch"))
            .schedulerDisableShortfallEvaluation(config.getBoolean("scheduler.disableShortfallEvaluation"))
            .schedulerAutoScalerMapHostnameAttributeName(config.getString("scheduler.autoScalerMapHostnameAttributeName"))
            .schedulerActiveVmGroupAttributeName(config.getString("scheduler.activeVmGroupAttributeName"))
            .reconcilerInterval(config.getDuration("reconcilerInterval"))

            .frameworkName(config.getString("framework.name"))
            .frameworkUser(config.getString("framework.user"))
            .frameworkFailoverTimeout(config.getDuration("framework.failoverTimeout"))

            .agentSettings(AgentSettings.fromConfig(config.getConfig("agent")))
            .build();
    }
}
