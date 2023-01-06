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

package io.mantisrx.master.jobcluster.job;

import com.typesafe.config.Config;
import io.mantisrx.runtime.MachineDefinition;
import java.time.Duration;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class JobSettings {
    MachineDefinition workerMaxMachineDefinition;
    int workerMaxResubmits;
    List<Duration> workerResubmitIntervals;
    Duration workerResubmitExpiry;

    int maxWorkersPerStage;
    MachineDefinition jobMasterMachineDefinition;
    Duration defaultSubscriptionTimeout;
    Duration stageAssignmentRefreshInterval;

    public static JobSettings fromConfig(Config config) {
        return
            JobSettings
                .builder()
                .workerMaxMachineDefinition(
                    MachineDefinition.fromConfig(config.getConfig("worker.maxMachineDefinition")))
                .workerMaxResubmits(config.getInt("worker.maxResubmits"))
                .workerResubmitIntervals(config.getDurationList("worker.resubmitIntervals"))
                .workerResubmitExpiry(config.getDuration("worker.resubmitExpiry"))
                .maxWorkersPerStage(config.getInt("maxWorkersPerStage"))
                .jobMasterMachineDefinition(
                    MachineDefinition.fromConfig(config.getConfig("master.machineDefinition")))
                .defaultSubscriptionTimeout(config.getDuration("subscriptionTimeout"))
                .build();
    }
}
