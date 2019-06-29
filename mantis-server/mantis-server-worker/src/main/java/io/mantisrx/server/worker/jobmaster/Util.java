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

package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import org.slf4j.Logger;


public class Util {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Util.class);

    public static double getEffectiveValue(StageSchedulingInfo stageSchedulingInfo, StageScalingPolicy.ScalingReason type, double value) {
        switch (type) {
        case CPU:
            return 100.0 * value / stageSchedulingInfo.getMachineDefinition().getCpuCores();
        case Memory:
            return 100.0 * value / stageSchedulingInfo.getMachineDefinition().getMemoryMB();
        case DataDrop:
        case KafkaLag:
        case UserDefined:
            return value;
        case Network:
            // value is in bytes, multiply by 8, divide by M
            return 100.0 * value * 8 / (1024.0 * 1024.0 * stageSchedulingInfo.getMachineDefinition().getNetworkMbps());
        default:
            log.warn("Unsupported type " + type);
            return 0.0;
        }
    }
}
