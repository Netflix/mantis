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

package io.mantisrx.server.worker.jobmaster.control.actuators;

import io.mantisrx.control.IActuator;
import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Actuator for Mantis Stages which acts as a lifter for the
 * JobAutoScaler.StageScaler which brings it into the context of our
 * control system DSL.
 */
public class MantisStageActuator extends IActuator {

    private static Logger logger = LoggerFactory.getLogger(MantisStageActuator.class);
    private final JobAutoScaler.StageScaler scaler;
    private Long lastValue;

    public MantisStageActuator(long initialSize, JobAutoScaler.StageScaler scaler) {
        this.scaler = scaler;
        this.lastValue = initialSize;
    }

    protected Double processStep(Double input) {
        Long desiredNumWorkers = ((Double) Math.ceil(input)).longValue();

        String reason = "Clutch determined " + desiredNumWorkers + " instance(s) for target resource usage.";
        if (desiredNumWorkers < this.lastValue) {
            if (!scaler.scaleDownStage(lastValue.intValue(), desiredNumWorkers.intValue(), reason)) {
                return this.lastValue * 1.0;
            }
            this.lastValue = desiredNumWorkers;
        } else if (desiredNumWorkers > this.lastValue) {
            scaler.scaleUpStage(lastValue.intValue(), desiredNumWorkers.intValue(), reason);
            this.lastValue = desiredNumWorkers;
        } else {
        }

        return desiredNumWorkers * 1.0;
    }
}
