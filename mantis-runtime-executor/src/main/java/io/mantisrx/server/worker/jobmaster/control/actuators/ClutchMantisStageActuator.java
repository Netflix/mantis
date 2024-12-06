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

import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import io.vavr.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class ClutchMantisStageActuator implements Observable.Transformer<Tuple3<String, Double, Integer>, Double> {

    private static Logger logger = LoggerFactory.getLogger(MantisStageActuator.class);
    private final JobAutoScaler.StageScaler scaler;

    public ClutchMantisStageActuator(JobAutoScaler.StageScaler scaler) {
        this.scaler = scaler;
    }

    protected Double processStep(Tuple3<String, Double, Integer> tup) {
        int desiredNumWorkers = ((Double) Math.ceil(tup._2)).intValue();
        logger.info("Received request to scale to {} from {} workers.", desiredNumWorkers, tup._3);

        String reason = tup._1;
        if (desiredNumWorkers < tup._3) {
            // If scale down is disabled, return current value
            if (!scaler.scaleDownStage(tup._3, desiredNumWorkers, reason)) {
                return tup._3 * 1.0;
            }
        } else if (desiredNumWorkers > tup._3) {
            scaler.scaleUpStage(tup._3, desiredNumWorkers, reason);
        } else {
        }

        return desiredNumWorkers * 1.0;
    }

    @Override
    public Observable<Double> call(Observable<Tuple3<String, Double, Integer>> tuple2Observable) {
        return tuple2Observable.map(this::processStep);
    }
}
