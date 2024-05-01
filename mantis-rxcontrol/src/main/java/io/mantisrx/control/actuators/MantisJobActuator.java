/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.actuators;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import io.mantisrx.control.IActuator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Actuator which controls the number of instances for a particular job id and stage.
 */
public class MantisJobActuator extends IActuator {

    private final String jobId;
    private final Integer stageNumber;
    private static Logger logger = LoggerFactory.getLogger(MantisJobActuator.class);

    private Long lastValue = Long.MIN_VALUE;

    private final String url;

    public MantisJobActuator(String jobId, Integer stageNumber, String environ, String region, String stack) {
        this.jobId = jobId;
        this.stageNumber = stageNumber;

        this.url = "staging".equals(stack.toLowerCase())
            ? "https://mantisapi.staging." + region + "." + environ + ".netflix.net"
            : "https://mantisapi." + region + "." + environ + ".netflix.net";

        logger.debug("Using scaling endpoint: " + url);
    }


    private final String scaleEndPoint = "/api/jobs/scaleStage";




    @Override
    protected Double processStep(Double input) {
        Long numWorkers = ((Double) Math.ceil(input)).longValue();

        if (numWorkers != lastValue) {
            logger.info("Scaling " + this.jobId + " to " + numWorkers + " workers.");

            String payload = "{\"JobId\":\"" + this.jobId + "\",\"StageNumber\":"
              + this.stageNumber + ",\"NumWorkers\":\"" + numWorkers + "\"}";

            try {
                HttpResponse<String> resp = Unirest.post(url + scaleEndPoint)
                        .header("accept", "application/json")
                        .body(payload)
                        .asString();
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }

            lastValue = numWorkers;
        }
        return numWorkers * 1.0;
    }
}
