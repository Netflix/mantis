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

package io.mantisrx.master.api.akka.payloads;

public class JobPayloads {
    public static final String RESUBMIT_WORKER = "{" +
        "\"JobId\": \"sine-function-1\"," +
        "\"user\": \"JobRouteTest\"," +
        "\"workerNumber\": 2," +
        "\"reason\": \"test worker resubmit\"}";

    public static final String RESUBMIT_WORKER_NONEXISTENT = "{" +
                                                 "\"JobId\": \"NonExistent-1\"," +
                                                 "\"user\": \"JobRouteTest\"," +
                                                 "\"workerNumber\": 2," +
                                                 "\"reason\": \"test worker resubmit\"}";

    public static final String SCALE_STAGE = "{" +
        "\"JobId\": \"sine-function-1\"," +
        "\"NumWorkers\": 3," +
        "\"StageNumber\": 1," +
        "\"Reason\": \"test stage scaling\"}";

    public static final String SCALE_STAGE_NonExistent = "{" +
                                             "\"JobId\": \"NonExistent-1\"," +
                                             "\"NumWorkers\": 3," +
                                             "\"StageNumber\": 1," +
                                             "\"Reason\": \"test stage scaling\"}";

    public static final String KILL_JOB = "{" +
        "\"JobId\": \"sine-function-1\"," +
        "\"user\": \"JobRouteTest\"," +
        "\"reason\": \"test job kill\"}";

    public static final String KILL_JOB_NonExistent = "{" +
                                          "\"JobId\": \"NonExistent-1\"," +
                                          "\"user\": \"JobRouteTest\"," +
                                          "\"reason\": \"test job kill\"}";

    public static final String JOB_STATUS = "{\"jobId\":\"sine-function-1\",\"status\":{\"jobId\":\"sine-function-1\",\"stageNum\":1,\"workerIndex\":0,\"workerNumber\":2,\"type\":\"HEARTBEAT\",\"message\":\"heartbeat\",\"state\":\"Noop\",\"hostname\":null,\"timestamp\":1525813363585,\"reason\":\"Normal\",\"payloads\":[{\"type\":\"SubscriptionState\",\"data\":\"false\"},{\"type\":\"IncomingDataDrop\",\"data\":\"{\\\"onNextCount\\\":0,\\\"droppedCount\\\":0}\"}]}}";
}
