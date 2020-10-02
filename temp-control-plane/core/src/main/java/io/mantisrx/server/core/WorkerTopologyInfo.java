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

import java.util.HashMap;
import java.util.Map;

import io.mantisrx.runtime.descriptor.StageSchedulingInfo;


public class WorkerTopologyInfo {

    private static final String MANTIS_JOB_NAME = "MANTIS_JOB_NAME";
    private static final String MANTIS_JOB_ID = "MANTIS_JOB_ID";
    private static final String MANTIS_WORKER_INDEX = "MANTIS_WORKER_INDEX";
    private static final String MANTIS_WORKER_NUMBER = "MANTIS_WORKER_NUMBER";
    private static final String MANTIS_WORKER_STAGE_NUMBER = "MANTIS_WORKER_STAGE_NUMBER";
    private static final String MANTIS_NUM_STAGES = "MANTIS_NUM_STAGES";
    private static final String MANTIS_PREV_STAGE_INITIAL_NUM_WORKERS = "MANTIS_PREV_STAGE_INITIAL_NUM_WORKERS";
    private static final String MANTIS_NEXT_STAGE_INITIAL_NUM_WORKERS = "MANTIS_NEXT_STAGE_INITIAL_NUM_WORKERS";
    private static final String MANTIS_METRICS_PORT = "MANTIS_METRICS_PORT";
    private static final String MANTIS_WORKER_CUSTOM_PORT = "MANTIS_WORKER_CUSTOM_PORT";

    public static class Data {

        private String jobName;
        private String JobId;
        private int workerIndex;
        private int workerNumber;
        private int stageNumber;
        private int numStages;
        private int prevStageInitialNumWorkers = -1;
        private int nextStageInitialNumWorkers = -1;
        private int metricsPort;

        public Data(String jobName, String jobId, int workerIndex, int workerNumber, int stageNumber, int numStages) {
            this.jobName = jobName;
            JobId = jobId;
            this.workerIndex = workerIndex;
            this.workerNumber = workerNumber;
            this.stageNumber = stageNumber;
            this.numStages = numStages;
        }

        public Data(String jobName, String jobId, int workerIndex, int workerNumber, int stageNumber, int numStages,
                    int prevStageInitialNumWorkers, int nextStageInitialNumWorkers,
                    int metricsPort) {
            this.jobName = jobName;
            JobId = jobId;
            this.workerIndex = workerIndex;
            this.workerNumber = workerNumber;
            this.stageNumber = stageNumber;
            this.numStages = numStages;
            this.prevStageInitialNumWorkers = prevStageInitialNumWorkers;
            this.nextStageInitialNumWorkers = nextStageInitialNumWorkers;
            this.metricsPort = metricsPort;
        }

        public String getJobName() {
            return jobName;
        }

        public String getJobId() {
            return JobId;
        }

        public int getWorkerIndex() {
            return workerIndex;
        }

        public int getWorkerNumber() {
            return workerNumber;
        }

        public int getStageNumber() {
            return stageNumber;
        }

        public int getNumStages() {
            return numStages;
        }

        public int getPrevStageInitialNumWorkers() {
            return prevStageInitialNumWorkers;
        }

        public int getNextStageInitialNumWorkers() {
            return nextStageInitialNumWorkers;
        }

        public int getMetricsPort() {
            return metricsPort;
        }
    }

    public static class Writer {

        private final Map<String, String> envVars;

        public Writer(ExecuteStageRequest request) {
            envVars = new HashMap<>();
            envVars.put(MANTIS_JOB_NAME, request.getJobName());
            envVars.put(MANTIS_JOB_ID, request.getJobId());
            envVars.put(MANTIS_WORKER_INDEX, request.getWorkerIndex() + "");
            envVars.put(MANTIS_WORKER_NUMBER, request.getWorkerNumber() + "");
            envVars.put(MANTIS_WORKER_STAGE_NUMBER, request.getStage() + "");
            envVars.put(MANTIS_NUM_STAGES, request.getTotalNumStages() + "");
            final int totalNumWorkerStages = request.getTotalNumStages() - (request.getHasJobMaster() ? 1 : 0);
            if (totalNumWorkerStages > 1 && request.getStage() > 1) {
                StageSchedulingInfo prevStage = request.getSchedulingInfo().forStage(request.getStage() - 1);
                envVars.put(MANTIS_PREV_STAGE_INITIAL_NUM_WORKERS, prevStage.getNumberOfInstances() + "");
                if (totalNumWorkerStages > request.getStage()) {
                    StageSchedulingInfo nextStage = request.getSchedulingInfo().forStage(
                            request.getStage() + 1);
                    envVars.put(MANTIS_NEXT_STAGE_INITIAL_NUM_WORKERS, nextStage.getNumberOfInstances() + "");
                }
            }
            envVars.put(MANTIS_METRICS_PORT, request.getMetricsPort() + "");
            envVars.put(MANTIS_WORKER_CUSTOM_PORT, request.getWorkerPorts().getCustomPort() + "");
        }

        public Map<String, String> getEnvVars() {
            return envVars;
        }
    }

    public static class Reader {

        private static Data data;

        static {
            data = new Data(
                    System.getenv(MANTIS_JOB_NAME),
                    System.getenv(MANTIS_JOB_ID),
                    System.getenv(MANTIS_WORKER_INDEX) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_WORKER_INDEX)),
                    System.getenv(MANTIS_WORKER_NUMBER) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_WORKER_NUMBER)),
                    System.getenv(MANTIS_WORKER_STAGE_NUMBER) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_WORKER_STAGE_NUMBER)),
                    System.getenv(MANTIS_NUM_STAGES) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_NUM_STAGES)),
                    System.getenv(MANTIS_PREV_STAGE_INITIAL_NUM_WORKERS) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_PREV_STAGE_INITIAL_NUM_WORKERS)),
                    System.getenv(MANTIS_NEXT_STAGE_INITIAL_NUM_WORKERS) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_NEXT_STAGE_INITIAL_NUM_WORKERS)),
                    System.getenv(MANTIS_METRICS_PORT) == null ?
                            -1 : Integer.parseInt(System.getenv(MANTIS_METRICS_PORT))
            );
        }

        public static Data getData() {
            return data;
        }
    }
}
