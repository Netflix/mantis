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

package io.mantisrx.server.core.stats;

public class MetricStringConstants {

    public static final String METRIC_NAME_STR = "name";
    public static final String MANTIS_JOB_NAME = "mantisJobName";
    public static final String MANTIS_JOB_ID = "mantisJobId";
    public static final String MANTIS_STAGE_NUM = "mantisStageNum";
    public static final String MANTIS_WORKER_STAGE_NUMBER = "mantisWorkerStageNumber";

    public static final String MANTIS_WORKER_INDEX = "mantisWorkerIndex";
    public static final String MANTIS_WORKER_NUM = "mantisWorkerNum";
    public static final String MANTIS_WORKER_NUMBER = "mantisWorkerNumber";
    // Resource Usage metrics
    public static final String RESOURCE_USAGE_METRIC_GROUP = "ResourceUsage";
    public static final String CPU_PCT_LIMIT = "cpuPctLimit";
    public static final String CPU_PCT_USAGE_CURR = "cpuPctUsageCurr";
    public static final String CPU_PCT_USAGE_PEAK = "cpuPctUsagePeak";
    public static final String MEM_LIMIT = "memLimit";
    public static final String CACHED_MEM_USAGE_CURR = "cachedMemUsageCurr";
    public static final String CACHED_MEM_USAGE_PEAK = "cachedMemUsagePeak";
    public static final String TOT_MEM_USAGE_CURR = "totMemUsageCurr";
    public static final String TOT_MEM_USAGE_PEAK = "totMemUsagePeak";
    public static final String NW_BYTES_LIMIT = "nwBytesLimit";
    public static final String NW_BYTES_USAGE_CURR = "nwBytesUsageCurr";
    public static final String NW_BYTES_USAGE_PEAK = "nwBytesUsagePeak";
    // Data drop metrics
    public static final String DATA_DROP_METRIC_GROUP = "DataDrop";
    public static final String DROP_COUNT = "dropCount";
    public static final String ON_NEXT_COUNT = "onNextCount";
    public static final String DROP_PERCENT = "dropPercent";
    // Kafka lag metric
    public static final String KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP = "consumer-fetch-manager-metrics";
    public static final String KAFKA_LAG = "records-lag-max";
    public static final String KAFKA_PROCESSED = "records-consumed-rate";

    // RPS Metrics
    public static final String WORKER_STAGE_INNER_INPUT = "worker_stage_inner_input";
    public static final String ON_NEXT_GAUGE = "onNextGauge";

    private MetricStringConstants() {}
}
