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

package io.mantisrx.common;

public final class SystemParameters {
    public static final String JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM = "mantis.jobmaster.autoscale.metric";
    public static final String JOB_MASTER_AUTOSCALE_CONFIG_SYSTEM_PARAM = "mantis.jobmaster.autoscale.adaptive.config";
    public static final String JOB_MASTER_CLUTCH_SYSTEM_PARAM = "mantis.jobmaster.clutch.config";
    public static final String JOB_MASTER_CLUTCH_EXPERIMENTAL_PARAM = "mantis.jobmaster.clutch.experimental.enabled";
    public static final String JOB_MASTER_AUTOSCALE_SOURCEJOB_METRIC_PARAM = "mantis.jobmaster.autoscale.sourcejob.metric.enabled";
    public static final String JOB_MASTER_AUTOSCALE_SOURCEJOB_TARGET_PARAM = "mantis.jobmaster.autoscale.sourcejob.target";
    public static final String JOB_MASTER_AUTOSCALE_SOURCEJOB_DROP_METRIC_PATTERNS_PARAM = "mantis.jobmaster.autoscale.sourcejob.dropMetricPatterns";
    public static final String JOB_WORKER_HEARTBEAT_INTERVAL_SECS = "mantis.job.worker.heartbeat.interval.secs";
    public static final String JOB_WORKER_TIMEOUT_SECS = "mantis.job.worker.timeout.secs";

    @Deprecated
    public static final String MANTIS_WORKER_JVM_OPTS_STAGE_PREFIX = "MANTIS_WORKER_JVM_OPTS_STAGE";

    @Deprecated
    public static final String PER_STAGE_JVM_OPTS_FORMAT = MANTIS_WORKER_JVM_OPTS_STAGE_PREFIX + "%d";
    public static final String STAGE_CONCURRENCY = "mantis.stageConcurrency";

    @Deprecated
    public static final int MAX_NUM_STAGES_FOR_JVM_OPTS_OVERRIDE = 5;

    private SystemParameters() {}
}
