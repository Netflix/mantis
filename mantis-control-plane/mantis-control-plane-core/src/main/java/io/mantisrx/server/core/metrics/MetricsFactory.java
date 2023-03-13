/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.core.metrics;

import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.stats.MetricStringConstants;
import java.util.HashMap;
import java.util.Map;

public class MetricsFactory {

  /**
   * Returns a metrics server, publishing metrics every 1 second
   *
   * @param request request for which the metrics need to be published
   * @return MetricsServerService server
   */
  public static MetricsServerService newMetricsServer(CoreConfiguration configuration, ExecuteStageRequest request) {

    // todo(sundaram): get rid of the dependency on the metrics port defined at the ExecuteStageRequest level
    // because that's a configuration of the task manager rather than the request.
    return new MetricsServerService(request.getMetricsPort(), 1, getCommonTags(request));
  }

  public static MetricsPublisherService newMetricsPublisher(CoreConfiguration config, ExecuteStageRequest request) {
    return new MetricsPublisherService(config.getMetricsPublisher(),
        config.getMetricsPublisherFrequencyInSeconds(), getCommonTags(request));
  }

  public static Map<String, String> getCommonTags(ExecuteStageRequest request) {
    // provide common tags to metrics publishing service
    Map<String, String> commonTags = new HashMap<>();
    commonTags.put(MetricStringConstants.MANTIS_WORKER_NUM, Integer.toString(request.getWorkerNumber()));
    commonTags.put(MetricStringConstants.MANTIS_STAGE_NUM, Integer.toString(request.getStage()));
    commonTags.put(MetricStringConstants.MANTIS_WORKER_INDEX, Integer.toString(request.getWorkerIndex()));
    commonTags.put(MetricStringConstants.MANTIS_JOB_NAME, request.getJobName());
    commonTags.put(MetricStringConstants.MANTIS_JOB_ID, request.getJobId());

    // adding the following for mesos metrics back compat
    commonTags.put(MetricStringConstants.MANTIS_WORKER_NUMBER, Integer.toString(request.getWorkerNumber()));
    commonTags.put(MetricStringConstants.MANTIS_WORKER_STAGE_NUMBER, Integer.toString(request.getStage()));

    return commonTags;
  }
}
