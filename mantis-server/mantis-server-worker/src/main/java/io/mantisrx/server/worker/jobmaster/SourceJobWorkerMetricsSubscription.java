/*
 * Copyright 2020 Netflix, Inc.
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

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SourceJobParameters;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Manages subscriptions to source job workers.
 */
public class SourceJobWorkerMetricsSubscription {
    private static final Logger logger = LoggerFactory.getLogger(SourceJobWorkerMetricsSubscription.class);

    private final List<SourceJobParameters.TargetInfo> targetInfos;
    private final MantisMasterGateway masterClient;
    private final WorkerMetricsClient workerMetricsClient;
    private final AutoScaleMetricsConfig metricsConfig;

    public SourceJobWorkerMetricsSubscription(List<SourceJobParameters.TargetInfo> targetInfos,
                                              MantisMasterGateway masterClient,
                                              WorkerMetricsClient workerMetricsClient,
                                              AutoScaleMetricsConfig metricsConfig) {
        this.targetInfos = targetInfos;
        this.masterClient = masterClient;
        this.workerMetricsClient = workerMetricsClient;
        this.metricsConfig = metricsConfig;
    }

    public Observable<Observable<MantisServerSentEvent>> getResults() {
        return Observable.merge(getSourceJobToClientMap().entrySet().stream().map(entry -> {
            String sourceJobName = entry.getKey();
            Set<String> clientIds = entry.getValue();
            Set<String> sourceJobMetrics = metricsConfig.generateSourceJobMetricGroups(clientIds);
            return masterClient
                    .namedJobInfo(sourceJobName)
                    .map(NamedJobInfo::getJobId)
                    .flatMap(jobId -> getResultsForJobId(jobId, sourceJobMetrics));
        }).collect(Collectors.toList()));
    }

    protected Observable<Observable<MantisServerSentEvent>> getResultsForJobId(String jobId, Set<String> sourceJobMetrics) {
        return new WorkerMetricSubscription(jobId, workerMetricsClient, sourceJobMetrics).getMetricsClient().getResults();
    }

    protected Map<String, Set<String>> getSourceJobToClientMap() {
        Map<String, Set<String>> results = new HashMap<>();
        for (SourceJobParameters.TargetInfo info : targetInfos) {
            Set<String> clientIds = results.get(info.sourceJobName);
            if (clientIds == null) {
                clientIds = new HashSet<>();
                results.put(info.sourceJobName, clientIds);
            }
            clientIds.add(info.clientId);
        }
        return results;
    }
}
