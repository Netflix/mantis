package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SourceJobParameters;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages subscriptions to source job workers.
 */
public class SourceJobWorkerMetricsSubscription {
    private static final Logger logger = LoggerFactory.getLogger(SourceJobWorkerMetricsSubscription.class);

    private final List<SourceJobParameters.TargetInfo> targetInfos;
    private final MantisMasterClientApi masterClient;
    private final WorkerMetricsClient workerMetricsClient;
    private final AutoScaleMetricsConfig metricsConfig;

    public SourceJobWorkerMetricsSubscription(List<SourceJobParameters.TargetInfo> targetInfos,
                                              MantisMasterClientApi masterClient,
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
