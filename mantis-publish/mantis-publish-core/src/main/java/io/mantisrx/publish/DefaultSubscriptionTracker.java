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

package io.mantisrx.publish;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import com.netflix.spectator.ipc.http.HttpResponse;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.discovery.proto.StageWorkers;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSubscriptionTracker extends AbstractSubscriptionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSubscriptionTracker.class);

    private static final String SUBSCRIPTIONS_URL_FORMAT = "http://%s:%d?jobId=%s";
    private final MrePublishConfiguration mrePublishConfiguration;
    private final String subscriptionsFetchQueryParamString;
    private final Counter fetchSubscriptionsFailedCount;
    private final Counter fetchSubscriptionsNon200Count;
    private final HttpClient httpClient;
    private final MantisJobDiscovery jobDiscovery;
    private final Random random = new Random();

    public DefaultSubscriptionTracker(
            MrePublishConfiguration mrePublishConfiguration,
            Registry registry,
            MantisJobDiscovery jobDiscovery,
            StreamManager streamManager,
            HttpClient httpClient) {
        super(mrePublishConfiguration, registry, jobDiscovery, streamManager);
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.jobDiscovery = jobDiscovery;
        this.subscriptionsFetchQueryParamString = mrePublishConfiguration.subscriptionFetchQueryParams();
        this.httpClient = httpClient;

        this.fetchSubscriptionsFailedCount = SpectatorUtils.buildAndRegisterCounter(registry, "fetchSubscriptionsFailedCount");
        this.fetchSubscriptionsNon200Count = SpectatorUtils.buildAndRegisterCounter(registry, "fetchSubscriptionsNon200Count");
    }

    private Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String jobId, MantisWorker worker) {
        try {
            String uri;
            if (!subscriptionsFetchQueryParamString.isEmpty()) {
                uri = String.format(SUBSCRIPTIONS_URL_FORMAT, worker.getHost(), worker.getPort(), jobId + "&" + subscriptionsFetchQueryParamString);
            } else {
                uri = String.format(SUBSCRIPTIONS_URL_FORMAT, worker.getHost(), worker.getPort(), jobId);
            }
            LOG.trace("Subscription fetch URL: {}", uri);
            HttpResponse response = httpClient
                    .get(URI.create(uri))
                    .withConnectTimeout(1000)
                    .withReadTimeout(1000)
                    .send();
            if (response.status() == 200) {
                MantisServerSubscriptionEnvelope subscriptionEnvelope =
                        DefaultObjectMapper.getInstance().readValue(response.entityAsString(), MantisServerSubscriptionEnvelope.class);
                LOG.debug("got subs {} from Mantis worker {}", subscriptionEnvelope, worker);
                return Optional.ofNullable(subscriptionEnvelope);
            } else {
                LOG.info("got {} {} response from Mantis worker {}", response.status(), response.entityAsString(), worker);
                fetchSubscriptionsNon200Count.increment();
                return Optional.empty();
            }
        } catch (Exception e) {
            LOG.info("caught exception fetching subs from {}", worker, e);
            fetchSubscriptionsFailedCount.increment();
            return Optional.empty();
        }
    }

    private List<MantisWorker> randomSubset(List<MantisWorker> workers, int subsetSize) {
        int size = workers.size();
        if (subsetSize < 0 || subsetSize >= size) {
            return workers;
        }
        for (int idx = 0; idx < subsetSize; idx++) {
            int randomWorkerIdx = random.nextInt(size);
            MantisWorker tmp = workers.get(idx);
            workers.set(idx, workers.get(randomWorkerIdx));
            workers.set(randomWorkerIdx, tmp);
        }
        return workers.subList(0, subsetSize);
    }

    private Optional<MantisServerSubscriptionEnvelope> subsetSubscriptionsResolver(String jobId, List<MantisWorker> workers) {
        Map<MantisServerSubscriptionEnvelope, Integer> subCount = new HashMap<>();
        int numWorkers = workers.size();
        int maxWorkersToFetchSubsFrom = Math.min(mrePublishConfiguration.maxNumWorkersToFetchSubscriptionsFrom(), numWorkers);
        int threshold = maxWorkersToFetchSubsFrom / 2;

        List<MantisWorker> subset = randomSubset(workers, maxWorkersToFetchSubsFrom);
        for (final MantisWorker mantisWorker : subset) {
            Optional<MantisServerSubscriptionEnvelope> subscriptionsO = fetchSubscriptions(jobId, mantisWorker);
            if (subscriptionsO.isPresent()) {
                Integer prevCount;
                MantisServerSubscriptionEnvelope subscriptions = subscriptionsO.get();
                if ((prevCount = subCount.putIfAbsent(subscriptions, 0)) != null) {
                    subCount.put(subscriptions, prevCount + 1);
                    if (prevCount >= threshold) {
                        return Optional.ofNullable(subscriptions);
                    }
                }
            }
        }
        return subCount.entrySet().stream().max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    @Override
    public Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String jobCluster) {
        Optional<JobDiscoveryInfo> jobDiscoveryInfo = jobDiscovery.getCurrentJobWorkers(jobCluster);

        if (jobDiscoveryInfo.isPresent()) {
            JobDiscoveryInfo jdi = jobDiscoveryInfo.get();
            StageWorkers workers = jdi.getIngestStageWorkers();
            if (workers != null) {
                return subsetSubscriptionsResolver(jdi.getJobId(), workers.getWorkers());
            } else {
                LOG.info("Subscription refresh failed, workers null for {}", jobCluster);
            }
        } else {
            LOG.info("Subscription refresh failed, job discovery info not found for {}", jobCluster);
        }
        return Optional.empty();
    }
}
