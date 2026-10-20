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

package io.mantisrx.publish.internal.discovery.mantisapi;

import com.netflix.spectator.ipc.http.HttpClient;
import com.netflix.spectator.ipc.http.HttpResponse;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.discovery.proto.AppJobClustersMap;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.discovery.proto.StageWorkers;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.proto.JobSchedulingInfo;
import io.mantisrx.publish.internal.discovery.proto.MantisJobState;
import io.mantisrx.publish.internal.discovery.proto.WorkerAssignments;
import io.mantisrx.publish.internal.discovery.proto.WorkerHost;
import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.exceptions.RetryableException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMantisApiClient implements MantisApiClient {

    private static final int CONNECT_TIMEOUT_MS = 1_000;
    private static final int READ_TIMEOUT_MS = 1_000;
    private static final int MAX_LOGGED_BODY_CHARS = 200;
    private static final Logger logger = LoggerFactory.getLogger(DefaultMantisApiClient.class);
    private static final String JOB_CLUSTER_MAPPING_URL_FORMAT = "http://%s:%d/api/v1/mantis/publish/streamJobClusterMap";
    private static final String JOB_DISCOVERY_URL_FORMAT = "http://%s:%d/jobClusters/discoveryInfo/%s";
    private static final String JOB_DISCOVERY_STREAM_URL_FORMAT = "http://%s:%d/assignmentresults/%s";
    private final JsonSerializer serializer = new JsonSerializer();
    private final MrePublishConfiguration mrePublishConfiguration;
    private final HttpClient httpClient;

    public DefaultMantisApiClient(MrePublishConfiguration mrePublishConfiguration, HttpClient client) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.httpClient = client;
    }

    @Override
    public CompletableFuture<AppJobClustersMap> getJobClusterMapping(final Optional<String> app) {
        return CompletableFuture.supplyAsync(() -> {

            StringBuilder uriBuilder = new StringBuilder(String.format(JOB_CLUSTER_MAPPING_URL_FORMAT, mrePublishConfiguration.discoveryApiHostname(), mrePublishConfiguration.discoveryApiPort()));
            app.ifPresent(appName -> uriBuilder.append("?app=").append(appName));
            String uri = uriBuilder.toString();
            logger.debug("job cluster mapping fetch url {}", uri);
            try {
                HttpResponse response = httpClient.get(URI.create(uri))
                        .withConnectTimeout(CONNECT_TIMEOUT_MS)
                        .withReadTimeout(READ_TIMEOUT_MS)
                        .send();
                int status = response.status();
                if (status >= 200 && status < 300) {
                    AppJobClustersMap appJobClustersMap = serializer.fromJSON(response.entityAsString(), AppJobClustersMap.class);
                    logger.debug("AppJobClustersMap: {}", appJobClustersMap);
                    return appJobClustersMap;
                } else {
                    throw failureFor(response, "job cluster mapping", String.valueOf(app));
                }
            } catch (JsonProcessingException e) {
                // a 2xx with a non-JSON body (e.g. a proxy or master that is not ready yet) is transient
                logger.warn("unparseable response from api for {}, will retry: {}", uri, e.getOriginalMessage());
                throw new CompletionException(new RetryableException("Unparseable response from " + uri + ": " + e.getOriginalMessage()));
            } catch (IOException e) {
                logger.warn("caught exception", e);
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<JobDiscoveryInfo> jobDiscoveryInfo(final String jobClusterName) {
        return CompletableFuture.supplyAsync(() -> {
            String uri = String.format(JOB_DISCOVERY_URL_FORMAT, mrePublishConfiguration.discoveryApiHostname(), mrePublishConfiguration.discoveryApiPort(), jobClusterName);
            logger.debug("discovery info fetch url {}", uri);
            try {
                HttpResponse response = httpClient.get(URI.create(uri))
                        .withConnectTimeout(CONNECT_TIMEOUT_MS)
                        .withReadTimeout(READ_TIMEOUT_MS)
                        .send();
                int status = response.status();
                if (status >= 200 && status < 300) {
                    JobSchedulingInfo jobSchedulingInfo = serializer.fromJSON(response.entityAsString(), JobSchedulingInfo.class);
                    JobDiscoveryInfo jobDiscoveryInfo = convertJobSchedInfo(jobSchedulingInfo, jobClusterName);
                    logger.debug(jobDiscoveryInfo.toString());
                    return jobDiscoveryInfo;
                } else {
                    throw failureFor(response, "job discovery", jobClusterName);
                }
            } catch (JsonProcessingException e) {
                // a 2xx with a non-JSON body (e.g. a proxy or master that is not ready yet) is transient
                logger.warn("unparseable response from api for {}, will retry: {}", uri, e.getOriginalMessage());
                throw new CompletionException(new RetryableException("Unparseable response from " + uri + ": " + e.getOriginalMessage()));
            } catch (IOException e) {
                logger.warn("caught exception", e);
                throw new CompletionException(e);
            }
        });
    }

    /**
     * 3xx (e.g. a non-leader master redirecting to the leader) and 5xx (e.g. master not ready yet) are transient and
     * retryable. 4xx indicates a problem with the request itself and will not resolve by retrying.
     */
    private CompletionException failureFor(HttpResponse response, String request, String target) {
        int status = response.status();
        String body = truncate(response.entityAsString());
        String message = "Failed to get " + request + " info for " + target + " status " + status + " body: " + body;
        if (status >= 400 && status < 500) {
            logger.error("got {} response from api on {} request for {}, body: {}", status, request, target, body);
            return new CompletionException(new NonRetryableException(message));
        }
        logger.warn("got {} response from api on {} request for {}, body: {}", status, request, target, body);
        return new CompletionException(new RetryableException(message));
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= MAX_LOGGED_BODY_CHARS ? s : s.substring(0, MAX_LOGGED_BODY_CHARS) + "...";
    }

    private JobDiscoveryInfo convertJobSchedInfo(JobSchedulingInfo jobSchedulingInfo, String jobClusterName) {
        Map<Integer, StageWorkers> jobWorkers = new HashMap<>();
        for (Map.Entry<Integer, WorkerAssignments> e : jobSchedulingInfo.getWorkerAssignments().entrySet()) {
            Integer stageNum = e.getKey();
            WorkerAssignments workerAssignments = e.getValue();
            List<MantisWorker> workerList = new ArrayList<>(workerAssignments.getHosts().size());
            for (WorkerHost w : workerAssignments.getHosts().values()) {
                if (MantisJobState.Started.equals(w.getState())) {
                    workerList.add(new MantisWorker(w.getHost(), w.getCustomPort()));
                }
            }
            jobWorkers.put(stageNum, new StageWorkers(jobClusterName, jobSchedulingInfo.getJobId(), stageNum, workerList));
        }

        return new JobDiscoveryInfo(jobClusterName, jobSchedulingInfo.getJobId(), jobWorkers);
    }

}
