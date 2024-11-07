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

package io.mantisrx.server.master.resourcecluster;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.post;

import com.spotify.futures.CompletableFutures;
import io.mantisrx.common.Ack;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder;
import org.asynchttpclient.Request;

@ToString(of = {"masterDescription", "clusterID"})
@Slf4j
public class ResourceClusterGatewayClient implements ResourceClusterGateway, Closeable {

  private final ClusterID clusterID;
  @Getter
  private final MasterDescription masterDescription;
  private AsyncHttpClient client;
  private final ObjectMapper mapper;

  public ResourceClusterGatewayClient(
      ClusterID clusterID,
      MasterDescription masterDescription,
      CoreConfiguration configuration) {
    this.clusterID = clusterID;
    this.masterDescription = masterDescription;
    this.mapper = new ObjectMapper();
    this.client = buildCloseableHttpClient(configuration);
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public CompletableFuture<Ack> registerTaskExecutor(TaskExecutorRegistration registration) {
    return performAction("registerTaskExecutor", registration);
  }

  @Override
  public CompletableFuture<Ack> heartBeatFromTaskExecutor(TaskExecutorHeartbeat heartbeat) {
    return performAction("heartBeatFromTaskExecutor", heartbeat);
  }

  @Override
  public CompletableFuture<Ack> notifyTaskExecutorStatusChange(
      TaskExecutorStatusChange taskExecutorStatusChange) {
    return performAction("notifyTaskExecutorStatusChange", taskExecutorStatusChange);
  }

  @Override
  public CompletableFuture<Ack> disconnectTaskExecutor(
      TaskExecutorDisconnection taskExecutorDisconnection) {
    return performAction("disconnectTaskExecutor", taskExecutorDisconnection);
  }

  private CompletableFuture<Ack> performAction(String action, Object body) {
    try {
      final String bodyStr = mapper.writeValueAsString(body);
      final Request request = post(
          getActionUri(action)).setBody(bodyStr).addHeader("Content-Type", "application/json").build();
      log.debug("request={}", request);
      return client.executeRequest(request).toCompletableFuture().thenCompose(response -> {
        if (response.getStatusCode() == 200) {
          return CompletableFuture.completedFuture(Ack.getInstance());
        }
        else if (response.getStatusCode() == 429) {
          log.warn("request was throttled on control plane side: {}", request);
          return CompletableFutures.exceptionallyCompletedFuture(
                  new RequestThrottledException("request was throttled on control plane side: " + request));
        }
        else {
          try {
            log.error("failed request {} with response {}", request, response.getResponseBody());
            return CompletableFutures.exceptionallyCompletedFuture(
                mapper.readValue(response.getResponseBody(), Throwable.class));
          } catch (Exception e) {
            return CompletableFutures.exceptionallyCompletedFuture(
                new Exception(String.format("response=%s", response), e));
          }
        }
      });
    } catch (Exception e) {
      return CompletableFutures.exceptionallyCompletedFuture(e);
    }
  }

  private String getActionUri(String action) {
    String uri = String.format("http://%s:%d/api/v1/resourceClusters/%s/actions/%s",
        masterDescription.getHostname(), masterDescription.getApiPort(), clusterID.getResourceID(),
        action);

    log.debug("uri={}", uri);
    return uri;
  }

  private AsyncHttpClient buildCloseableHttpClient(CoreConfiguration configuration) {
    return asyncHttpClient(
        new Builder()
            .setMaxConnections(configuration.getAsyncHttpClientMaxConnectionsPerHost())
            .setConnectTimeout(configuration.getAsyncHttpClientConnectionTimeoutMs())
            .setRequestTimeout(configuration.getAsyncHttpClientRequestTimeoutMs())
            .setReadTimeout(configuration.getAsyncHttpClientReadTimeoutMs())
            .setFollowRedirect(configuration.getAsyncHttpClientFollowRedirect())
            // set the http client thread priority to max - 1 to ensure control plane signals can still be retrieved
            // even when the worker is busy.
            .setThreadFactory(new DefaultThreadFactory(generateThreadPoolName(), Thread.MAX_PRIORITY - 1))
            .build());
  }

  private String generateThreadPoolName() {
      return String.format(
          "resourceClusterGatewayClient-httpclient-%s-%s",
          this.masterDescription.getHostname(),
          this.clusterID.getResourceID());
  }
}
