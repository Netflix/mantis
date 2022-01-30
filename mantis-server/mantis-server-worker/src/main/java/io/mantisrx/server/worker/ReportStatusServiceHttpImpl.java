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

package io.mantisrx.server.worker;

/**
 * The goal of this class is to essentially publish status / heartbeat values to the mantis master
 * periodically so that the mantis master is aware of the stage's current state.
 */
//public class ReportStatusServiceHttpImpl extends BaseReportStatusService {
//
//  private static final Logger logger = LoggerFactory.getLogger(ReportStatusServiceHttpImpl.class);
//  private final MantisMasterGateway masterGateway;
//
//
//  public ReportStatusServiceHttpImpl(
//      MantisMasterGateway masterGateway,
//      Observable<Observable<Status>> tasksStatusSubject) {
//    super(tasksStatusSubject);
//    this.masterGateway = masterGateway;
//  }
//
//  @Override
//  public CompletableFuture<Ack> updateTaskExecutionStatus(Status status) {
//  }

//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

//        final MantisPropertiesService mantisPropertiesService = ServiceRegistry.INSTANCE.getPropertiesService();
//        this.defaultConnTimeout = Integer.valueOf(mantisPropertiesService.getStringValue(
//                "mantis.worker.heartbeat.connection.timeout.ms",
//                "100"));
//        this.defaultSocketTimeout = Integer.valueOf(mantisPropertiesService.getStringValue(
//                "mantis.worker.heartbeat.socket.timeout.ms",
//                "1000"));
//        this.defaultConnMgrTimeout = Integer.valueOf(mantisPropertiesService.getStringValue(
//                "mantis.worker.heartbeat.connectionmanager.timeout.ms",
//                "2000"));
//        try (CloseableHttpClient defaultHttpClient = buildCloseableHttpClient()) {
//            HttpPost post = new HttpPost(masterMonitor.getLatestMaster().getFullApiStatusUri());
//            String statusUpdate = mapper.writeValueAsString(new PostJobStatusRequest(status.getJobId(), status));
//            post.setEntity(new StringEntity(statusUpdate));
//            org.apache.http.HttpResponse response = defaultHttpClient.execute(post);
//            int code = response.getStatusLine().getStatusCode();
//            if (code != 200) {
//                logger.info("Non 200 response: " + code + ", from master with state: " + status.getState()
//                    + " for heartbeat request at URI: " + masterMonitor.getLatestMaster().getFullApiStatusUri() + " with post data: " + statusUpdate);
//                if (code > 299) {
//                    for (Header header : response.getAllHeaders()) {
//                        logger.info("Response Header: [" + header.getName() + "=" + header.getValue() + "]");
//                    }
//                }
//                return CompletableFutures.exceptionallyCompletedFuture(
//                    new HttpResponseException(code, response.getStatusLine().getReasonPhrase()));
//            } else {
//                workerSentHeartbeats.increment();
//                return CompletableFuture.completedFuture(Ack.getInstance());
//            }
//        } catch (SocketTimeoutException e) {
//            logger.warn("SocketTimeoutException: Failed to send status update", e);
//            hbSocketTimeoutCounter.increment();
//            return CompletableFutures.exceptionallyCompletedFuture(e);
//        } catch (ConnectionPoolTimeoutException e) {
//            logger.warn("ConnectionPoolTimeoutException: Failed to send status update", e);
//            hbConnectionRequestTimeoutCounter.increment();
//            return CompletableFutures.exceptionallyCompletedFuture(e);
//        } catch (ConnectTimeoutException e) {
//            logger.warn("ConnectTimeoutException: Failed to send status update", e);
//            hbConnectionTimeoutCounter.increment();
//            return CompletableFutures.exceptionallyCompletedFuture(e);
//        } catch (IOException e) {
//            logger.warn("Failed to send status update", e);
//            return CompletableFutures.exceptionallyCompletedFuture(e);
//        }
//  private CloseableHttpClient buildCloseableHttpClient() {
//    return HttpClients
//        .custom()
//        .setRedirectStrategy(new LaxRedirectStrategy())
//        .disableAutomaticRetries()
//        .setDefaultRequestConfig(
//            RequestConfig
//                .custom()
//                .setConnectTimeout(defaultConnTimeout)
//                .setConnectionRequestTimeout(defaultConnMgrTimeout)
//                .setSocketTimeout(defaultSocketTimeout)
//                .build())
//        .build();
//  }
//}
