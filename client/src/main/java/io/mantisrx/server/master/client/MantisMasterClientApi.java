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

package io.mantisrx.server.master.client;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.common.Label;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.codec.JsonCodec;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.JobAssignmentResult;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.reactivex.mantis.remote.observable.ConnectToObservable;
import io.reactivex.mantis.remote.observable.DynamicConnectionSet;
import io.reactivex.mantis.remote.observable.ToDeltaEndpointInjector;
import io.reactivex.mantis.remote.observable.reconciliator.Reconciliator;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import mantis.io.reactivex.netty.protocol.http.websocket.WebSocketClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;


public class MantisMasterClientApi {

    static final String ConnectTimeoutSecsPropertyName = "MantisClientConnectTimeoutSecs";
    private static final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(MantisMasterClientApi.class);
    private static final String JOB_METADATA_FIELD = "jobMetadata";
    private static final String STAGE_MEDATA_LIST_FIELD = "stageMetadataList";
    private static final String STAGE_NUM_FIELD = "stageNum";
    private static final String NUM_STAGES_FIELD = "numStages";
    private static final int MAX_REDIRECTS = 10;
    private static final String API_JOBS_LIST_PATH = "/api/jobs/list";
    private static final String API_JOBS_LIST_MATCHING_PATH = "/api/jobs/list/matching";
    private static final String API_JOB_SUBMIT_PATH = "/api/submit";
    private static final String API_JOB_NAME_CREATE = "/api/namedjob/create";
    private static final String API_JOB_NAME_UPDATE = "/api/namedjob/update";
    private static final String API_JOB_NAME_LIST = "/api/namedjob/list";
    private static final String API_JOB_KILL = "/api/jobs/kill";
    private static final String API_JOB_STAGE_SCALE = "/api/jobs/scaleStage";
    private static final String API_JOB_RESUBMIT_WORKER = "/api/jobs/resubmitWorker";

    // Retry attempts before giving up in connection to master
    // each attempt waits attempt amount of time, 10=55 seconds
    private static final int SUBSCRIBE_ATTEMPTS_TO_MASTER = 100;
    private static final int MAX_RANDOM_WAIT_RETRY_SEC = 10;
    // The following timeout should be what's in master configuration's mantis.scheduling.info.observable.heartbeat.interval.secs
    private static final long MASTER_SCHED_INFO_HEARTBEAT_INTERVAL_SECS = 120;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new Jdk8Module());
    }

    final String DEFAULT_RESPONSE = "NO_RESPONSE_FROM_MASTER";
    private final long GET_TIMEOUT_SECS = 30;
    private final Observable<Endpoint> masterEndpoint;
    private final int subscribeAttemptsToMaster;
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic = attempts -> attempts
            .zipWith(Observable.range(1, Integer.MAX_VALUE), (Func2<Throwable, Integer, Integer>) (t1, integer) -> integer)
            .flatMap((Func1<Integer, Observable<?>>) integer -> {
                long delay = 2 * (integer > 10 ? 10 : integer);
                logger.info(": retrying conx after sleeping for " + delay + " secs");
                return Observable.timer(delay, TimeUnit.SECONDS);
            });
    private final Func1<Observable<? extends Void>, Observable<?>> repeatLogic = attempts -> attempts
            .zipWith(Observable.range(1, Integer.MAX_VALUE), (Func2<Void, Integer, Integer>) (t1, integer) -> integer)
            .flatMap((Func1<Integer, Observable<?>>) integer -> {
                long delay = 2 * (integer > 10 ? 10 : integer);
                logger.warn("On Complete received! : repeating conx after sleeping for " + delay + " secs");
                return Observable.timer(delay, TimeUnit.SECONDS);
            });
    private MasterMonitor masterMonitor;

    public MantisMasterClientApi(MasterMonitor masterMonitor) {
        this.masterMonitor = masterMonitor;
        masterEndpoint = masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .map(description -> {
                    logger.info("New Mantis Master notification, host: " + description.getHostname() + "," +
                            " swapping out client API connection to new master.");
                    return new Endpoint(description.getHostname(), description.getApiPortV2());
                });
        int a = SUBSCRIBE_ATTEMPTS_TO_MASTER;
        final String p = System.getProperty(ConnectTimeoutSecsPropertyName);
        if (p != null) {
            try {
                long t = Long.parseLong(p);
                a = Math.max(1, (int) Math.sqrt(2.0 * t)); // timeout = SUM(1 + 2 + ... + N) =~ (N^2)/2
            } catch (NumberFormatException e) {
                logger.warn("Invalid number for connectTimeoutSecs: " + p);
            }
        }
        //subscribeAttemptsToMaster = a;
        subscribeAttemptsToMaster = Integer.MAX_VALUE;
    }

    private String toUri(MasterDescription md, String path) {
        return "http://" + md.getHostname() + ":" + md.getApiPort() + path;
    }

    //
    //    public Observable<Void> createNamedJob(final CreateJobClusterRequest request) {
    //        return masterMonitor.getMasterObservable()
    //                .filter(md -> md != null)
    //                .switchMap((Func1<MasterDescription, Observable<Void>>) md -> {
    //                    try {
    //                        NamedJobDefinition namedJobDefinition = MantisProtoAdapter.toNamedJobDefinition(request);
    //                        String namedJobDefnJsonString = objectMapper.writeValueAsString(namedJobDefinition);
    //                        return getPostResponse(toUri(md, API_JOB_NAME_CREATE), namedJobDefnJsonString)
    //                                .onErrorResumeNext(t -> {
    //                                    logger.warn("Can't connect to master: {}", t.getMessage(), t);
    //                                    return Observable.empty();
    //                                })
    //                                .map(s -> {
    //                                    logger.info(s);
    //                                    return null;
    //                                });
    //                    } catch (JsonProcessingException | MalformedURLException e) {
    //                        return Observable.error(e);
    //                    }
    //                });
    //    }
    //
    //    public Observable<Void> updateNamedJob(final UpdateJobClusterRequest request) {
    //        return masterMonitor.getMasterObservable()
    //                .filter(md -> md != null)
    //                .switchMap((Func1<MasterDescription, Observable<Void>>) md -> {
    //                    try {
    //                        NamedJobDefinition namedJobDefinition = MantisProtoAdapter.toNamedJobDefinition(request);
    //                        String namedJobDefnJsonString = objectMapper.writeValueAsString(namedJobDefinition);
    //                        return getPostResponse(toUri(md, API_JOB_NAME_UPDATE), namedJobDefnJsonString)
    //                                .onErrorResumeNext(t -> {
    //                                    logger.warn("Can't connect to master: {}", t.getMessage(), t);
    //                                    return Observable.empty();
    //                                })
    //                                .map(s -> {
    //                                    logger.info(s);
    //                                    return null;
    //                                });
    //                    } catch (JsonProcessingException | MalformedURLException e) {
    //                        return Observable.error(e);
    //                    }
    //                });
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final SchedulingInfo schedulingInfo) {
    //        return submitJob(name, version, parameters, jobSla, 0L, schedulingInfo, WorkerMigrationConfig.DEFAULT);
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final long subscriptionTimeoutSecs,
    //                                                   final SchedulingInfo schedulingInfo,
    //                                                   final WorkerMigrationConfig migrationConfig) {
    //        return submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, false, migrationConfig);
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final long subscriptionTimeoutSecs,
    //                                                   final SchedulingInfo schedulingInfo) {
    //        return submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, false, WorkerMigrationConfig.DEFAULT);
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final long subscriptionTimeoutSecs,
    //                                                   final SchedulingInfo schedulingInfo,
    //                                                   final boolean readyForJobMaster) {
    //        return submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, readyForJobMaster, WorkerMigrationConfig.DEFAULT);
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final long subscriptionTimeoutSecs,
    //                                                   final SchedulingInfo schedulingInfo,
    //                                                   final boolean readyForJobMaster,
    //                                                   final WorkerMigrationConfig migrationConfig) {
    //        return submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, readyForJobMaster, migrationConfig, Lists.newLinkedList());
    //    }
    //
    //    /**
    //     * @deprecated use {{@link #submitJob(SubmitJobRequest)}} instead
    //     */
    //    @Deprecated
    //    public Observable<JobSubmitResponse> submitJob(final String name, final String version,
    //                                                   final List<Parameter> parameters,
    //                                                   final JobSla jobSla,
    //                                                   final long subscriptionTimeoutSecs,
    //                                                   final SchedulingInfo schedulingInfo,
    //                                                   final boolean readyForJobMaster,
    //                                                   final WorkerMigrationConfig migrationConfig,
    //                                                   final List<Label> labels) {
    //        try {
    //            String jobDef = getJobDefinitionString(name, null, version, parameters, jobSla,
    //                    subscriptionTimeoutSecs, schedulingInfo, readyForJobMaster, migrationConfig, labels);
    //            return submitJob(jobDef);
    //        } catch (MalformedURLException | JsonProcessingException e) {
    //            return Observable.error(e);
    //        }
    //    }
    //
    //    public Observable<JobSubmitResponse> submitJob(final SubmitJobRequest submitJobRequest) {
    //        try {
    //            String submitJobReqJsonStr = JsonFormat.printer().includingDefaultValueFields().print(submitJobRequest);
    //            return submitJob(submitJobReqJsonStr);
    //        } catch (InvalidProtocolBufferException e) {
    //            return Observable.error(e);
    //        }
    //    }
    //
    //    public Observable<JobSubmitResponse> submitJob(final JobDefinition jobDefinition) {
    //        try {
    //            String submitJobReqJsonStr = JsonFormat.printer().includingDefaultValueFields().print(jobDefinition);
    //            return submitJob(submitJobReqJsonStr);
    //        } catch (InvalidProtocolBufferException e) {
    //            return Observable.error(e);
    //        }
    //    }

    private Observable<JobSubmitResponse> submitJob(final String submitJobRequestJson) {
        return masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .switchMap((Func1<MasterDescription, Observable<JobSubmitResponse>>) masterDescription -> {
                    String uri = "http://" + masterDescription.getHostname() + ":" +
                            masterDescription.getApiPort() + API_JOB_SUBMIT_PATH;
                    logger.info("Doing POST on " + uri);
                    try {
                        return getPostResponse(uri, submitJobRequestJson)
                                .onErrorResumeNext(throwable -> {
                                    logger.warn("Can't connect to master: {}", throwable.getMessage(), throwable);
                                    return Observable.empty();
                                })
                                .map(s -> new JobSubmitResponse(s, false, null));
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                });
    }

    private String getJobDefinitionString(String name, String jobUrl, String version, List<Parameter> parameters,
                                          JobSla jobSla, long subscriptionTimeoutSecs, SchedulingInfo schedulingInfo,
                                          boolean readyForJobMaster, final WorkerMigrationConfig migrationConfig, final List<Label> labels)
            throws JsonProcessingException, MalformedURLException {
        MantisJobDefinition jobDefinition = new MantisJobDefinition(name, System.getProperty("user.name"),
                jobUrl == null ? null : new URL(jobUrl),
                version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, 0, 0, null, null, readyForJobMaster, migrationConfig, labels);
        return objectMapper.writeValueAsString(jobDefinition);
    }

    public Observable<Void> killJob(final String jobId) {
        return killJob(jobId, "Unknown", "User requested");
    }

    public Observable<Void> killJob(final String jobId, final String user, final String reason) {
        return masterMonitor.getMasterObservable()
                .filter(md -> md != null)
                .switchMap((Func1<MasterDescription, Observable<Void>>) md -> {
                    Map<String, String> content = new HashMap<>();
                    content.put("JobId", jobId);
                    content.put("user", user);
                    content.put("reason", reason);
                    try {
                        return getPostResponse(toUri(md, API_JOB_KILL), objectMapper.writeValueAsString(content))
                                .onErrorResumeNext(throwable -> {
                                    logger.warn("Can't connect to master: {}", throwable.getMessage(), throwable);
                                    return Observable.empty();
                                })
                                .map(s -> {
                                    logger.info(s);
                                    return null;
                                });
                    } catch (JsonProcessingException e) {
                        return Observable.error(e);
                    }
                });
    }

    public Observable<Boolean> scaleJobStage(final String jobId, final int stageNum, final int numWorkers, final String reason) {
        return masterMonitor
                .getMasterObservable()
                .filter(md -> md != null)
                .take(1)
                .flatMap((Func1<MasterDescription, Observable<Boolean>>) md -> {
                    final StageScaleRequest stageScaleRequest = new StageScaleRequest(jobId, stageNum, numWorkers, reason);
                    try {
                        return submitPostRequest(toUri(md, API_JOB_STAGE_SCALE), objectMapper.writeValueAsString(stageScaleRequest))
                                .map(s -> {
                                    logger.info("POST to scale stage returned status: {}", s);
                                    return s.codeClass().equals(HttpStatusClass.SUCCESS);
                                });
                    } catch (JsonProcessingException e) {
                        logger.error("failed to serialize stage scale request {} to json", stageScaleRequest);
                        return Observable.error(e);
                    }
                });
    }

    public Observable<Boolean> resubmitJobWorker(final String jobId, final String user, final int workerNum, final String reason) {
        return masterMonitor.getMasterObservable()
                .filter(md -> md != null)
                .take(1)
                .flatMap((Func1<MasterDescription, Observable<Boolean>>) md -> {
                    final ResubmitJobWorkerRequest resubmitJobWorkerRequest = new ResubmitJobWorkerRequest(jobId, user, workerNum, reason);
                    logger.info("sending request to resubmit worker {} for jobId {}", workerNum, jobId);
                    try {
                        return submitPostRequest(toUri(md, API_JOB_RESUBMIT_WORKER), objectMapper.writeValueAsString(resubmitJobWorkerRequest))
                                .map(s -> {
                                    logger.info("POST to resubmit worker {} returned status: {}", workerNum, s);
                                    return s.codeClass().equals(HttpStatusClass.SUCCESS);
                                });
                    } catch (JsonProcessingException e) {
                        logger.error("failed to serialize resubmit job worker request {} to json", resubmitJobWorkerRequest);
                        return Observable.error(e);
                    }
                });
    }

    private Observable<HttpResponseStatus> submitPostRequest(String uri, String postContent) {
        logger.info("sending POST request to {} content {}", uri, postContent);
        return RxNetty
                .createHttpRequest(
                        HttpClientRequest.createPost(uri)
                                .withContent(postContent),
                        new HttpClient.HttpClientConfig.Builder()
                                .build())
                .map(b -> b.getStatus());
    }

    private Observable<String> getPostResponse(String uri, String postContent) {
        logger.info("sending POST request to {} content {}", uri, postContent);
        return RxNetty
                .createHttpRequest(
                        HttpClientRequest.createPost(uri)
                                .withContent(postContent),
                        new HttpClient.HttpClientConfig.Builder()
                                .build())
                .flatMap((Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>) b -> b.getContent())
                .map(o -> o.toString(Charset.defaultCharset()));
    }

    //    private Observable<String> getGetResponse(final String uri) {
    //        return RxNetty
    //                .createHttpRequest(HttpClientRequest.createGet(uri), new HttpClient.HttpClientConfig.Builder()
    //                        .setFollowRedirect(true).followRedirect(MAX_REDIRECTS).build())
    //                .lift(new OperatorOnErrorResumeNextViaFunction<>(new Func1<Throwable, Observable<? extends HttpClientResponse<ByteBuf>>>() {
    //                    @Override
    //                    public Observable<? extends HttpClientResponse<ByteBuf>> call(Throwable t) {
    //                        return Observable.error(t);
    //                    }
    //                }))
    //                .timeout(GET_TIMEOUT_SECS, TimeUnit.SECONDS)
    //                .retryWhen(retryLogic)
    //                .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>() {
    //                    @Override
    //                    public Observable<ByteBuf> call(HttpClientResponse<ByteBuf> r) {
    //                        return r.getContent();
    //                    }
    //                })
    //                .map(new Func1<ByteBuf, String>() {
    //                    @Override
    //                    public String call(ByteBuf o) {
    //                        return o.toString(Charset.defaultCharset());
    //                    }
    //                });
    //    }

    public Observable<Boolean> namedJobExists(final String jobName) {
        return masterMonitor.getMasterObservable()
                .filter(md -> md != null)
                .switchMap((Func1<MasterDescription, Observable<Boolean>>) masterDescription -> {
                    String uri = API_JOB_NAME_LIST + "/" + jobName;
                    logger.info("Calling GET on " + uri);
                    return HttpUtility.getGetResponse(masterDescription.getHostname(), masterDescription.getApiPort(), uri)
                            .onErrorResumeNext(throwable -> {
                                logger.warn("Can't connect to master: {}", throwable.getMessage(), throwable);
                                return Observable.error(throwable);
                            })
                            .map(response -> {
                                logger.debug("Job cluster response: " + response);
                                JSONArray jsonArray = new JSONArray(response);
                                return jsonArray.length() > 0;
                            })
                            .retryWhen(retryLogic)
                            ;
                })
                .retryWhen(retryLogic)
                ;
    }

    public Observable<Integer> getSinkStageNum(final String jobId) {
        return masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .switchMap(masterDescription -> {
                    String uri = API_JOBS_LIST_PATH + "/" + jobId;
                    logger.info("Calling GET on " + uri);
                    return HttpUtility.getGetResponse(masterDescription.getHostname(), masterDescription.getApiPort(), uri)
                            .onErrorResumeNext(throwable -> {
                                logger.warn("Can't connect to master: {}", throwable.getMessage(), throwable);
                                return Observable.error(throwable);
                            })
                            .flatMap(response -> {
                                try {
                                    logger.info("Got response for job info on " + jobId);
                                    Integer sinkStage = getSinkStageNumFromJsonResponse(jobId, response);
                                    if (sinkStage < 0) {
                                        logger.warn("Job " + jobId + " not found");
                                        return Observable.empty();
                                    }
                                    return Observable.just(sinkStage);
                                } catch (MasterClientException e) {
                                    logger.warn("Can't get sink stage info for " + jobId + ": " + e.getMessage());
                                    return Observable.empty();
                                }
                            })
                            .retryWhen(retryLogic)
                            ;
                });
    }

    // returns json array of job metadata
    public Observable<String> getJobsOfNamedJob(final String jobName, final MantisJobState.MetaState state) {
        return masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .switchMap(masterDescription -> {
                    String uri = API_JOBS_LIST_MATCHING_PATH + "/" + jobName;
                    if (state != null)
                        uri = uri + "?jobState=" + state;
                    logger.info("Calling GET on " + uri);
                    return HttpUtility.getGetResponse(masterDescription.getHostname(), masterDescription.getApiPort(), uri)
                            .onErrorResumeNext(throwable -> {
                                logger.warn("Can't connect to master: {}", throwable.getMessage(), throwable);
                                return Observable.empty();
                            });
                })
                .retryWhen(retryLogic)
                ;
    }

    private Integer getSinkStageNumFromJsonResponse(String jobId, String response) throws MasterClientException {
        final String throwMessage = "Can't parse json response for job " + jobId;
        if (response == null) {
            logger.warn("Null info response from master for job " + jobId);
            throw new MasterClientException(throwMessage);
        }
        try {
            JSONObject jsonObject = new JSONObject(response);
            JSONObject jobMetadata = jsonObject.optJSONObject(JOB_METADATA_FIELD);
            if (jobMetadata == null) {
                logger.warn("Didn't find meta data for job " + jobId + " in json (" + response + ")");
                return -1;
            }
            String state = jobMetadata.optString("state");
            if (state == null) {
                throw new MasterClientException("Can't read job state in response (" + response + ")");
            }
            if (MantisJobState.isTerminalState(MantisJobState.valueOf(state))) {
                logger.info("Can't get sink stage of job in state " + MantisJobState.valueOf(state));
                return -1;
            }
            int lastStage = 0;
            JSONArray stages = jsonObject.optJSONArray(STAGE_MEDATA_LIST_FIELD);
            if (stages == null) {
                logger.warn("Didn't find stages metadata for job " + jobId + " in json: " + response);
                throw new MasterClientException(throwMessage);
            }
            for (int i = 0; i < stages.length(); i++) {
                final JSONObject s = stages.getJSONObject(i);
                final int n = s.optInt(STAGE_NUM_FIELD, 0);
                lastStage = Math.max(lastStage, n);
            }
            if (lastStage == 0) {
                logger.warn("Didn't find " + STAGE_NUM_FIELD + " field in stage metadata json (" + response + ")");
                throw new MasterClientException(throwMessage);
            }
            logger.info("Got sink stage number for job " + jobId + ": " + lastStage);
            return lastStage;
        } catch (JSONException e) {
            logger.error("Error parsing info for job " + jobId + " from json data (" + response + "): " + e.getMessage());
            throw new MasterClientException(throwMessage);
        }
    }

    // Keep until confirming new method works in prod
    //    public Observable<JobSchedulingInfo> schedulingChangesOld(String jobId) {
    //        Map<String,String> subscriptionParams = new HashMap<String,String>();
    //        subscriptionParams.put("jobId", jobId);
    //
    //        final ConnectToObservable.Builder<JobSchedulingInfo> connectionBuilder =
    //                new ConnectToObservable.Builder<JobSchedulingInfo>()
    //                        .subscribeAttempts(subscribeAttemptsToMaster)
    //                        .name("/v1/api/master/schedulingchanges")
    //                        .subscribeParameters(subscriptionParams)
    //                        .decoder(new Decoder<JobSchedulingInfo>() {
    //                            @Override
    //                            public JobSchedulingInfo decode(byte[] bytes) {
    //                                try {
    //                                    final String data = new String(bytes, Charset.defaultCharset());
    //                                    return objectMapper.readValue(data, JobSchedulingInfo.class);
    //                                } catch (Exception e) {
    //                                    logger.warn("SchedulingInfoBytes: " + new String(bytes));
    //                                    logger.error("Error parsing scheduling info: " + e.getMessage());
    //                                    throw new RuntimeException(e);
    //                                }
    //                            }
    //                        })
    //                ;
    //
    //        Observable<List<Endpoint>> changes = masterEndpoint
    //                .map(new Func1<Endpoint,List<Endpoint>>(){
    //                    @Override
    //                    public List<Endpoint> call(Endpoint t1) {
    //                        List<Endpoint> list = new ArrayList<Endpoint>(1);
    //                        list.add(t1);
    //                        return list;
    //                    }
    //                });
    //
    //        Reconciliator<JobSchedulingInfo> reconciliator = new Reconciliator.Builder<JobSchedulingInfo>()
    //                .name("master-schedulingChanges")
    //                .connectionSet(DynamicConnectionSet.create(connectionBuilder, MAX_RANDOM_WAIT_RETRY_SEC))
    //                .injector(new ToDeltaEndpointInjector(changes))
    //                .build();
    //
    //        return Observable.merge(reconciliator.observables());
    //    }

    private HttpClient<ByteBuf, ServerSentEvent> getRxnettySseClient(String hostname, int port) {
        return RxNetty.<ByteBuf, ServerSentEvent>newHttpClientBuilder(hostname, port)
                .pipelineConfigurator(PipelineConfigurators.clientSseConfigurator())
                //.enableWireLogging(LogLevel.ERROR)
                .withNoConnectionPooling().build();
    }

    private WebSocketClient<TextWebSocketFrame, TextWebSocketFrame> getRxnettyWebSocketClient(String host, int port, String uri) {
        return
                RxNetty.<TextWebSocketFrame, TextWebSocketFrame>newWebSocketClientBuilder(host, port)
                        .withWebSocketURI(uri)
                        //      .withWebSocketVersion(WebSocketVersion.V13)
                        .build();
    }

    public Observable<String> getJobStatusObservable(final String jobId) {
        return masterMonitor.getMasterObservable()
                .filter((md) -> md != null)
                .retryWhen(retryLogic)
                .switchMap((md) -> getRxnettyWebSocketClient(md.getHostname(), md.getConsolePort(), "/job/status/" + jobId)
                        .connect()
                        .flatMap((ObservableConnection<TextWebSocketFrame, TextWebSocketFrame> connection) -> connection.getInput()
                                .map((TextWebSocketFrame webSocketFrame) -> webSocketFrame.text())));
    }

    public Observable<JobSchedulingInfo> schedulingChanges(final String jobId) {
        return masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .retryWhen(retryLogic)
                .switchMap((Func1<MasterDescription, Observable<JobSchedulingInfo>>) masterDescription -> getRxnettySseClient(masterDescription.getHostname(), masterDescription.getSchedInfoPort())
                        .submit(HttpClientRequest.createGet("/assignmentresults/" + jobId + "?sendHB=true"))
                        .flatMap((Func1<HttpClientResponse<ServerSentEvent>, Observable<JobSchedulingInfo>>) response -> {
                            if (!HttpResponseStatus.OK.equals(response.getStatus())) {
                                return Observable.error(new Exception(response.getStatus().reasonPhrase()));
                            }
                            return response.getContent()
                                    .map(event -> {
                                        try {
                                            return objectMapper.readValue(event.contentAsString(), JobSchedulingInfo.class);
                                        } catch (IOException e) {
                                            throw new RuntimeException("Invalid schedInfo json: " + e.getMessage(), e);
                                        }
                                    })
                                    .timeout(3 * MASTER_SCHED_INFO_HEARTBEAT_INTERVAL_SECS, TimeUnit.SECONDS)
                                    .filter(schedulingInfo -> schedulingInfo != null && !JobSchedulingInfo.HB_JobId.equals(schedulingInfo.getJobId()))
                                    ;
                        }))
                .repeatWhen(repeatLogic)
                .retryWhen(retryLogic)
                ;
    }

    public Observable<NamedJobInfo> namedJobInfo(final String jobName) {
        return masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .retryWhen(retryLogic)
                .switchMap((Func1<MasterDescription, Observable<NamedJobInfo>>) masterDescription -> getRxnettySseClient(masterDescription.getHostname(), masterDescription.getSchedInfoPort())
                        .submit(HttpClientRequest.createGet("/namedjobs/" + jobName + "?sendHB=true"))
                        .flatMap((Func1<HttpClientResponse<ServerSentEvent>, Observable<NamedJobInfo>>) response -> {
                            if (!HttpResponseStatus.OK.equals(response.getStatus()))
                                return Observable.error(new Exception(response.getStatus().reasonPhrase()));
                            return response.getContent()
                                    .map(event -> {
                                        try {
                                            return objectMapper.readValue(event.contentAsString(), NamedJobInfo.class);
                                        } catch (IOException e) {
                                            throw new RuntimeException("Invalid namedJobInfo json: " + e.getMessage(), e);
                                        }
                                    })
                                    .timeout(3 * MASTER_SCHED_INFO_HEARTBEAT_INTERVAL_SECS, TimeUnit.SECONDS)
                                    .filter(namedJobInfo -> namedJobInfo != null && !JobSchedulingInfo.HB_JobId.equals(namedJobInfo.getName()))
                                    ;
                        }))
                .repeatWhen(repeatLogic)
                .retryWhen(retryLogic)
                ;
    }

    // Keep until confirming new method works in prod
    //    public Observable<NamedJobInfo> namedJobInfoOld(String jobName) {
    //        Map<String,String> subscriptionParams = new HashMap<String,String>();
    //        subscriptionParams.put("jobName", jobName);
    //        ConnectToObservable.Builder<NamedJobInfo> connectionBuilder =
    //                new ConnectToObservable.Builder<NamedJobInfo>()
    //                        .subscribeAttempts(subscribeAttemptsToMaster)
    //                        .name("/v1/api/master/namedjobs")
    //                        .subscribeParameters(subscriptionParams)
    //                        .decoder(new JsonCodec<NamedJobInfo>(NamedJobInfo.class));
    //        logger.info("Creating dynamic connection for getting scheduling info");
    //
    //        Observable<List<Endpoint>> changes = masterEndpoint
    //                .map(new Func1<Endpoint,List<Endpoint>>(){
    //                    @Override
    //                    public List<Endpoint> call(Endpoint t1) {
    //                        List<Endpoint> list = new ArrayList<Endpoint>(1);
    //                        list.add(t1);
    //                        return list;
    //                    }
    //                });
    //
    //        Reconciliator<NamedJobInfo> reconciliator = new Reconciliator.Builder<NamedJobInfo>()
    //                .name("master-namedJobInfo")
    //                .connectionSet(DynamicConnectionSet.create(connectionBuilder, MAX_RANDOM_WAIT_RETRY_SEC))
    //                .injector(new ToDeltaEndpointInjector(changes))
    //                .build();
    //
    //        return Observable.merge(reconciliator.observables());
    //    }

    public Observable<JobAssignmentResult> assignmentResults(String jobId) {
        ConnectToObservable.Builder<JobAssignmentResult> connectionBuilder =
                new ConnectToObservable.Builder<JobAssignmentResult>()
                        .subscribeAttempts(subscribeAttemptsToMaster)
                        .name("/v1/api/master/assignmentresults")
                        .decoder(new JsonCodec<JobAssignmentResult>(JobAssignmentResult.class));
        if (jobId != null && !jobId.isEmpty()) {
            Map<String, String> subscriptionParams = new HashMap<>();
            subscriptionParams.put("jobId", jobId);
            connectionBuilder = connectionBuilder.subscribeParameters(subscriptionParams);
        }
        Observable<List<Endpoint>> changes = masterEndpoint
                .map(t1 -> {
                    List<Endpoint> list = new ArrayList<>(1);
                    list.add(t1);
                    return list;
                });

        Reconciliator<JobAssignmentResult> reconciliator = new Reconciliator.Builder<JobAssignmentResult>()
                .name("master-jobAssignmentResults")
                .connectionSet(DynamicConnectionSet.create(connectionBuilder, MAX_RANDOM_WAIT_RETRY_SEC))
                .injector(new ToDeltaEndpointInjector(changes))
                .build();

        return Observable.merge(reconciliator.observables());
    }

}
