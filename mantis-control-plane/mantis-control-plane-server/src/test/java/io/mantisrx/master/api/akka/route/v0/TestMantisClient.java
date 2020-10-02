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

package io.mantisrx.master.api.akka.route.v0;

import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.core.master.MasterDescription;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TestMantisClient {
    private static final Logger logger = LoggerFactory.getLogger(TestMantisClient.class);
    private final int serverPort;

    public TestMantisClient(final int serverPort) {
        this.serverPort = serverPort;
    }

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

    private HttpClient<ByteBuf, ServerSentEvent> getRxnettySseClient(String hostname, int port) {
        return RxNetty.<ByteBuf, ServerSentEvent>newHttpClientBuilder(hostname, port)
            .pipelineConfigurator(PipelineConfigurators.<ByteBuf>clientSseConfigurator())
//            .enableWireLogging(LogLevel.INFO)
            .withNoConnectionPooling().build();
    }

    public Observable<JobSchedulingInfo> schedulingChanges(final String jobId,
                                                           final Func1<Observable<? extends Throwable>, Observable<?>> retryFn,
                                                           final Func1<Observable<? extends Void>, Observable<?>> repeatFn) {
        return Observable.just(new MasterDescription("localhost", "127.0.0.1", serverPort, serverPort,
            serverPort, "/api/postjobstatus", serverPort, System.currentTimeMillis()))
            .retryWhen(retryFn)
            .switchMap(new Func1<MasterDescription, Observable<JobSchedulingInfo>>() {
                @Override
                public Observable<JobSchedulingInfo> call(MasterDescription masterDescription) {
                    return getRxnettySseClient(masterDescription.getHostname(), masterDescription.getSchedInfoPort())
                        .submit(HttpClientRequest.createGet("/assignmentresults/" + jobId + "?sendHB=true"))
                        .flatMap(new Func1<HttpClientResponse<ServerSentEvent>, Observable<JobSchedulingInfo>>() {
                            @Override
                            public Observable<JobSchedulingInfo> call(HttpClientResponse<ServerSentEvent> response) {
                                if (!HttpResponseStatus.OK.equals(response.getStatus())) {
                                    return Observable.error(new Exception(response.getStatus().reasonPhrase()));
                                }
                                return response.getContent()
                                    .map(new Func1<ServerSentEvent, JobSchedulingInfo>() {
                                        @Override
                                        public JobSchedulingInfo call(ServerSentEvent event) {
                                            try {
                                                return Jackson.fromJSON(event.contentAsString(), JobSchedulingInfo.class);
                                            } catch (IOException e) {
                                                throw new RuntimeException("Invalid schedInfo json: " + e.getMessage(), e);
                                            }
                                        }
                                    })
                                    .timeout(3 * 60, TimeUnit.SECONDS)
                                    .filter(new Func1<JobSchedulingInfo, Boolean>() {
                                        @Override
                                        public Boolean call(JobSchedulingInfo schedulingInfo) {
                                            return schedulingInfo != null && !JobSchedulingInfo.HB_JobId.equals(schedulingInfo.getJobId());
                                        }
                                    })
                                    ;
                            }
                        })
                        ;
                }
            })
            .repeatWhen(repeatFn)
            .retryWhen(retryFn)
            ;
    }

    public Observable<JobSchedulingInfo> schedulingChanges(final String jobId) {
        return schedulingChanges(jobId, retryLogic, repeatLogic);
    }


    public Observable<NamedJobInfo> namedJobInfo(final String jobName, final Func1<Observable<? extends Throwable>, Observable<?>> retryFn,
                                                 final Func1<Observable<? extends Void>, Observable<?>> repeatFn) {
        return Observable.just(new MasterDescription("localhost", "127.0.0.1", serverPort, serverPort,
            serverPort, "/api/postjobstatus", serverPort, System.currentTimeMillis()))
            .filter(new Func1<MasterDescription, Boolean>() {
                @Override
                public Boolean call(MasterDescription masterDescription) {
                    return masterDescription != null;
                }
            })
            .retryWhen(retryFn)
            .switchMap(new Func1<MasterDescription, Observable<NamedJobInfo>>() {
                @Override
                public Observable<NamedJobInfo> call(MasterDescription masterDescription) {
                    return getRxnettySseClient(masterDescription.getHostname(), masterDescription.getSchedInfoPort())
                        .submit(HttpClientRequest.createGet("/namedjobs/" + jobName + "?sendHB=true"))
                        .flatMap(new Func1<HttpClientResponse<ServerSentEvent>, Observable<NamedJobInfo>>() {
                            @Override
                            public Observable<NamedJobInfo> call(HttpClientResponse<ServerSentEvent> response) {
                                if(!HttpResponseStatus.OK.equals(response.getStatus()))
                                    return Observable.error(new Exception(response.getStatus().reasonPhrase()));
                                return response.getContent()
                                    .map(new Func1<ServerSentEvent, NamedJobInfo>() {
                                        @Override
                                        public NamedJobInfo call(ServerSentEvent event) {
                                            try {
                                                return Jackson.fromJSON(event.contentAsString(), NamedJobInfo.class);
                                            } catch (IOException e) {
                                                throw new RuntimeException("Invalid namedJobInfo json: " + e.getMessage(), e);
                                            }
                                        }
                                    })
                                    .timeout(3 * 60, TimeUnit.SECONDS)
                                    .filter(new Func1<NamedJobInfo, Boolean>() {
                                        @Override
                                        public Boolean call(NamedJobInfo namedJobInfo) {
                                            return namedJobInfo != null && !JobSchedulingInfo.HB_JobId.equals(namedJobInfo.getName());
                                        }
                                    })
                                    ;
                            }})
                        ;
                }
            })
            .repeatWhen(repeatFn)
            .retryWhen(retryFn)
            ;
    }

    public Observable<NamedJobInfo> namedJobInfo(final String jobName) {
        return namedJobInfo(jobName, retryLogic, repeatLogic);
    }
}
