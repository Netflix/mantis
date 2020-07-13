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

package io.mantisrx.master;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.Timeout;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
import io.mantisrx.master.jobcluster.MantisJobClusterMetadataView;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.server.master.domain.JobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;
import scala.concurrent.duration.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static akka.pattern.PatternsCS.ask;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;

/**
 * Helper Actor used by JobClustersManager for listing jobs and clusters.
 * By offloading the scatter-gather to a separate Actor the JCM is free to move on to processing other messages.
 *
 * This Actor is stateless and can be part of a pool of actors if performance becomes a bottle neck
 */

public class JobListHelperActor extends AbstractActor {

    private final Logger logger = LoggerFactory.getLogger(JobListHelperActor.class);

    public static Props props() {
        return Props.create(JobListHelperActor.class);
    }


    public JobListHelperActor() {

    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ListJobClusterRequestWrapper.class, this::onJobClustersList)

                .match(ListJobRequestWrapper.class, this::onJobList)
                .match(ListJobIdRequestWrapper.class, this::onJobIdList)

                .matchAny(x -> logger.warn("Unexpected message {}", x))
                .build();
    }


    private void onJobList(ListJobRequestWrapper request) {
        ActorRef sender = getSender();
        Timeout t = new Timeout(Duration.create(500, TimeUnit.MILLISECONDS));
        List<MantisJobMetadataView> resultList = Lists.newArrayList();
        getJobClustersMatchingRegex(request.jobClusterInfoMap.values(), request.listJobsRequest.getCriteria())
                .flatMap((jobClusterInfo) -> {
                    CompletionStage<JobClusterManagerProto.ListJobsResponse> respCS = ask(jobClusterInfo.jobClusterActor, request.listJobsRequest, t)
                            .thenApply(JobClusterManagerProto.ListJobsResponse.class::cast);
                    return Observable.from(respCS.toCompletableFuture(), Schedulers.io())
                            .onErrorResumeNext(ex -> {
                                logger.warn("caught exception {}", ex.getMessage(), ex);
                                return Observable.empty();
                            });
                })
                .filter(Objects::nonNull)
                .flatMapIterable((listJobsResp) -> listJobsResp.getJobList())
                .toSortedList((o1, o2) -> Long.compare(o1.getJobMetadata().getSubmittedAt(),
                        o2.getJobMetadata().getSubmittedAt()))
                .subscribeOn(Schedulers.computation())
                .subscribe( resultList::addAll,
                        (e) -> {
                            request.sender.tell(new JobClusterManagerProto.ListJobsResponse(request.listJobsRequest.requestId, SERVER_ERROR, e.getMessage(), resultList), sender);
                        },() -> {
// todo limit is applied at cluster level as well if(request.listJobsRequest.getCriteria().getLimit().isPresent()) {
//                                int limit = request.listJobsRequest.getCriteria().getLimit().get();
//                                request.sender.tell(new JobClusterManagerProto.ListJobsResponse(request.listJobsRequest.requestId, SUCCESS, "", resultList.subList(0, Math.min(resultList.size(), limit))), sender);
//                            }
                            request.sender.tell(new JobClusterManagerProto.ListJobsResponse(request.listJobsRequest.requestId, SUCCESS, "", resultList), sender);
                        })
        ;

    }

    private void onJobIdList(ListJobIdRequestWrapper request) {
        if(logger.isTraceEnabled()) { logger.trace("In onJobIdList {}", request); }
        ActorRef sender = getSender();
        Timeout t = new Timeout(Duration.create(500, TimeUnit.MILLISECONDS));
        List<JobClusterProtoAdapter.JobIdInfo> resultList = Lists.newArrayList();
        getJobClustersMatchingRegex(request.jobClusterInfoMap.values(),request.listJobIdsRequest.getCriteria())
                .flatMap((jobClusterInfo) -> {
                    CompletionStage<JobClusterManagerProto.ListJobIdsResponse> respCS = ask(jobClusterInfo.jobClusterActor, request.listJobIdsRequest, t)
                            .thenApply(JobClusterManagerProto.ListJobIdsResponse.class::cast);
                    return Observable.from(respCS.toCompletableFuture(), Schedulers.io())
                            .onErrorResumeNext(ex -> {
                                logger.warn("caught exception {}", ex.getMessage(), ex);
                                return Observable.empty();
                            });
                })
                .filter(Objects::nonNull)
                .map(JobClusterManagerProto.ListJobIdsResponse::getJobIds)
                .subscribeOn(Schedulers.computation())
                .subscribe(
                    resultList::addAll
                ,(error) -> {
                        logger.warn("Exception in JobListHelperActor:onJobIdList", error);
                    request.sender.tell(new JobClusterManagerProto.ListJobIdsResponse(request.listJobIdsRequest.requestId, SERVER_ERROR, error.getMessage(), resultList), sender);
                },() -> {
                    if(logger.isTraceEnabled()) { logger.trace("Exit onJobIdList {}", resultList); }
//                    if(request.listJobIdsRequest.getCriteria().getLimit().isPresent()) {
//                        int limit = request.listJobIdsRequest.getCriteria().getLimit().get();
//                        request.sender.tell(new JobClusterManagerProto.ListJobIdsResponse(request.listJobIdsRequest.requestId, SUCCESS, "", resultList.subList(0, Math.min(resultList.size(), limit))), sender);
//                    }
                            if(logger.isTraceEnabled()) { logger.trace("Exit onJobIdList {}", resultList); }

                    request.sender.tell(new JobClusterManagerProto.ListJobIdsResponse(request.listJobIdsRequest.requestId, SUCCESS, "", resultList), sender);

                });



    }


    private void onJobClustersList(ListJobClusterRequestWrapper request) {

        if(logger.isTraceEnabled()) { logger.trace("In onJobClustersListRequest {}", request); }
        ActorRef callerActor = getSender();

        Timeout timeout = new Timeout(Duration.create(500, TimeUnit.MILLISECONDS));
        List<MantisJobClusterMetadataView> clusterList = Lists.newArrayList();

        Observable.from(request.jobClusterInfoMap.values())
                .flatMap((jInfo) -> {
                    CompletionStage<JobClusterManagerProto.GetJobClusterResponse> respCS = ask(jInfo.jobClusterActor, new JobClusterManagerProto.GetJobClusterRequest(jInfo.clusterName), timeout)
                            .thenApply(JobClusterManagerProto.GetJobClusterResponse.class::cast);
                    return Observable.from(respCS.toCompletableFuture(), Schedulers.io())
                            .onErrorResumeNext(ex -> {
                                logger.warn("caught exception {}", ex.getMessage(), ex);
                                return Observable.empty();
                            });
                })
                .filter((resp) -> resp !=null && resp.getJobCluster().isPresent())
                .map((resp) -> resp.getJobCluster().get())
                //.collect((Func0<ArrayList<MantisJobClusterMetadataView>>) ArrayList::new,ArrayList::add)
                .doOnError(this::logError)
                .subscribeOn(Schedulers.computation())
                //.toBlocking()
                .subscribe(
                    clusterList::add
                    ,(err) -> {
                        logger.warn("Exception in onJobClusterList ", err);
                        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClustersListRequest {}", err); }
                        request.sender.tell(new JobClusterManagerProto.ListJobClustersResponse(request.listJobClustersRequest.requestId, SERVER_ERROR, err.getMessage(), clusterList), callerActor);
                    },() -> {
                        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClustersListRequest {}", clusterList); }
                        request.sender.tell(new JobClusterManagerProto.ListJobClustersResponse(request.listJobClustersRequest.requestId, SUCCESS, "", clusterList), callerActor);
                    })
        ;
    }


    private void logError(Throwable e) {
        logger.error("Exception occurred retrieving job cluster list {}", e.getMessage());
    }

    private Observable<JobClustersManagerActor.JobClusterInfo> getJobClustersMatchingRegex(Collection<JobClustersManagerActor.JobClusterInfo> jobClusterList, JobClusterManagerProto.ListJobCriteria criteria) {

        return Observable.from(jobClusterList)
                .filter((jcInfo) -> {
                    if(criteria.getMatchingRegex().isPresent()) {
                        try {
                            return Pattern.compile(criteria.getMatchingRegex().get(), Pattern.CASE_INSENSITIVE)
                                    .matcher(jcInfo.clusterName).find();
                        } catch(Exception e) {
                            logger.warn("Invalid regex {}", e.getMessage());
                            return true;
                        }

                    } else {
                        return true;
                    }
                });
    }


    static class ListJobClusterRequestWrapper {
        private final JobClusterManagerProto.ListJobClustersRequest listJobClustersRequest;
        private final ActorRef sender;
        private final Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap;

        public ListJobClusterRequestWrapper(final JobClusterManagerProto.ListJobClustersRequest request, final ActorRef sender, final Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap) {
            this.jobClusterInfoMap = jobClusterInfoMap;
            this.sender = sender;
            this.listJobClustersRequest = request;
        }


        public JobClusterManagerProto.ListJobClustersRequest getListJobClustersRequest() {
            return listJobClustersRequest;
        }

        public ActorRef getSender() {
            return sender;
        }


        public Map<String, JobClustersManagerActor.JobClusterInfo> getJobClusterInfoMap() {
            return jobClusterInfoMap;
        }
    }

    static class ListJobRequestWrapper {
        private final JobClusterManagerProto.ListJobsRequest listJobsRequest;
        private final ActorRef sender;
        private final Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap;


        public ListJobRequestWrapper(JobClusterManagerProto.ListJobsRequest listJobsRequest, ActorRef sender, Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap) {
            this.listJobsRequest = listJobsRequest;
            this.sender = sender;
            this.jobClusterInfoMap = jobClusterInfoMap;
        }
    }

    static class ListJobIdRequestWrapper {
        private final JobClusterManagerProto.ListJobIdsRequest listJobIdsRequest;
        private final ActorRef sender;
        private final Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap;

        public ListJobIdRequestWrapper(JobClusterManagerProto.ListJobIdsRequest listJobIdsRequest, ActorRef sender, Map<String, JobClustersManagerActor.JobClusterInfo> jobClusterInfoMap) {
            this.listJobIdsRequest = listJobIdsRequest;
            this.sender = sender;
            this.jobClusterInfoMap = jobClusterInfoMap;
        }
    }


}
