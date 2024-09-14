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

import static akka.pattern.PatternsCS.ask;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_CONFLICT;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_NOT_FOUND;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS_CREATED;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.JobClustersManagerInitialize;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.JobClustersManagerInitializeResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListCompletedJobsInClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListCompletedJobsInClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobClustersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobClustersResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ReconcileJobCluster;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLAResponse;
import static io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.akka.MantisActorSupervisorStrategy;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.JobClusterActor;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobHelper;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

/*
Supervisor Actor responsible for creating/deletion/listing of all Job Clusters in the system
 */
public class JobClustersManagerActor extends AbstractActorWithTimers implements IJobClustersManager {
    private static final String CHECK_CLUSTERS_TIMER_KEY = "CHECK_CLUSTER_TIMER";
    public static final int STATE_TRANSITION_TIMEOUT_MSECS = 5000;
    private final Logger logger = LoggerFactory.getLogger(JobClustersManagerActor.class);
    private final long checkAgainInSecs = 30;

    private final Counter numJobClusterInitFailures;
    private final Counter numJobClusterInitSuccesses;
    private Receive initializedBehavior;
    public static Props props(final MantisJobStore jobStore, final LifecycleEventPublisher eventPublisher, final CostsCalculator costsCalculator, int slaHeadroomForAcceptedJobs) {
        return Props.create(JobClustersManagerActor.class, jobStore, eventPublisher, costsCalculator, slaHeadroomForAcceptedJobs)
            .withMailbox("akka.actor.metered-mailbox");
    }

    private final MantisJobStore jobStore;
    private final LifecycleEventPublisher eventPublisher;
    private final CostsCalculator costsCalculator;
    private MantisSchedulerFactory mantisSchedulerFactory = null;

    JobClusterInfoManager jobClusterInfoManager;

    private final int slaHeadroomForAcceptedJobs;

    private ActorRef jobListHelperActor;
    public JobClustersManagerActor(final MantisJobStore store, final LifecycleEventPublisher eventPublisher, final CostsCalculator costsCalculator, int slaHeadroomForAcceptedJobs) {
        this.jobStore = store;
        this.eventPublisher = eventPublisher;
        this.costsCalculator = costsCalculator;
        this.slaHeadroomForAcceptedJobs = slaHeadroomForAcceptedJobs;

        MetricGroupId metricGroupId = getMetricGroupId();
        Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addCounter("numJobClusterInitFailures")
                .addCounter("numJobClusterInitSuccesses")

                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        this.numJobClusterInitFailures = m.getCounter("numJobClusterInitFailures");
        this.numJobClusterInitSuccesses = m.getCounter("numJobClusterInitSuccesses");

        initializedBehavior = getInitializedBehavior();
    }

    MetricGroupId getMetricGroupId() {
        return new MetricGroupId("JobClustersManagerActor");
    }


    /**
     *  JobClusterManager Actor behaviors 27 total
     *  - Init
     *  // CLUSTER RELATED
     *  - CreateJC
     *  - InitalizeJCResponse
     *  - DeleteJC
     *  - DeleteJCResponse
     *  - UpdateJC
     *  - UpdateLabel
     *  - UpdateSLA
     *  - UpdateArtifact
     *  - UpdateMigrationStrat
     *  - ENABLE JC
     *  - DISABLE JC
     *  - GET CLUSTER
     *  - LIST completed jobs
     *  - GET LAST SUBMITTED JOB
     *  - LIST archived workers
     *
     *  - LIST JCs
     *  - LIST JOBS
     *  - LIST JOB IDS
     *  - LIST WORKERS -> (pass thru to each Job Actor)
     *      *
     *  // pass thru to JOB
     *  - SUBMIT JOB -> (INIT JOB on Job Actor)
     *  - KILL JOB -> (pass thru Job Actor)
     *  - GET JOB -> (pass thru Job Actor)
     *  - GET JOB SCHED INFO -> (pass thru Job Actor)
     *  - SCALE JOB -> (pass thru Job Actor)
     *  - RESUBMIT WORKER -> (pass thru Job Actor)
     *
     *  - WORKER EVENT -> (pass thru Job Actor)
     * @return
     */

    private Receive getInitializedBehavior() {
        String state = "initialized";
        return receiveBuilder()
                .match(ReconcileJobCluster.class, this::onReconcileJobClusters)
                // Specific Job Cluster related messages
                .match(CreateJobClusterRequest.class, this::onJobClusterCreate)
                .match(JobClusterProto.InitializeJobClusterResponse.class, this::onJobClusterInitializeResponse)
                .match(DeleteJobClusterRequest.class, this::onJobClusterDelete)
                .match(JobClusterProto.DeleteJobClusterResponse.class, this::onJobClusterDeleteResponse)
                .match(UpdateJobClusterRequest.class, this::onJobClusterUpdate)
                .match(UpdateJobClusterSLARequest.class, this::onJobClusterUpdateSLA)
                .match(UpdateJobClusterArtifactRequest.class, this::onJobClusterUpdateArtifact)
                .match(UpdateSchedulingInfo.class, this::onJobClusterUpdateSchedulingInfo)
                .match(UpdateJobClusterLabelsRequest.class, this::onJobClusterUpdateLabels)
                .match(UpdateJobClusterWorkerMigrationStrategyRequest.class, this::onJobClusterUpdateWorkerMigrationConfig)
                .match(EnableJobClusterRequest.class, this::onJobClusterEnable)
                .match(DisableJobClusterRequest.class, this::onJobClusterDisable)
                .match(GetJobClusterRequest.class, this::onJobClusterGet)
                .match(ListCompletedJobsInClusterRequest.class, this::onJobListCompleted)
                .match(GetLastSubmittedJobIdStreamRequest.class, this::onGetLastSubmittedJobIdSubject)
                .match(ListArchivedWorkersRequest.class, this::onListArchivedWorkers)
                // List Job Cluster related messages
                .match(ListJobClustersRequest.class, this::onJobClustersList)
                // List Jobs related messages
                .match(ListJobsRequest.class, this::onJobList)
                .match(ListJobIdsRequest.class, this::onJobIdList)
                .match(ListWorkersRequest.class, this::onListActiveWorkers)

                //delegate to job
                .match(SubmitJobRequest.class, this::onJobSubmit)
                .match(KillJobRequest.class, this::onJobKillRequest)
              //  .match(JobClusterProto.KillJobResponse.class, this::onJobKillResponse)
                .match(GetJobDetailsRequest.class, this::onGetJobDetailsRequest)
                .match(GetJobSchedInfoRequest.class, this::onGetJobStatusSubject)
                .match(GetLatestJobDiscoveryInfoRequest.class, this::onGetLatestJobDiscoveryInfo)
                .match(ScaleStageRequest.class, this::onScaleStage)
                .match(ResubmitWorkerRequest.class, this::onResubmitWorker)

                //delegate to worker
                .match(WorkerEvent.class, this::onWorkerEvent)
                .match(Terminated.class, this::onTerminated)

                // Unexpected
                .match(JobClustersManagerInitialize.class, (x) -> getSender().tell(new JobClustersManagerInitializeResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state) ), getSelf()))

                .matchAny(x -> logger.warn("unexpected message {} received by Job Cluster Manager actor. In initialized state ", x))
                .build();
    }

    private String genUnexpectedMsg(String event, String state) {
        return String.format("Unexpected message %s received by JobClustersManager actor in %s State", event, state);
    }

    private Receive getInitializingBehavior() {
        String state = "initializing";
        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN
                .match(JobClustersManagerInitialize.class, this::initialize)
                // EXPECTED MESSAGES END

                // UNEXPECTED MESSAGES BEGIN
                .match(ReconcileJobCluster.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                .match(CreateJobClusterRequest.class, (x) -> getSender().tell(new CreateJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), x.getJobClusterDefinition().getName()), getSelf()))
                .match(JobClusterProto.InitializeJobClusterResponse.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                .match(DeleteJobClusterRequest.class, (x) -> getSender().tell(new DeleteJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(JobClusterProto.DeleteJobClusterResponse.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                .match(UpdateJobClusterRequest.class, (x) -> getSender().tell(new UpdateJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(UpdateJobClusterSLARequest.class, (x) -> getSender().tell(new UpdateJobClusterSLAResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(UpdateJobClusterArtifactRequest.class, (x) -> getSender().tell(new UpdateJobClusterArtifactResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(UpdateSchedulingInfo.class, (x) -> getSender().tell(new UpdateSchedulingInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(UpdateJobClusterLabelsRequest.class, (x) -> getSender().tell(new UpdateJobClusterLabelsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(UpdateJobClusterWorkerMigrationStrategyRequest.class, (x) -> getSender().tell(new UpdateJobClusterWorkerMigrationStrategyResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(EnableJobClusterRequest.class, (x) -> getSender().tell(new EnableJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(DisableJobClusterRequest.class, (x) -> getSender().tell(new DisableJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(GetJobClusterRequest.class, (x) -> getSender().tell(new GetJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(ListCompletedJobsInClusterRequest.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                .match(GetLastSubmittedJobIdStreamRequest.class, (x) -> getSender().tell(new GetLastSubmittedJobIdStreamResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(ListArchivedWorkersRequest.class, (x) -> getSender().tell(new ListArchivedWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), Lists.newArrayList()), getSelf()))
                .match(ListJobClustersRequest.class, (x) -> getSender().tell(new ListJobClustersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), Lists.newArrayList()), getSelf()))
                .match(ListJobsRequest.class, (x) -> getSender().tell(new ListJobsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), Lists.newArrayList()), getSelf()))
                .match(ListJobIdsRequest.class, (x) -> getSender().tell(new ListJobIdsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), Lists.newArrayList()), getSelf()))
                .match(ListWorkersRequest.class, (x) -> getSender().tell(new ListWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), Lists.newArrayList()), getSelf()))
                .match(SubmitJobRequest.class, (x) -> getSender().tell(new SubmitJobResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(KillJobRequest.class, (x) -> getSender().tell(new KillJobResponse(x.requestId, CLIENT_ERROR, JobState.Noop, genUnexpectedMsg(x.toString(), state), x.getJobId(), x.getUser()), getSelf()))
                .match(JobClusterProto.KillJobResponse.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                .match(GetJobDetailsRequest.class, (x) -> getSender().tell(new GetJobDetailsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), empty()), getSelf()))
                .match(ScaleStageRequest.class, (x) -> getSender().tell(new ScaleStageResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state), 0), getSelf()))
                .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), state)), getSelf()))
                .match(WorkerEvent.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), state)))
                // everything else
                .matchAny(x -> logger.warn("unexpected message {} received by Job Cluster Manager actor. It needs to be initialized first ", x))
                // UNEXPECTED MESSAGES BEGIN
                .build();
    }


    private void initialize(JobClustersManagerInitialize initMsg) {
        ActorRef sender = getSender();
        try {
            logger.info("In JobClustersManagerActor:initialize");

            this.jobListHelperActor = getContext().actorOf(JobListHelperActor.props(), "JobListHelperActor");
            getContext().watch(jobListHelperActor);

            mantisSchedulerFactory = initMsg.getScheduler();
            Map<String, IJobClusterMetadata> jobClusterMap = new HashMap<>();

            this.jobClusterInfoManager = new JobClusterInfoManager(jobStore, mantisSchedulerFactory, eventPublisher, costsCalculator);

            if (!initMsg.isLoadJobsFromStore()) {
                getContext().become(initializedBehavior);
                sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SUCCESS, "JobClustersManager successfully inited"), getSelf());

            } else {
                List<IJobClusterMetadata> jobClusters = jobStore.loadAllJobClusters();
                logger.info("Read {} job clusters from storage", jobClusters.size());

                List<IMantisJobMetadata> activeJobs = jobStore.loadAllActiveJobs();
                logger.info("Read {} jobs from storage", activeJobs.size());

                for (IJobClusterMetadata jobClusterMeta : jobClusters) {
                    String clusterName = jobClusterMeta.getJobClusterDefinition().getName();
                    jobClusterMap.put(clusterName, jobClusterMeta);
                }

                Map<String, List<IMantisJobMetadata>> clusterToJobMap = new HashMap<>();

                // group jobs by cluster
                for (IMantisJobMetadata jobMeta : activeJobs) {
                    String clusterName = jobMeta.getClusterName();
                    clusterToJobMap.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(jobMeta);
                }

                long masterInitTimeoutSecs = ConfigurationProvider.getConfig().getMasterInitTimeoutSecs();
                long timeout = ((masterInitTimeoutSecs - 60)) > 0 ? (masterInitTimeoutSecs - 60) : masterInitTimeoutSecs;
                Observable.from(jobClusterMap.values())
                        .filter((jobClusterMeta) -> jobClusterMeta != null && jobClusterMeta.getJobClusterDefinition() != null)
                        .flatMap((jobClusterMeta) -> {
                            Duration t = Duration.ofSeconds(timeout);
                            Optional<JobClusterInfo> jobClusterInfoO = jobClusterInfoManager.createClusterActorAndRegister(jobClusterMeta.getJobClusterDefinition());
                            if (!jobClusterInfoO.isPresent()) {
                                logger.info("skipping job cluster {} on bootstrap as actor creating failed", jobClusterMeta.getJobClusterDefinition().getName());
                                return Observable.empty();
                            }
                            JobClusterInfo jobClusterInfo = jobClusterInfoO.get();
                            List<IMantisJobMetadata> jobList = Lists.newArrayList();
                            List<IMantisJobMetadata> jList = clusterToJobMap.get(jobClusterMeta.getJobClusterDefinition().getName());
                            if (jList != null) {
                                jobList.addAll(jList);
                            }

                            List<CompletedJob> completedJobsList = Lists.newArrayList();
                            JobClusterProto.InitializeJobClusterRequest req = new JobClusterProto.InitializeJobClusterRequest((JobClusterDefinitionImpl) jobClusterMeta.getJobClusterDefinition(),
                                jobClusterMeta.isDisabled(), jobClusterMeta.getLastJobCount(), jobList,
                                "system", getSelf(), false);
                            return jobClusterInfoManager.initializeCluster(jobClusterInfo, req, t);


                        })
                        .filter(Objects::nonNull)
                        .toBlocking()
                        .subscribe((clusterInit) -> {
                            logger.info("JobCluster {} inited with code {}", clusterInit.jobClusterName, clusterInit.responseCode);
                            numJobClusterInitSuccesses.increment();
                        }, (error) -> {
                            logger.warn("Exception initializing clusters {}", error.getMessage(), error);

                            logger.error("JobClusterManagerActor had errors during initialization NOT transitioning to initialized behavior");
                          //  getContext().become(initializedBehavior);
                            sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SERVER_ERROR, "JobClustersManager  inited with errors"), getSelf());

                        }, () -> {
                            logger.info("JobClusterManagerActor transitioning to initialized behavior");
                            getContext().become(initializedBehavior);
                            sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SUCCESS, "JobClustersManager successfully inited"), getSelf());
                        });

                getTimers().startPeriodicTimer(CHECK_CLUSTERS_TIMER_KEY, new ReconcileJobCluster(), Duration.ofSeconds(checkAgainInSecs));
                // kick off loading of archived jobs
                logger.info("Kicking off archived job load asynchronously");
                jobStore.loadAllArchivedJobsAsync();
            }

        } catch(Exception e) {
            logger.error("caught exception", e);
            sender.tell(new JobClustersManagerInitializeResponse(initMsg.requestId, SERVER_ERROR, e.getMessage()), getSelf());
        }

        logger.info("JobClustersManagerActor:initialize ends");
    }


    @Override
    public void onReconcileJobClusters(ReconcileJobCluster p) {
        Set<JobClusterInfo> jobClusterInfos = this.jobClusterInfoManager.getAllJobClusterInfo().values().stream()
                .filter((jci) -> ((jci.currentState == JobClusterInfo.JobClusterState.INITIALIZING || jci.currentState == JobClusterInfo.JobClusterState.DELETING)
                        && (p.timeOfEnforcement.toEpochMilli() - jci.stateUpdateTime) > STATE_TRANSITION_TIMEOUT_MSECS))
                .collect(Collectors.toSet());
        if(jobClusterInfos.size() > 0) {
            logger.warn("{} JobClusters stuck in initializing/deleting state ", jobClusterInfos.size());
            jobClusterInfos.stream().forEach((jci) -> {
                if(jci.currentState.equals(JobClusterInfo.JobClusterState.INITIALIZING)) {
                    // retry init request
                    logger.warn("Retrying init on JobCluster {} stuck in {} state since {}", jci.clusterName, jci.currentState, jci.stateUpdateTime);
                    jci.stateUpdateTime = p.timeOfEnforcement.toEpochMilli();
                    jci.jobClusterActor.tell(jci.initRequest, getSelf());

                } else { // in pending delete state
                    logger.warn("Deregistering JobCluster {} stuck in {} state since {}", jci.clusterName, jci.currentState, jci.stateUpdateTime);

                    jobClusterInfoManager.deregisterJobCluster(jci.clusterName);
                }

            });

        }

    }


    @Override
    public void onJobClusterCreate(final CreateJobClusterRequest request) {
        final String name = request.getJobClusterDefinition().getName();

        if (!jobClusterInfoManager.isClusterExists(name)) {
            try {
                Optional<JobClusterInfo> jobClusterInfoO = jobClusterInfoManager.createClusterActorAndRegister(request.getJobClusterDefinition());
                if (jobClusterInfoO.isPresent()) {
                    jobClusterInfoManager.initializeClusterAsync(jobClusterInfoO.get(), new JobClusterProto.InitializeJobClusterRequest(request.getJobClusterDefinition(), request.getUser(), getSender()));
                } else {
                    getSender().tell(new CreateJobClusterResponse(
                        request.requestId, CLIENT_ERROR,
                        "Job Cluster " + request.getJobClusterDefinition().getName() + " could not be created due to invalid name",
                        request.getJobClusterDefinition().getName()), getSelf());
                }
            } catch (Exception e) {
                getSender().tell(new CreateJobClusterResponse(
                        request.requestId, SERVER_ERROR,
                        "Job Cluster " + request.getJobClusterDefinition().getName() + " could not be created due to " + e.getMessage(),
                        request.getJobClusterDefinition().getName()), getSelf());
            }
        } else {
            getSender().tell(new CreateJobClusterResponse(
                    request.requestId, CLIENT_ERROR_CONFLICT,
                    "Job Cluster " + request.getJobClusterDefinition().getName() + " already exists",
                    request.getJobClusterDefinition().getName()), getSelf());
        }

    }
    @Override
    public void onJobClusterInitializeResponse(final JobClusterProto.InitializeJobClusterResponse createResp) {
        logger.info("Got JobClusterInitializeResponse {}", createResp);

        jobClusterInfoManager.processInitializeResponse(createResp);


    }
    @Override
    public void onJobClusterDelete(final DeleteJobClusterRequest request) {
        jobClusterInfoManager.processDeleteRequest(request);
    }
    @Override
    public void onJobClusterDeleteResponse(final JobClusterProto.DeleteJobClusterResponse resp) {
        jobClusterInfoManager.processDeleteResponse(resp);

    }
    @Override
    public void onJobClusterUpdate(final UpdateJobClusterRequest request) {

        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobClusterDefinition().getName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new UpdateJobClusterResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getJobClusterDefinition().getName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClustersList(final ListJobClustersRequest request) {
        if(logger.isDebugEnabled()) { logger.info("In onJobClustersListRequest {}", request); }
        ActorRef sender = getSender();
        Map<String, JobClusterInfo> jobClusterInfoMap = jobClusterInfoManager.getAllJobClusterInfo();
        jobListHelperActor.tell(new JobListHelperActor.ListJobClusterRequestWrapper(request,sender,jobClusterInfoMap),getSelf());

    }
    @Override
    public void onJobClusterGet(GetJobClusterRequest r) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(r.getJobClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(r, getContext());
        } else {
            sender.tell(new GetJobClusterResponse(r.requestId, CLIENT_ERROR_NOT_FOUND, "No such Job cluster " + r.getJobClusterName(), empty()), getSelf());
        }
    }
    @Override
    public void onGetLastSubmittedJobIdSubject(GetLastSubmittedJobIdStreamRequest r) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(r.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(r, getContext());
        } else {
            sender.tell(new GetLastSubmittedJobIdStreamResponse(r.requestId, CLIENT_ERROR_NOT_FOUND, "No such Job cluster " + r.getClusterName(), empty()), getSelf());
        }
    }
    @Override
    public void onWorkerEvent(WorkerEvent workerEvent) {
        if(logger.isDebugEnabled()) { logger.debug("Entering JobClusterManagerActor:onWorkerEvent {}", workerEvent); }
        String clusterName = workerEvent.getWorkerId().getJobCluster();
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(clusterName);
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(workerEvent, getContext());
        } else {
            if(!JobHelper.isTerminalWorkerEvent(workerEvent)) {
                logger.warn("Event from Worker {} for a cluster {} that no longer exists. Terminate worker", workerEvent, workerEvent.getWorkerId().getJobCluster());
                Optional<String> host = JobHelper.getWorkerHostFromWorkerEvent(workerEvent);
                Optional<JobDefinition> archivedJobDefinition =
                    jobClusterInfoManager.getArchivedJobDefinition(workerEvent.getWorkerId().getJobId());
                if (archivedJobDefinition.isPresent()) {
                    mantisSchedulerFactory
                        .forJob(archivedJobDefinition.get())
                        .unscheduleAndTerminateWorker(workerEvent.getWorkerId(), host);
                } else {
                    logger.error("Non-Terminal Event {} from worker {} for a cluster {} that no longer exists and the job definition not yet archived", workerEvent, workerEvent.getWorkerId(), workerEvent.getWorkerId().getJobCluster());
                }
            } else {
                logger.warn("Terminal Event from Worker {} for a cluster {} that no longer exists. Ignore worker", workerEvent, workerEvent.getWorkerId().getJobCluster());
            }

        }
    }



    private void onTerminated(final Terminated terminated) {
        logger.warn("onTerminated {}", terminated.actor());
    }

    //////////////////// JOB OPERATIONS ////////////////////////////////////////////////
    @Override
    public void onJobSubmit(final SubmitJobRequest request) {
        logger.info("Submitting job " + request);
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new SubmitJobResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "Job Cluster " + request.getClusterName() + " doesn't exist", empty()), getSelf());
        }
    }


    @Override
    public void onJobKillRequest(final KillJobRequest request) {
        logger.info("Killing job " + request);
        ActorRef sender = getSender();
        JobId jobIdToKill = request.getJobId();
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(jobIdToKill.getCluster());

        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.tell(
                    new JobClusterProto.KillJobRequest(request.getJobId(), request.getReason(),
                            JobCompletedReason.Killed, request.getUser(), sender), getSelf());
        } else {
            logger.info("Job cluster {} not found", jobIdToKill.getCluster());
            sender.tell(new KillJobResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, JobState.Noop, "Job cluster " + jobIdToKill.getCluster() + " doesn't exist", jobIdToKill, request.getUser()), getSelf());
        }
    }


    ////////////////////// JOB OPERATIONS END //////////////////////////////////////////////

    @Override
    public void preStart() throws Exception {
        logger.info("JobClusterManager Actor started");
        super.preStart();
    }

    @Override
    public void postStop() throws Exception {
        logger.info("JobClusterManager Actor stopped");
        super.postStop();
    }

    @Override
    public void preRestart(Throwable t, Optional<Object> m) throws Exception {
        logger.info("preRestart {} (exc: {})", m, t.getMessage());
        // do not kill all children, which is the default here
        // super.preRestart(t, m);
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        logger.info("postRestart (exc={})", reason.getMessage());
        super.postRestart(reason);
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        // custom supervisor strategy to resume the Actor on Exception instead of the default restart
        return MantisActorSupervisorStrategy.getInstance().create();

    }

    @Override
    public Receive createReceive() {
        return getInitializingBehavior();
    }

    private void logError(Throwable e) {
        logger.error("Exception occurred retrieving job cluster list {}", e.getMessage());
    }



    @Override
    public void onJobClusterUpdateSLA(UpdateJobClusterSLARequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new UpdateJobClusterSLAResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClusterUpdateArtifact(UpdateJobClusterArtifactRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new UpdateJobClusterArtifactResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClusterUpdateSchedulingInfo(UpdateSchedulingInfo request) {
        ActorRef sender = getSender();
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(
                new UpdateJobClusterArtifactResponse(
                    request.getRequestId(),
                    CLIENT_ERROR_NOT_FOUND,
                    "JobCluster " + request.getClusterName() + " doesn't exist"),
                getSelf());
        }
    }

    @Override
    public void onJobClusterUpdateLabels(UpdateJobClusterLabelsRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new UpdateJobClusterLabelsResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClusterUpdateWorkerMigrationConfig(UpdateJobClusterWorkerMigrationStrategyRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new UpdateJobClusterWorkerMigrationStrategyResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClusterEnable(EnableJobClusterRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new EnableJobClusterResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }
    }

    @Override
    public void onJobClusterDisable(DisableJobClusterRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new DisableJobClusterResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist"), getSelf());
        }

    }

    @Override
    public void onGetJobDetailsRequest(GetJobDetailsRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobId().getCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new GetJobDetailsResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "Job " + request.getJobId().getId() + " doesn't exist", empty()), getSelf());
        }

    }
    @Override
    public void onGetJobStatusSubject(GetJobSchedInfoRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobId().getCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new GetJobSchedInfoResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getJobId().getCluster() + " doesn't exist", Optional.empty()), getSelf());
        }
    }

    @Override
    public void onGetLatestJobDiscoveryInfo(GetLatestJobDiscoveryInfoRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new GetLatestJobDiscoveryInfoResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getJobCluster() + " doesn't exist", Optional.empty()), getSelf());
        }
    }

    @Override
    public void onJobListCompleted(ListCompletedJobsInClusterRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getClusterName());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            sender.tell(new ListCompletedJobsInClusterResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getClusterName() + " doesn't exist", Lists.newArrayList()), getSelf());
        }

    }
    @Override
    public void onJobIdList(ListJobIdsRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobIdList"); }
        ActorRef sender = getSender();
        this.jobListHelperActor.tell(new JobListHelperActor.ListJobIdRequestWrapper(request, sender, jobClusterInfoManager.getAllJobClusterInfo()), getSelf());
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobIdList"); }
    }
    @Override
    public void onListArchivedWorkers(ListArchivedWorkersRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobId().getCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            getSender().tell(new ListArchivedWorkersResponse(request.requestId, CLIENT_ERROR, "Job Cluster " + request.getJobId().getCluster() + " Not found", Lists.newArrayList()), getSelf());
        }
    }

    public void  onListActiveWorkers(ListWorkersRequest request) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(request.getJobId().getCluster());
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(request, getContext());
        } else {
            getSender().tell(new ListWorkersResponse(request.requestId, CLIENT_ERROR, "Job Cluster " + request.getJobId().getCluster() + " Not found", Lists.newArrayList()), getSelf());
        }
    }


    @Override
    public void onJobList(ListJobsRequest request) {
        ActorRef sender = getSender();
        this.jobListHelperActor.tell(new JobListHelperActor.ListJobRequestWrapper(request, sender, jobClusterInfoManager.getAllJobClusterInfo()),getSelf());
    }

    @Override
    public void onScaleStage(ScaleStageRequest scaleStage) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(scaleStage.getJobId().getCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(scaleStage, getContext());
        } else {
            sender.tell(new ScaleStageResponse(scaleStage.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + scaleStage.getJobId().getCluster() + " doesn't exist",0), getSelf());
        }

    }

    @Override
    public void onResubmitWorker(ResubmitWorkerRequest r) {
        Optional<JobClusterInfo> jobClusterInfo = jobClusterInfoManager.getJobClusterInfo(r.getJobId().getCluster());
        ActorRef sender = getSender();
        if(jobClusterInfo.isPresent()) {
            jobClusterInfo.get().jobClusterActor.forward(r, getContext());
        } else {
            sender.tell(new ResubmitWorkerResponse(r.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + r.getJobId().getCluster() + " doesn't exist"), getSelf());
        }

    }

     class JobClusterInfoManager {

        private final Map<String, JobClusterInfo> jobClusterNameToInfoMap = new HashMap<>();

        private final LifecycleEventPublisher eventPublisher;
        private MantisSchedulerFactory mantisSchedulerFactory;
        private final MantisJobStore jobStore;
        private final Metrics metrics;
        private final CostsCalculator costsCalculator;

        JobClusterInfoManager(MantisJobStore jobStore, MantisSchedulerFactory mantisSchedulerFactory, LifecycleEventPublisher eventPublisher, CostsCalculator costsCalculator) {
            this.eventPublisher = eventPublisher;
            this.mantisSchedulerFactory  = mantisSchedulerFactory;
            this.jobStore = jobStore;
            this.costsCalculator = costsCalculator;


            MetricGroupId metricGroupId = new MetricGroupId("JobClusterInfoManager");
            Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addGauge(new GaugeCallback(metricGroupId, "jobClustersGauge", () -> 1.0 * jobClusterNameToInfoMap.size()))
                .build();
            this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        }

         /**
          * Creates the job cluster Actor
          * Watches it
          * Adds it to internal map and publishes Lifecycle event
          * Could throw an unchecked exception if actor creation fails
          * @param jobClusterDefn
          * @return jobClusterInfo if actor creation and registration succeeds, else empty
          */
        Optional<JobClusterInfo> createClusterActorAndRegister(IJobClusterDefinition jobClusterDefn) {
            String clusterName = jobClusterDefn.getName();
            if(!isClusterExists(clusterName)) {
                if (!ActorPaths.isValidPathElement(clusterName)) {
                    logger.error("Cannot create actor for cluster with invalid name {}", clusterName);
                    return empty();
                }

                ActorRef jobClusterActor =
                    getContext().actorOf(
                        JobClusterActor.props(clusterName, this.jobStore, this.mantisSchedulerFactory, this.eventPublisher, this.costsCalculator, slaHeadroomForAcceptedJobs),
                        "JobClusterActor-" + clusterName);
                getContext().watch(jobClusterActor);

                JobClusterInfo jobClusterInfo = new JobClusterInfo(clusterName, jobClusterDefn, jobClusterActor);
                jobClusterNameToInfoMap.put(clusterName, jobClusterInfo);
                return ofNullable(jobClusterInfo);
            } else {
                return ofNullable(jobClusterNameToInfoMap.get(clusterName));
            }
        }

        void deregisterJobCluster(String jobClusterName) {

            Optional<JobClusterInfo> jobClusterInfo = getJobClusterInfo(jobClusterName);
            if(jobClusterInfo.isPresent()) {
                jobClusterInfo.get().markDeleted(System.currentTimeMillis());
                // unwatch and stop actor
                ActorRef jobClusterActor = jobClusterInfo.get().jobClusterActor;
                getContext().unwatch(jobClusterActor);
                getContext().stop(jobClusterActor);
                jobClusterNameToInfoMap.remove(jobClusterName);
            } else {
                logger.warn("Job Cluster does not exist {}", jobClusterInfo);
            }

        }

        Observable<JobClusterProto.InitializeJobClusterResponse> initializeCluster(JobClusterInfo jobClusterInfo, JobClusterProto.InitializeJobClusterRequest req, Duration t) {
             jobClusterInfo.markInitializing(req, System.currentTimeMillis());
             CompletionStage<JobClusterProto.InitializeJobClusterResponse> respCS =  ask(jobClusterInfo.jobClusterActor, req, t)
                     .thenApply(JobClusterProto.InitializeJobClusterResponse.class::cast);
            return Observable.from(respCS.toCompletableFuture(),Schedulers.io())
                    .map((resp)-> {
                        logger.info("JobCluster {} inited with code {}", resp.jobClusterName, resp.responseCode);
                        Optional<JobClusterInfo> jClusterInfo = jobClusterInfoManager.getJobClusterInfo(resp.jobClusterName);
                        if(resp.responseCode == SUCCESS) {
                            jClusterInfo.ifPresent((jci) -> jci.markInitialized(System.currentTimeMillis()));
                        }

                        return resp;
                    })
                    .onErrorResumeNext(ex -> {

                        logger.warn("caught exception {}", ex.getMessage(), ex);
                        numJobClusterInitFailures.increment();

                        // initialization fails deregister cluster
                        deregisterJobCluster(jobClusterInfo.clusterName);
                        return Observable.just(new JobClusterProto.InitializeJobClusterResponse(req.requestId, BaseResponse.ResponseCode.SERVER_ERROR,ex.getMessage(), jobClusterInfo.clusterName, ActorRef.noSender()));
                    });
        }

        void initializeClusterAsync(JobClusterInfo jobClusterInfo, JobClusterProto.InitializeJobClusterRequest req) {
             jobClusterInfo.markInitializing(req,System.currentTimeMillis());
             jobClusterInfo.jobClusterActor.tell(req, getSelf());
        }


        Optional<JobClusterInfo> getJobClusterInfo(String jobClusterName) {
            return ofNullable(jobClusterNameToInfoMap.get(jobClusterName));
        }

        Optional<JobDefinition> getArchivedJobDefinition(String jobId) {
            return jobStore.getArchivedJob(jobId).map(IMantisJobMetadata::getJobDefinition);
        }

        Map<String, JobClusterInfo> getAllJobClusterInfo() {
            return Collections.unmodifiableMap(jobClusterNameToInfoMap);
        }

        boolean isClusterExists(String clusterName) {

            return jobClusterNameToInfoMap.containsKey(clusterName);
        }


        void processInitializeResponse(JobClusterProto.InitializeJobClusterResponse createResp) {
             Optional<JobClusterInfo> jClusterInfo = getJobClusterInfo(createResp.jobClusterName);
             if(jClusterInfo.isPresent()) {
                 JobClusterInfo jobClusterInfo = jClusterInfo.get();
                 if(createResp.responseCode == SUCCESS) {
                      jobClusterInfo.markInitialized(System.currentTimeMillis());
                     createResp.requestor.tell(
                             new CreateJobClusterResponse(createResp.requestId,
                                                          SUCCESS_CREATED,
                                                          createResp.jobClusterName + " created",
                                                          createResp.jobClusterName),
                             getSelf());
                 } else if( createResp.responseCode == SERVER_ERROR){
                     deregisterJobCluster(createResp.jobClusterName);
                     createResp.requestor.tell(new CreateJobClusterResponse(createResp.requestId, createResp.responseCode, createResp.message, createResp.jobClusterName), getSelf());
                 }

             } else {
                 logger.warn("Received JobClusterInitializeResponse {} for unknown Job Cluster {}", createResp, createResp.jobClusterName);
             }

         }

         void processDeleteRequest(DeleteJobClusterRequest request) {
             Optional<JobClusterInfo> jobClusterInfoOp= getJobClusterInfo(request.getName());
             ActorRef sender = getSender();
             if (jobClusterInfoOp.isPresent()) {
                 JobClusterInfo jobClusterInfo = jobClusterInfoOp.get();
                 jobClusterInfo.jobClusterActor.tell(
                         new JobClusterProto.DeleteJobClusterRequest(request.getUser(), request.getName(), sender),
                         getSelf());
                 jobClusterInfo.markDeleting(System.currentTimeMillis());

             } else {
                 sender.tell(
                         new DeleteJobClusterResponse(request.requestId, CLIENT_ERROR_NOT_FOUND, "JobCluster " + request.getName() + " doesn't exist"),
                         getSelf());
             }
         }

         void processDeleteResponse(JobClusterProto.DeleteJobClusterResponse resp) {
             Optional<JobClusterInfo> jobClusterInfoOp= getJobClusterInfo(resp.clusterName);
             if(jobClusterInfoOp.isPresent()) {
                 if(resp.responseCode == SUCCESS) {
                     deregisterJobCluster(resp.clusterName);

                 }

             } else {
                 // No Such job cluster ignore
                 logger.warn("Received delete job cluster response {} for unknown job cluster {}", resp, resp.clusterName);
             }

             // inform caller
             resp.requestingActor.tell(
                     new DeleteJobClusterResponse(resp.requestId, resp.responseCode, resp.message)
                     , getSelf());

         }
     }

     @Value
     public static class UpdateSchedulingInfo {
         long requestId;
         String clusterName;
         SchedulingInfo schedulingInfo;
         String version;
     }

    static class JobClusterInfo {

        private static final Logger logger = LoggerFactory.getLogger(JobClusterInfo.class);
        public enum JobClusterState  { UNINITIALIZED, INITIALIZING, INITIALIZED, DELETING, DELETED}
        private JobClusterProto.InitializeJobClusterRequest initRequest;

        final String clusterName;
        final ActorRef jobClusterActor;

        private volatile JobClusterState currentState;
        volatile long stateUpdateTime;

        final IJobClusterDefinition jobClusterDefinition;

        JobClusterInfo(final String clusterName, final IJobClusterDefinition clusterDefn, final ActorRef actor) {
            this.clusterName = clusterName;
            this.jobClusterActor = actor;
            this.jobClusterDefinition = clusterDefn;
            this.currentState = JobClusterState.UNINITIALIZED;
            this.stateUpdateTime = System.currentTimeMillis();
        }

        public String getClusterName() {
            return clusterName;
        }

        public IJobClusterDefinition getJobClusterDefinition() {
            return jobClusterDefinition;
        }


        void markInitializing(JobClusterProto.InitializeJobClusterRequest req, long time) {
            if(currentState == JobClusterState.UNINITIALIZED) {
                this.stateUpdateTime = time;
                currentState = JobClusterState.INITIALIZING;
                initRequest = req;
            } else {
                logger.warn("Invalid state transition from {} to {} for job cluster {}", currentState, JobClusterState.INITIALIZING, clusterName);
            }
        }

        void markInitialized(long time) {
            if(currentState == JobClusterState.INITIALIZING ) {
                this.stateUpdateTime = time;
                this.currentState = JobClusterState.INITIALIZED;
            } else {
                logger.warn("Invalid state transition from {} to {} for job cluster {}", currentState, JobClusterState.INITIALIZED, clusterName);
            }
        }

        void markDeleting(long time) {
            this.currentState = JobClusterState.DELETING;
            this.stateUpdateTime = time;
        }

        void markDeleted(long time) {
            this.currentState = JobClusterState.DELETED;
            this.stateUpdateTime = time;
        }


        @Override
        public String toString() {
            return "JobClusterInfo{" +
                    "clusterName='" + clusterName + '\'' +
                    ", jobClusterActor=" + jobClusterActor +
                    ", currentState=" + currentState +
                    ", stateUpdateTime=" + stateUpdateTime +
                    ", jobClusterDefinition=" + jobClusterDefinition +
                    '}';
        }
    }

}
