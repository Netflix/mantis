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

package io.mantisrx.master.jobcluster;

import static akka.pattern.PatternsCS.ask;
import static io.mantisrx.common.SystemParameters.JOB_WORKER_HEARTBEAT_INTERVAL_SECS;
import static io.mantisrx.common.SystemParameters.JOB_WORKER_TIMEOUT_SECS;
import static io.mantisrx.master.StringConstants.MANTIS_MASTER_USER;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR_NOT_FOUND;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import com.mantisrx.common.utils.LabelUtils;
import com.netflix.fenzo.triggers.CronTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.fenzo.triggers.exceptions.TriggerNotFoundException;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.Label;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.JobClustersManagerActor.UpdateSchedulingInfo;
import io.mantisrx.master.akka.MantisActorSupervisorStrategy;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter.JobIdInfo;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.job.CostsCalculator;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobActor;
import io.mantisrx.master.jobcluster.job.JobHelper;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataImpl;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.CreateJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DeleteJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.DisableJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.EnableJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDefinitionUpdatedFromJobActorRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDefinitionUpdatedFromJobActorResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLastSubmittedJobIdStreamResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.JobClustersManagerInitializeResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListArchivedWorkersResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListCompletedJobsInClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListCompletedJobsInClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobCriteria;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobIdsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListJobsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.SubmitJobResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterArtifactResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterLabelsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLARequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterSLAResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateJobClusterWorkerMigrationStrategyResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.JobStartedEvent;
import io.mantisrx.master.jobcluster.proto.JobClusterProto.KillJobRequest;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.master.InvalidJobRequestException;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.IJobClusterDefinition.CronPolicy;
import io.mantisrx.server.master.domain.JobClusterConfig;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl.CompletedJob;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.SLA;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.JobClusterAlreadyExistsException;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.MantisSchedulerFactory;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;


/**
 * Actor responsible for handling all operations related to one Job Cluster.
 * @author njoshi
 *
 */
public class JobClusterActor extends AbstractActorWithTimers implements IJobClusterManager {

    private static final int BOOKKEEPING_INTERVAL_SECS = 5;

    private static final String BOOKKEEPING_TIMER_KEY = "JOB_CLUSTER_BOOKKEEPING";
    private static final Integer DEFAULT_LIMIT = 100;
    private static final Integer DEFAULT_ACTIVE_JOB_LIMIT = 5000;

    private final Logger logger = LoggerFactory.getLogger(JobClusterActor.class);

    private static final String CHECK_EXPIRED_TIMER_KEY = "EXPIRE_OLD_JOBS";

    private static final long EXPIRED_JOBS_CHECK_INTERVAL_SECS = 3600;


    private final Counter numJobSubmissions;
    private final Counter numJobShutdowns;
    private final Counter numJobActorCreationCounter;
    private final Counter numJobClustersInitialized;
    private final Counter numJobClusterInitializeFailures;
    private final Counter numJobsInitialized;
    private final Counter numJobSubmissionFailures;
    private final Counter numJobClusterEnable;
    private final Counter numJobClusterEnableErrors;
    private final Counter numJobClusterDisable;
    private final Counter numJobClusterDisableErrors;
    private final Counter numJobClusterDelete;
    private final Counter numJobClusterDeleteErrors;
    private final Counter numJobClusterUpdate;
    private final Counter numJobClusterUpdateErrors;
    private final Counter numSLAEnforcementExecutions;


    public static Props props(
        final String name,
        final MantisJobStore jobStore,
        final MantisSchedulerFactory mantisSchedulerFactory,
        final LifecycleEventPublisher eventPublisher,
        final CostsCalculator costsCalculator,
        final int slaHeadroomForAcceptedJobs) {
        return Props.create(JobClusterActor.class, name, jobStore, mantisSchedulerFactory, eventPublisher, costsCalculator, slaHeadroomForAcceptedJobs);
    }

    private final Receive initializedBehavior;

    private final Receive disabledBehavior;

    private final String name;
    private final MantisJobStore jobStore;
    private IJobClusterMetadata jobClusterMetadata;
    private CronManager cronManager;

    private SLAEnforcer slaEnforcer;
    private final JobManager jobManager;
    private final MantisSchedulerFactory mantisSchedulerFactory;
    private final LifecycleEventPublisher eventPublisher;

    private final BehaviorSubject<JobId> jobIdStartedSubject;
    private final JobDefinitionResolver jobDefinitionResolver = new JobDefinitionResolver();
    private final Metrics metrics;

    private final int slaHeadroomForAcceptedJobs;


    public JobClusterActor(
        final String name,
        final MantisJobStore jobStore,
        final MantisSchedulerFactory schedulerFactory,
        final LifecycleEventPublisher eventPublisher,
        final CostsCalculator costsCalculator,
        final int slaHeadroomForAcceptedJobs) {
        this.name = name;
        this.jobStore = jobStore;
        this.mantisSchedulerFactory = schedulerFactory;
        this.eventPublisher = eventPublisher;
        this.slaHeadroomForAcceptedJobs = slaHeadroomForAcceptedJobs;

        this.jobManager = new JobManager(name, getContext(), mantisSchedulerFactory, eventPublisher, jobStore, costsCalculator);

        jobIdStartedSubject = BehaviorSubject.create();

        initializedBehavior =  buildInitializedBehavior();
        disabledBehavior = buildDisabledBehavior();

        MetricGroupId metricGroupId = getMetricGroupId(name);
        Metrics m = new Metrics.Builder()
            .id(metricGroupId)
            .addCounter("numJobSubmissions")
            .addCounter("numJobSubmissionFailures")
            .addCounter("numJobShutdowns")
            .addCounter("numJobActorCreationCounter")
            .addCounter("numJobsInitialized")
            .addCounter("numJobClustersInitialized")
            .addCounter("numJobClusterInitializeFailures")
            .addCounter("numJobClusterEnable")
            .addCounter("numJobClusterEnableErrors")
            .addCounter("numJobClusterDisable")
            .addCounter("numJobClusterDisableErrors")
            .addCounter("numJobClusterDelete")
            .addCounter("numJobClusterDeleteErrors")
            .addCounter("numJobClusterUpdate")
            .addCounter("numJobClusterUpdateErrors")
            .addCounter("numSLAEnforcementExecutions")
            .addGauge(new GaugeCallback(metricGroupId, "acceptedJobsGauge", () -> 1.0 * this.jobManager.acceptedJobsCount()))
            .addGauge(new GaugeCallback(metricGroupId, "activeJobsGauge", () -> 1.0 * this.jobManager.activeJobsCount()))
            .addGauge(new GaugeCallback(metricGroupId, "terminatingJobsGauge", () -> 1.0 * this.jobManager.terminatingJobsMap.size()))
            .addGauge(new GaugeCallback(metricGroupId, "actorToJobIdMappingsGauge", () -> 1.0 * this.jobManager.actorToJobIdMap.size()))
            .addGauge(new GaugeCallback(metricGroupId, "numJobsStuckInAccepted",
                () -> 1.0 * this.jobManager.getJobsStuckInAccepted(Instant.now().toEpochMilli(),
                    getExpireAcceptedDelayMs()).size()))
            .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        this.metrics = m;
        this.numJobSubmissions = m.getCounter("numJobSubmissions");
        this.numJobActorCreationCounter = m.getCounter("numJobActorCreationCounter");
        this.numJobSubmissionFailures = m.getCounter("numJobSubmissionFailures");
        this.numJobShutdowns = m.getCounter("numJobShutdowns");
        this.numJobsInitialized = m.getCounter("numJobsInitialized");
        this.numJobClustersInitialized = m.getCounter("numJobClustersInitialized");
        this.numJobClusterInitializeFailures = m.getCounter("numJobClusterInitializeFailures");
        this.numJobClusterEnable = m.getCounter("numJobClusterEnable");
        this.numJobClusterDisable = m.getCounter("numJobClusterDisable");
        this.numJobClusterDelete = m.getCounter("numJobClusterDelete");
        this.numJobClusterUpdate = m.getCounter("numJobClusterUpdate");
        this.numJobClusterEnableErrors = m.getCounter("numJobClusterEnableErrors");
        this.numJobClusterDisableErrors = m.getCounter("numJobClusterDisableErrors");
        this.numJobClusterDeleteErrors = m.getCounter("numJobClusterDeleteErrors");
        this.numJobClusterUpdateErrors = m.getCounter("numJobClusterUpdateErrors");
        this.numSLAEnforcementExecutions = m.getCounter("numSLAEnforcementExecutions");
    }


    @Override
    public Receive createReceive() {
        return buildInitialBehavior();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /*
        JobCluster Actor behaviors 30 total

        // CLUSTER RELATED
     * - Init
     * - UpdateJC
     * - UpdateLabel
     * - UpdateSLA
     * - UpdateArtifact
     * - UpdateMigrationStrat
     *
     * - ENABLE JC
     * - DISABLE JC
     *
     * - GET CLUSTER
     * - DELETE
     *
     * - ENFORCE SLA
     * - TRIGGER CRON
     * - EXPIRE OLD JOBS
     *
     * - LIST archived workers
     * - LIST completed jobs
     * - GET LAST SUBMITTED JOB
     * - LIST JOB IDS
     * - LIST JOBS
     * - LIST WORKERS -> (pass thru to each Job Actor)
     *
     * // pass thru to JOB
     * - SUBMIT JOB -> (INIT JOB on Job Actor)
     * - GET JOB -> (pass thru Job Actor)
     * - GET JOB SCHED INFO -> (pass thru Job Actor)
     * - KILL JOB -> (pass thru Job Actor)
     * - RESUBMIT WORKER -> (pass thru Job Actor)
     * - KILL JOB Response
     * - JOB SHUTDOWN EVENT
     * - WORKER EVENT -> (pass thru Job Actor)
     * - SCALE JOB -> (pass thru Job Actor)
     *
     * - JOB INITED
     * - JOB STARTED
     * - GetJobDefinitionUpdatedFromJobActorResponse (resume job submit request)
    */
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * DISABLED BEHAVIOR
     * EXPECTED EVENTS (14)
     *
     *
     *   - UpdateJC
     *   - UpdateLabel
     *   - UpdateSLA
     *   - UpdateArtifact
     *   - UpdateMigrationStrat
     *   - ENABLE JC
     *   - GET CLUSTER
     *   - DELETE
     *   - LIST archived workers
     *   - LIST completed jobs
     *   - KILL JOB Response
     *   - JOB SHUTDOWN EVENT
     *   - EXPIRE OLD JOBS
     *   - WORKER EVENT ( KILL WORKER)
     *
     *  UNEXPECTED EVENTS (16)
     *  - Init
     *  - DISABLE JC
     *  - ENFORCE SLA
     *  - TRIGGER CRON
     *  - LIST JOB IDS
     *  - LIST JOBS
     *  - LIST WORKERS -> (pass thru to each Job Actor)
     *  - SUBMIT JOB -> (INIT JOB on Job Actor)
     *  - GetJobDefinitionUpdatedFromJobActorResponse (resume job submit request)
     *  - GET JOB -> (pass thru Job Actor)
     *  - GET JOB SCHED INFO -> (pass thru Job Actor)
     *  - KILL JOB -> (pass thru Job Actor)
     *  - RESUBMIT WORKER -> (pass thru Job Actor)
     *  - SCALE JOB -> (pass thru Job Actor)
     *  - JOB INITED
     *  - JOB STARTED
     *  - GET LAST SUBMITTED JOB
     *
     * @return
     */
    private Receive buildDisabledBehavior() {
        String state = "disabled";

        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN //
            .match(UpdateJobClusterRequest.class, this::onJobClusterUpdate)
            .match(UpdateJobClusterLabelsRequest.class, this::onJobClusterUpdateLabels)
            .match(UpdateJobClusterSLARequest.class, this::onJobClusterUpdateSLA)
            .match(UpdateJobClusterArtifactRequest.class, this::onJobClusterUpdateArtifact)
            .match(UpdateSchedulingInfo.class, this::onJobClusterUpdateSchedulingInfo)
            .match(UpdateJobClusterWorkerMigrationStrategyRequest.class, this::onJobClusterUpdateWorkerMigrationConfig)
            .match(GetJobClusterRequest.class   , this::onJobClusterGet)
            .match(JobClusterProto.DeleteJobClusterRequest.class, this::onJobClusterDelete)
            .match(ListArchivedWorkersRequest.class, this::onListArchivedWorkers)
            .match(ListCompletedJobsInClusterRequest.class, this::onJobListCompleted)
            .match(JobClusterProto.KillJobResponse.class, this::onKillJobResponse)
            .match(GetJobDetailsRequest.class, this::onGetJobDetailsRequest)
            .match(WorkerEvent.class, this::onWorkerEvent)
            .match(EnableJobClusterRequest.class, this::onJobClusterEnable)
            .match(Terminated.class, this::onTerminated)

            // EXPECTED MESSAGES END //

            // UNEXPECTED MESSAGES BEGIN //

            // from user job submit request
            .match(SubmitJobRequest.class, (x) -> getSender().tell(new SubmitJobResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty() ), getSelf()))
            .match(GetJobDefinitionUpdatedFromJobActorResponse.class, (x) -> getSender().tell(new SubmitJobResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty() ), getSelf()))
            .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(JobProto.JobInitialized.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(JobStartedEvent.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(ScaleStageRequest.class, (x) -> getSender().tell(new ScaleStageResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), 0), getSelf()))
            .match(KillJobRequest.class, (x) -> x.requestor.tell(new KillJobResponse(x.requestId, CLIENT_ERROR, JobState.Noop, genUnexpectedMsg(x.toString(), this.name, state), x.jobId, x.user), getSelf()))
            .match(GetJobDetailsRequest.class, (x) -> getSender().tell(new GetJobDetailsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(GetLastSubmittedJobIdStreamRequest.class, (x) -> getSender().tell(new GetLastSubmittedJobIdStreamResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(ListJobIdsRequest.class, (x) -> getSender().tell(new ListJobIdsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), new ArrayList()), getSelf()))
            .match(ListJobsRequest.class, (x) -> getSender().tell(new ListJobsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), new ArrayList()), getSelf()))
            .match(ListWorkersRequest.class, (x) -> getSender().tell(new ListWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), new ArrayList()), getSelf()))
            .match(JobClusterProto.EnforceSLARequest.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(JobClusterProto.TriggerCronRequest.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(DisableJobClusterRequest.class, (x) -> getSender().tell(new DisableJobClusterResponse(x.requestId, SUCCESS,"Cluster is already disabled"), getSelf()))
            .match(Terminated.class, this::onTerminated)
            .match(JobClusterProto.InitializeJobClusterRequest.class, (x) -> getSender().tell(new JobClustersManagerInitializeResponse(x.requestId, SUCCESS,"Cluster is already initialized"), getSelf()))

            // UNEXPECTED MESSAGES END //
            .matchAny(x -> logger.warn("unexpected message '{}' received by JobCluster actor {} in Disabled State", x, this.name))
            .build();
    }


    private String genUnexpectedMsg(String event, String cluster, String state) {
        return String.format("Unexpected message %s received by JobCluster actor %s in %s State", event, cluster, state);
    }

    /**
     * INITIAL BEHAVIOR
     * EXPECTED EVENTS (1)
     *   - Init
     *
     *
     *  UNEXPECTED EVENTS (29)
     *   - UpdateJC
     *   - UpdateLabel
     *   - UpdateSLA
     *   - UpdateArtifact
     *   - UpdateMigrationStrat
     *   - ENABLE JC
     *   - GET CLUSTER
     *   - DELETE
     *   - LIST archived workers
     *   - LIST completed jobs
     *   - KILL JOB Response
     *   - JOB SHUTDOWN EVENT
     *   - EXPIRE OLD JOBS
     *   - WORKER EVENT ( KILL WORKER)
     *  - DISABLE JC
     *  - ENFORCE SLA
     *  - TRIGGER CRON
     *  - LIST JOB IDS
     *  - LIST JOBS
     *  - LIST WORKERS -> (pass thru to each Job Actor)
     *  - SUBMIT JOB -> (INIT JOB on Job Actor)
     *  - GetJobDefinitionUpdatedFromJobActorResponse (resume job submit request)
     *  - GET JOB -> (pass thru Job Actor)
     *  - GET JOB SCHED INFO -> (pass thru Job Actor)
     *  - KILL JOB -> (pass thru Job Actor)
     *  - RESUBMIT WORKER -> (pass thru Job Actor)
     *  - SCALE JOB -> (pass thru Job Actor)
     *  - JOB INITED
     *  - JOB STARTED
     *  - GET LAST SUBMITTED JOB
     *
     * @return
     */

    private Receive buildInitialBehavior() {

        String state = "Uninited";

        return receiveBuilder()
            // EXPECTED MESSAGES BEGIN //
            .match(JobClusterProto.InitializeJobClusterRequest.class, this::onJobClusterInitialize)
            // EXPECTED MESSAGES END //

            // UNEXPECTED MESSAGES BEGIN //
            .match(UpdateJobClusterRequest.class, (x) -> getSender().tell(new UpdateJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(UpdateJobClusterLabelsRequest.class, (x) -> getSender().tell(new UpdateJobClusterLabelsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(UpdateJobClusterSLARequest.class, (x) -> getSender().tell(new UpdateJobClusterSLAResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(UpdateJobClusterArtifactRequest.class, (x) -> getSender().tell(new UpdateJobClusterArtifactResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(UpdateSchedulingInfo.class, (x) -> getSender().tell(new UpdateSchedulingInfoResponse(x.getRequestId(), CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(UpdateJobClusterWorkerMigrationStrategyRequest.class, (x) -> getSender().tell(new UpdateJobClusterWorkerMigrationStrategyResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(GetJobClusterRequest.class, (x) -> getSender().tell(new GetJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty() ), getSelf()))
            .match(JobClusterProto.DeleteJobClusterRequest.class, (x) -> getSender().tell(new DeleteJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(ListArchivedWorkersRequest.class, (x) -> getSender().tell(new ListArchivedWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), Lists.newArrayList()), getSelf()))
            .match(ListCompletedJobsInClusterRequest.class, (x) -> getSender().tell(new ListCompletedJobsInClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), Lists.newArrayList()), getSelf()))
            .match(JobClusterProto.KillJobResponse.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(GetJobDetailsRequest.class, (x) -> getSender().tell(new GetJobDetailsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(WorkerEvent.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(EnableJobClusterRequest.class, (x) -> getSender().tell(new EnableJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(SubmitJobRequest.class, (x) -> getSender().tell(new SubmitJobResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty() ), getSelf()))
            .match(GetJobDefinitionUpdatedFromJobActorResponse.class, (x) -> getSender().tell(new SubmitJobResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty() ), getSelf()))
            .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))
            .match(JobProto.JobInitialized.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(JobStartedEvent.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(ScaleStageRequest.class, (x) -> getSender().tell(new ScaleStageResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), 0), getSelf()))
            .match(KillJobRequest.class, (x) -> getSender().tell(new KillJobResponse(x.requestId, CLIENT_ERROR, JobState.Noop, genUnexpectedMsg(x.toString(), this.name, state), x.jobId, x.user), getSelf()))
            .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(GetLastSubmittedJobIdStreamRequest.class, (x) -> getSender().tell(new GetLastSubmittedJobIdStreamResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), empty()), getSelf()))
            .match(ListJobIdsRequest.class, (x) -> getSender().tell(new ListJobIdsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), Lists.newArrayList()), getSelf()))
            .match(ListJobsRequest.class, (x) -> getSender().tell(new ListJobsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), Lists.newArrayList()), getSelf()))
            .match(ListWorkersRequest.class, (x) -> getSender().tell(new ListWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state), Lists.newArrayList()), getSelf()))
            .match(JobClusterProto.EnforceSLARequest.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(JobClusterProto.TriggerCronRequest.class, (x) -> logger.warn(genUnexpectedMsg(x.toString(), this.name, state)))
            .match(DisableJobClusterRequest.class, (x) -> getSender().tell(new DisableJobClusterResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.name, state)), getSelf()))

            .match(Terminated.class, this::onTerminated)

            // UNEXPECTED MESSAGES END //
            .matchAny(x -> logger.warn("unexpected message '{}' received by JobCluster actor {} in Uninited State", x, this.name))
            .build();
        }


    /**
     * INITED BEHAVIOR
     *  EXPECTED EVENTS (29)
     *   - UpdateJC
     *   - UpdateLabel
     *   - UpdateSLA
     *   - UpdateArtifact
     *   - UpdateMigrationStrat
     *   - ENABLE JC
     *   - GET CLUSTER
     *   - DELETE
     *   - LIST archived workers
     *   - LIST completed jobs
     *   - KILL JOB Response
     *   - JOB SHUTDOWN EVENT
     *   - EXPIRE OLD JOBS
     *   - WORKER EVENT ( KILL WORKER)
     *  - DISABLE JC
     *  - ENFORCE SLA
     *  - TRIGGER CRON
     *  - LIST JOB IDS
     *  - LIST JOBS
     *  - LIST WORKERS -> (pass thru to each Job Actor)
     *  - SUBMIT JOB -> (INIT JOB on Job Actor)
     *  - GetJobDefinitionUpdatedFromJobActorResponse (resume job submit request)
     *  - GET JOB -> (pass thru Job Actor)
     *  - GET JOB SCHED INFO -> (pass thru Job Actor)
     *  - KILL JOB -> (pass thru Job Actor)
     *  - RESUBMIT WORKER -> (pass thru Job Actor)
     *  - SCALE JOB -> (pass thru Job Actor)
     *  - JOB INITED
     *  - JOB STARTED
     *  - GET LAST SUBMITTED JOB
     *
     *   UNEXPECTED EVENTS (1)
     *   - Init
     *
     *
     * @return
     */


    private Receive buildInitializedBehavior() {
        String state = "Initialized";
        return receiveBuilder()
                    // EXPECTED MESSAGES BEGIN //
                .match(UpdateJobClusterRequest.class, this::onJobClusterUpdate)
                .match(UpdateJobClusterLabelsRequest.class, this::onJobClusterUpdateLabels)
                .match(UpdateJobClusterSLARequest.class, this::onJobClusterUpdateSLA)
                .match(UpdateJobClusterArtifactRequest.class, this::onJobClusterUpdateArtifact)
                .match(UpdateSchedulingInfo.class, this::onJobClusterUpdateSchedulingInfo)
                .match(UpdateJobClusterWorkerMigrationStrategyRequest.class,
                        this::onJobClusterUpdateWorkerMigrationConfig)
                .match(EnableJobClusterRequest.class, (x) -> getSender().tell(
                        new EnableJobClusterResponse(x.requestId, SUCCESS, genUnexpectedMsg(x.toString(),
                                this.name, state)), getSelf()))
                .match(GetJobClusterRequest.class, this::onJobClusterGet)
                .match(JobClusterProto.DeleteJobClusterRequest.class, this::onJobClusterDelete)
                .match(ListArchivedWorkersRequest.class, this::onListArchivedWorkers)
                .match(ListCompletedJobsInClusterRequest.class, this::onJobListCompleted)
                .match(JobClusterProto.KillJobResponse.class, this::onKillJobResponse)
                .match(WorkerEvent.class, this::onWorkerEvent)
                .match(DisableJobClusterRequest.class, this::onJobClusterDisable)
                .match(JobClusterProto.EnforceSLARequest.class, this::onEnforceSLARequest)
                .match(JobClusterProto.BookkeepingRequest.class, this::onBookkeepingRequest)
                .match(JobClusterProto.TriggerCronRequest.class, this::onTriggerCron)
                .match(ListJobIdsRequest.class, this::onJobIdList)
                .match(ListJobsRequest.class, this::onJobList)
                .match(ListWorkersRequest.class, this::onListActiveWorkers)
                .match(SubmitJobRequest.class, this::onJobSubmit)
                .match(GetJobDefinitionUpdatedFromJobActorResponse.class, this::onGetJobDefinitionUpdatedFromJobActorResponse)
                .match(GetJobDetailsRequest.class, this::onGetJobDetailsRequest)
                .match(GetJobSchedInfoRequest.class, this::onGetJobStatusSubject)
                .match(GetLatestJobDiscoveryInfoRequest.class, this::onGetLatestJobDiscoveryInfo)
                .match(KillJobRequest.class, this::onJobKillRequest)
                .match(ResubmitWorkerRequest.class, this::onResubmitWorkerRequest)
                .match(JobProto.JobInitialized.class, this::onJobInitialized)
                .match(JobStartedEvent.class, this::onJobStarted)
                .match(GetLastSubmittedJobIdStreamRequest.class, this::onGetLastSubmittedJobIdSubject)
                .match(ScaleStageRequest.class, this::onScaleStage)
                 // EXPECTED MESSAGES END //
                 // EXPECTED MESSAGES BEGIN //
                .match(JobClusterProto.InitializeJobClusterRequest.class,(x) -> getSender().tell(
                        new JobClustersManagerInitializeResponse(x.requestId, SUCCESS,
                                "Cluster is already initialized"), getSelf()))
                // UNEXPECTED MESSAGES END //
                .match(Terminated.class, this::onTerminated)
                .matchAny(x -> {
                    logger.info("unexpected message '{}' received by JobCluster actor {} in Initialized State."
                            + "from class {}", x, this.name, x.getClass().getCanonicalName());
                    // TODO getSender().tell();
                })
                .build();
    }

    MetricGroupId getMetricGroupId(String name) {
        return new MetricGroupId("JobClusterActor", new BasicTag("jobCluster", name));
    }

    @Override
    public void preStart() throws Exception {
        logger.info("JobClusterActor {} started", name);
        super.preStart();
    }

    @Override
    public void postStop() throws Exception {
        logger.info("JobClusterActor {} stopped", name);
        super.postStop();
        if (name != null) {
            // de-register metrics from MetricsRegistry
            MetricsRegistry.getInstance().remove(getMetricGroupId(name));
        }
    }

    @Override
    public void preRestart(Throwable t, Optional<Object> m) throws Exception {
        logger.info("{} preRestart {} (exc: {})", name, m, t.getMessage());
        // do not kill all children, which is the default here
        // super.preRestart(t, m);
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        logger.info("{} postRestart (exc={})", name, reason.getMessage());
        super.postRestart(reason);
    }


    @Override
    public SupervisorStrategy supervisorStrategy() {
        // custom supervisor strategy to resume the child actors on Exception instead of the default restart
        return MantisActorSupervisorStrategy.getInstance().create();
    }


    private void setBookkeepingTimer(long checkAgainInSecs) {
        getTimers().startPeriodicTimer(BOOKKEEPING_TIMER_KEY, new JobClusterProto.BookkeepingRequest(),
                Duration.ofSeconds(checkAgainInSecs));
    }



    /**
     * Initialize cluster request sent by JCM. Called in following cases.
     * 1. Master bootup : Already exists in DB
     * 2. new cluster is being created : Requires the createInStore flag to be set. If writing to DB fails a
     * failure message is sent back. The caller should then kill this
     *      * actor and inform upstream of the failure
     *
     * @param initReq
     */
    @Override
    public void onJobClusterInitialize(JobClusterProto.InitializeJobClusterRequest initReq) {
        ActorRef sender = getSender();
        logger.info("In onJobClusterInitialize {}", this.name);
        if (logger.isDebugEnabled()) {
            logger.debug("Init Request {}", initReq);
        }
        jobClusterMetadata = new JobClusterMetadataImpl.Builder()
                .withLastJobCount(initReq.lastJobNumber)
                .withIsDisabled(initReq.isDisabled)
                .withJobClusterDefinition(initReq.jobClusterDefinition)
                .build();
        // create sla enforcer
        slaEnforcer = new SLAEnforcer(jobClusterMetadata.getJobClusterDefinition().getSLA());
        long expireFrequency = ConfigurationProvider.getConfig().getCompletedJobPurgeFrequencySeqs();
        String jobClusterName = jobClusterMetadata.getJobClusterDefinition().getName();
        // If cluster is disabled
        if(jobClusterMetadata.isDisabled()) {
            logger.info("Cluster {} initialized but is Disabled", jobClusterMetadata
                    .getJobClusterDefinition().getName());
            try {
                jobManager.initialize();
            } catch (Exception e) {
                sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId, CLIENT_ERROR,
                    String.format("JobCluster %s initialization failed",
                        initReq.jobClusterDefinition.getName()),
                    initReq.jobClusterDefinition.getName(), initReq.requestor), getSelf());
            }

            int count = 50;
            if(!initReq.jobList.isEmpty()) {
                logger.info("Cluster {} is disabled however it has {} active/accepted jobs",
                    jobClusterName, initReq.jobList.size());
                for(IMantisJobMetadata jobMeta : initReq.jobList) {
                    try {
                        if(count == 0) {
                            logger.info("Max cleanup limit of 50 reached abort");
                            break;
                        }
                        if(!JobState.isTerminalState(jobMeta.getState())) {
                            logger.info("Job {} is in non terminal state {} for disabled cluster {}."
                                    + "Marking it complete", jobMeta.getJobId(), jobMeta.getState(),
                                jobClusterName);
                            count--;
                            jobManager.markCompleted(jobMeta);
                            jobStore.archiveJob(jobMeta);
                        }
                    } catch (Exception e) {
                        logger.error("Exception {} archiving job {} during init ",e.getMessage(), jobMeta.getJobId(), e);
                    }
                }


            }

            sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId, SUCCESS,
                    String.format("JobCluster %s initialized successfully. But is currently disabled",
                            initReq.jobClusterDefinition.getName()),initReq.jobClusterDefinition.getName(),
                    initReq.requestor), getSelf());
            logger.info("Job expiry check frequency set to {}", expireFrequency);

            getContext().become(disabledBehavior);

            return;
        } else {
            // new cluster initialization
            if (initReq.createInStore) {
                try {
                    jobStore.createJobCluster(jobClusterMetadata);
                    eventPublisher.publishAuditEvent(
                            new LifecycleEventsProto.AuditEvent(
                                    LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_CREATE,
                                jobClusterName,
                                    "saved job cluster " + name)
                    );
                    logger.info("successfully saved job cluster {}", name);
                    numJobClustersInitialized.increment();

                } catch (final JobClusterAlreadyExistsException exists) {
                    numJobClusterInitializeFailures.increment();
                    logger.error("job cluster not created");
                    sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId, CLIENT_ERROR,
                            String.format("JobCluster %s already exists",
                                    initReq.jobClusterDefinition.getName()),
                            initReq.jobClusterDefinition.getName(), initReq.requestor), getSelf());
                    // TODO: handle case when job cluster exists in store but Job cluster actor is not running
                    return;
                } catch (final Exception e) {
                    numJobClusterInitializeFailures.increment();
                    logger.error("job cluster not created due to {}", e.getMessage(), e);
                    sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId,
                            SERVER_ERROR, String.format("JobCluster %s not created due to %s",
                            initReq.jobClusterDefinition.getName(), Throwables.getStackTraceAsString(e)),
                            initReq.jobClusterDefinition.getName(), initReq.requestor), getSelf());
                    // TODO: send PoisonPill to self if job cluster was not created ? Return'ing for now,
                    //  so we don't send back 2 InitJobClusterResponses
                    return;
                }
            }

            try {
                cronManager = new CronManager(name, getSelf(), jobClusterMetadata.getJobClusterDefinition().getSLA());
            } catch (Exception e) {
                logger.warn("Exception initializing cron", e);
                getSender().tell(new JobClusterManagerProto.CreateJobClusterResponse(
                    initReq.requestId, e instanceof SchedulerException?CLIENT_ERROR:SERVER_ERROR,
                    "Job Cluster " + jobClusterName + " could not be created due to cron initialization error" + e.getMessage(),
                    jobClusterName), getSelf());
                return;
            }
            initRunningJobs(initReq, sender);

            logger.info("Job expiry check frequency set to {}", expireFrequency);
            try {
                jobManager.initialize();
            } catch (Exception e) {
                sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId, CLIENT_ERROR,
                    String.format("JobCluster %s initialization failed",
                        initReq.jobClusterDefinition.getName()),
                    initReq.jobClusterDefinition.getName(), initReq.requestor), getSelf());
            }
        }

    }

    /**
     * Iterate through list of jobs in Active jobs table.
     * if a Job is completed move it completed table
     * else bootstrap the job (create actor, send init request)
     * Finally setup sla enforcement
     * @param initReq
     * @param sender
     */

    private void initRunningJobs(JobClusterProto.InitializeJobClusterRequest initReq, ActorRef sender) {
        List<IMantisJobMetadata> jobList = initReq.jobList;

         logger.info("In _initJobs for cluster {}: {} activeJobs", name, jobList.size());
         if (logger.isDebugEnabled()) {
            logger.debug("In _initJobs for cluster {} activeJobs -> {}", name, jobList);
         }

         Observable.from(jobList)
                 .flatMap((jobMeta) -> {
                     if(JobState.isTerminalState(jobMeta.getState())) {
                         try {
                             jobStore.archiveJob(jobMeta);
                             jobManager.markCompleted(jobMeta);
                         } catch (IOException e) {
                                logger.error("Exception archiving job {} during init ", jobMeta.getJobId(), e);
                         }
                         return Observable.empty();
                     } else {
                         if(jobMeta.getSchedulingInfo() == null) {
                             logger.error("Scheduling info is null for active job {} in cluster {}."
                                     + "Skipping bootstrap ", jobMeta.getJobId(), name);
                             return Observable.empty();
                         } else {
                             return Observable.just(jobMeta);
                         }
                     }
                 })
                 //
                 .flatMap((jobMeta) -> jobManager.bootstrapJob((MantisJobMetadataImpl)jobMeta, this.jobClusterMetadata))

                 .subscribe((jobInited) -> {
                            logger.info("Job Id {} initialized with code {}", jobInited.jobId, jobInited.responseCode);
                        },
                         (error) -> logger.warn("Exception initializing jobs {}", error.getMessage())
                         ,() -> {
                            // Push the last jobId

                             if(!initReq.jobList.isEmpty()) {
                                 JobId lastJobId = new JobId(this.name, initReq.lastJobNumber);
                                 JobId lastLaunchedJobId = initReq.jobList.stream()
                                     .filter(job -> job.getState() == JobState.Launched)
                                     .max(Comparator.comparingLong(a -> a.getJobId().getJobNum()))
                                     .map(jm -> new JobId(this.name, jm.getJobId().getJobNum()))
                                     .orElse(lastJobId);
                                logger.info("Publish last launched job id: {}, last submit job id: {}",
                                    lastLaunchedJobId, lastJobId);
                                 this.jobIdStartedSubject.onNext(lastLaunchedJobId);
                             }


                             setBookkeepingTimer(BOOKKEEPING_INTERVAL_SECS);

                             getContext().become(initializedBehavior);
                             logger.info("Job Cluster {} initialized", this.name);
                             sender.tell(new JobClusterProto.InitializeJobClusterResponse(initReq.requestId, SUCCESS,
                                     String.format("JobCluster %s initialized successfully",
                                             initReq.jobClusterDefinition.getName()),
                                      initReq.jobClusterDefinition.getName(), initReq.requestor), getSelf());
                         }
                 );



    }
    @Override
    public void onJobClusterUpdate(final UpdateJobClusterRequest request) {
        final String name = request.getJobClusterDefinition().getName();
        final ActorRef sender = getSender();

        String givenArtifactVersion = request.getJobClusterDefinition().getJobClusterConfig().getVersion();
        if (!isVersionUnique(givenArtifactVersion, jobClusterMetadata.getJobClusterDefinition()
                .getJobClusterConfigs())) {
            String msg = String.format("Job cluster %s not updated as the version %s is not unique", name,
                    givenArtifactVersion);
            logger.error(msg);
            sender.tell(new UpdateJobClusterResponse(request.requestId, CLIENT_ERROR, msg), getSelf());
            return;

        }

        IJobClusterDefinition currentJobClusterDefinition = jobClusterMetadata.getJobClusterDefinition();

        JobClusterDefinitionImpl mergedJobClusterDefinition = new JobClusterDefinitionImpl.Builder()
                .mergeConfigsAndOverrideRest(currentJobClusterDefinition, request.getJobClusterDefinition()).build();

        IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder()
                .withIsDisabled(jobClusterMetadata.isDisabled())
                .withLastJobCount(jobClusterMetadata.getLastJobCount())
                .withJobClusterDefinition(mergedJobClusterDefinition)
                .build();


        try {
            updateAndSaveJobCluster(jobCluster);
            sender.tell(new UpdateJobClusterResponse(request.requestId, SUCCESS, name
                    + " Job cluster updated"), getSelf());
            numJobClusterUpdate.increment();
        } catch  (Exception e) {
            logger.error("job cluster not created");
            sender.tell(new UpdateJobClusterResponse(request.requestId, SERVER_ERROR, name
                    + " Job cluster update failed " + e.getMessage()), getSelf());
            numJobClusterUpdateErrors.increment();
        }
    }



    @Override
    public void onJobClusterDelete(final JobClusterProto.DeleteJobClusterRequest request) {

        final ActorRef sender = getSender();
        try {
            if(jobManager.isJobListEmpty()) {
                jobManager.onJobClusterDeletion();
                jobStore.deleteJobCluster(name);
                logger.info("successfully deleted job cluster {}", name);
                eventPublisher.publishAuditEvent(
                        new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_DELETE, name, name + " deleted")
                );
                sender.tell(new JobClusterProto.DeleteJobClusterResponse(request.requestId, SUCCESS, name + " deleted", request.requestingActor, name), getSelf());
                numJobClusterDelete.increment();
            } else {
                logger.warn("job cluster {} cannot be deleted as it has active jobs", name);
                sender.tell(new JobClusterProto.DeleteJobClusterResponse(request.requestId, CLIENT_ERROR, name + " Job cluster deletion failed as there are active jobs", request.requestingActor,name), getSelf());
            }
        } catch( Exception e) {
            logger.error("job cluster {} not deleted", name, e);
            sender.tell(new JobClusterProto.DeleteJobClusterResponse(request.requestId, SERVER_ERROR, name + " Job cluster deletion failed " + e.getMessage(), request.requestingActor,name), getSelf());
            numJobClusterDeleteErrors.increment();
        }
    }

    @Override
    public void onJobIdList(final ListJobIdsRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Entering JCA:onJobIdList"); }
        final ActorRef sender = getSender();
        Set<JobId> jobIdsFilteredByLabelsSet = new HashSet<>();
        // If labels criterion is given prefilter by labels
        if(!request.getCriteria().getMatchingLabels().isEmpty()) {
            jobIdsFilteredByLabelsSet = jobManager.getJobsMatchingLabels(request.getCriteria().getMatchingLabels(), request.getCriteria().getLabelsOperand());
            // Found no matching jobs for given labels exit
            if(jobIdsFilteredByLabelsSet.isEmpty()) {
                sender.tell(new ListJobIdsResponse(request.requestId, SUCCESS, "No JobIds match given Label criterion", new ArrayList<>()), sender);
                if(logger.isTraceEnabled()) { logger.trace("Exit JCA:onJobIdList"); }
                return;
            }
        }

        // Found jobs matching labels or no labels criterion given.

        List<JobIdInfo> jobIdList;

        // Apply additional filtering to non terminal jobs
        jobIdList = getFilteredNonTerminalJobIdList(request.filters, jobIdsFilteredByLabelsSet);

        if(!request.getCriteria().getActiveOnly().orElse(true)) {
            jobIdList.addAll(getFilteredTerminalJobIdList(request.filters, jobIdsFilteredByLabelsSet));
        }

        sender.tell(new ListJobIdsResponse(request.requestId, SUCCESS, "", jobIdList), sender);
        if(logger.isTraceEnabled()) { logger.trace("Exit JCA:onJobIdList"); }
    }

    @Override
    public void onJobList(final ListJobsRequest request) {
        if(logger.isDebugEnabled()) { logger.info("Entering JCA:onJobList"); }
        final ActorRef sender = getSender();
        final ActorRef self = getSelf();
        Set<JobId> jobIdsFilteredByLabelsSet = new HashSet<>();
        // If labels criterion is given prefilter by labels
        if(!request.getCriteria().getMatchingLabels().isEmpty()) {
            jobIdsFilteredByLabelsSet = jobManager.getJobsMatchingLabels(request.getCriteria().getMatchingLabels(), request.getCriteria().getLabelsOperand());
            // Found no jobs matching labels exit
            if(jobIdsFilteredByLabelsSet.isEmpty()) {
                if(logger.isTraceEnabled()) { logger.trace("Exit JCA:onJobList {}" , jobIdsFilteredByLabelsSet.size()); }
                sender.tell(new ListJobsResponse(request.requestId, SUCCESS, "", new ArrayList<>()), self);
                return;
            }
        }

        // Found jobs matching labels or no labels criterion given.
        // Apply additional criterion to both active and completed jobs
        getFilteredNonTerminalJobList(request.getCriteria(), jobIdsFilteredByLabelsSet).mergeWith(
                getFilteredTerminalJobList(request.getCriteria(), jobIdsFilteredByLabelsSet))
            .collect(() -> Lists.<MantisJobMetadataView>newArrayList(), List::add)
            .doOnNext(resultList -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Exit JCA:onJobList {}", resultList.size());
                }
                sender.tell(new ListJobsResponse(request.requestId, SUCCESS, "", resultList), self);
            })
            .subscribe();
    }

    @Override
    public void onListArchivedWorkers(final ListArchivedWorkersRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("In onListArchiveWorkers {}", request); }
        try {
            List<IMantisWorkerMetadata> workerList = jobStore.getArchivedWorkers(request.getJobId().getId());
            if(workerList.size() > request.getLimit()) {
                workerList = workerList.subList(0, request.getLimit());
            }
            if(logger.isTraceEnabled()) { logger.trace("Returning {} archived Workers", workerList.size()); }
            getSender().tell(new ListArchivedWorkersResponse(request.requestId, SUCCESS, "", workerList), getSelf());
        } catch(Exception e) {
            logger.error("Exception listing archived workers", e);
            getSender().tell(new ListArchivedWorkersResponse(request.requestId, SERVER_ERROR, "Exception getting archived workers for job " + request.getJobId() + " -> " + e.getMessage(), Lists.newArrayList()), getSelf());
        }
    }

    public void onListActiveWorkers(final ListWorkersRequest r) {
        if(logger.isTraceEnabled()) { logger.trace("Enter JobClusterActor:onListActiveWorkers {}", r); }
        Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(r.getJobId());

        if(jobInfo.isPresent()) {
            jobInfo.get().jobActor.forward(r, getContext());
        } else {
            logger.warn("No such active job {} ", r.getJobId());
            getSender().tell(new ListWorkersResponse(r.requestId,CLIENT_ERROR,"No such active job " + r.getJobId(), Lists.newArrayList()),getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:onListActiveWorkers {}", r); }
    }


    private List<JobIdInfo> getFilteredNonTerminalJobIdList(ListJobCriteria request, Set<JobId> prefilteredJobIdSet) {
        if(logger.isTraceEnabled()) { logger.trace("Enter JobClusterActor:getFilteredNonTerminalJobIdList {}", request); }

        if((request.getJobState().isPresent() && request.getJobState().get().equals(JobState.MetaState.Terminal))) {
            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredNonTerminalJobIdList with empty"); }
            return Collections.emptyList();
        }
        List<JobInfo> jobInfoList;
        if(!prefilteredJobIdSet.isEmpty()) {
            jobInfoList = prefilteredJobIdSet.stream().map((jId) -> jobManager.getJobInfoForNonTerminalJob(jId))
                    .filter((jInfoOp) -> jInfoOp.isPresent()).map((jInfoOp) -> jInfoOp.get()).collect(Collectors.toList());
        } else {
            jobInfoList = jobManager.getAllNonTerminalJobsList();
        }

        List<JobInfo> shortenedList =  jobInfoList.subList(0, Math.min(jobInfoList.size(), request.getLimit().orElse(DEFAULT_LIMIT)));

        List<JobIdInfo> jIdList = shortenedList.stream()
                .map((JobInfo jInfo) -> new JobIdInfo.Builder()
                        .withJobId(jInfo.jobId)
                        .withJobState(jInfo.state)
                        .withSubmittedAt(jInfo.submittedAt)
                        .withTerminatedAt(jInfo.terminatedAt)
                        .withUser(jInfo.user)
                        .withVersion(jInfo.jobDefinition.getVersion())
                        .build())
                .collect(Collectors.toList());;

        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredNonTerminalJobIdList {}", jIdList.size()); }
        return jIdList;
    }

    private List<JobIdInfo> getFilteredTerminalJobIdList(ListJobCriteria request, Set<JobId> prefilteredJobIdSet) {
        if(logger.isTraceEnabled()) { logger.trace("Enter JobClusterActor:getFilteredTerminalJobIdList {}", request); }

        if((request.getJobState().isPresent() && !request.getJobState().get().equals(JobState.MetaState.Terminal))) {
            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredTerminalJobIdList with empty"); }
            return Collections.emptyList();
        } else if(!request.getJobState().isPresent() && (request.getActiveOnly().isPresent() && request.getActiveOnly().get())) {
            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredTerminalJobIdList with empty"); }
            return Collections.emptyList();
        }
        List<CompletedJob> completedJobsList;
        if(!prefilteredJobIdSet.isEmpty()) {
            completedJobsList = prefilteredJobIdSet.stream().map((jId) -> jobManager.getCompletedJob(jId)).filter((cjOp) -> cjOp.isPresent()).map((cjop) -> cjop.get()).collect(Collectors.toList());
        } else {
            completedJobsList = jobManager.getCompletedJobsList(
                request.getLimit().orElse(DEFAULT_LIMIT),
                request.getStartJobIdExclusive().orElse(null));
        }

        List<CompletedJob> subsetCompletedJobs = completedJobsList.subList(0, Math.min(completedJobsList.size(), request.getLimit().orElse(DEFAULT_LIMIT)));

        List<JobIdInfo> completedJobIdList = subsetCompletedJobs.stream()
                .map((CompletedJob cJob) -> new JobIdInfo.Builder()
                        .withJobIdStr(cJob.getJobId())
                        .withVersion(cJob.getVersion())
                        .withUser(cJob.getUser())
                        .withSubmittedAt(cJob.getSubmittedAt())
                        .withTerminatedAt(cJob.getTerminatedAt())
                        .withJobState(cJob.getState())
                        .build())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredTerminalJobIdList {}", completedJobIdList.size()); }
        return completedJobIdList;
    }


    private Observable<MantisJobMetadataView> getFilteredNonTerminalJobList(ListJobCriteria request, Set<JobId> prefilteredJobIdSet) {
        if(logger.isTraceEnabled()) { logger.trace("Entering JobClusterActor:getFilteredNonTerminalJobList"); }
        Duration timeout = Duration.ofMillis(500);

        if((request.getJobState().isPresent() && request.getJobState().get().equals(JobState.MetaState.Terminal))) {

            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredNonTerminalJobList with empty"); }
            return Observable.empty();
        }
        List<JobInfo> jobInfoList;
        //
        if(!prefilteredJobIdSet.isEmpty()) {
            jobInfoList = prefilteredJobIdSet.stream().map((jId) -> jobManager.getJobInfoForNonTerminalJob(jId))
                    .filter((jInfoOp) -> jInfoOp.isPresent()).map((jInfoOp) -> jInfoOp.get()).collect(Collectors.toList());
        } else {
            // no prefiltering applied start with complete set of non terminal jobs
            jobInfoList = jobManager.getAllNonTerminalJobsList();
        }

        List<JobInfo> shortenedList =  jobInfoList.subList(0, Math.min(jobInfoList.size(), request.getLimit().orElse(DEFAULT_ACTIVE_JOB_LIMIT)));
        if(logger.isDebugEnabled()) { logger.debug("List of non terminal jobs {}", jobInfoList); }
        return Observable.from(shortenedList)
                .flatMap((jInfo) -> {
                    GetJobDetailsRequest req = new GetJobDetailsRequest("system", jInfo.jobId);
                    CompletionStage<GetJobDetailsResponse> respCS = ask(jInfo.jobActor, req, timeout)
                        .thenApply(GetJobDetailsResponse.class::cast);
                    return Observable.from(respCS.toCompletableFuture(), Schedulers.io())
                        .onErrorResumeNext(ex -> {
                                    logger.warn("caught exception {}", ex.getMessage(), ex);
                                    return Observable.empty();
                                });
                })
                .filter((resp) -> resp != null && resp.getJobMetadata().isPresent())
                .map((resp) -> resp.getJobMetadata().get())
                .map((metaData) -> new MantisJobMetadataView(metaData, request.getStageNumberList(),
                        request.getWorkerIndexList(), request.getWorkerNumberList(), request.getWorkerStateList(),false));


    }

    /**
     * JobState ActiveOnly Execute?
     *   None    None        Y
     *   None    TRUE        N
     *   None    FALSE       Y
     *   Active  None        N
     *   Active  TRUE        N
     *   Active  FALSE       N
     *   Terminal None       Y
     *   Terminal TRUE       Y
     *   Terminal FALSE      Y
     * @param request
     * @return
     */

    private Observable<MantisJobMetadataView> getFilteredTerminalJobList(ListJobCriteria request, Set<JobId> jobIdSet) {
        if(logger.isTraceEnabled()) { logger.trace("JobClusterActor:getFilteredTerminalJobList"); }

        if((request.getJobState().isPresent() && !request.getJobState().get().equals(JobState.MetaState.Terminal))) {
            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredTerminalJobList with empty"); }
            return Observable.empty();
        } else if(!request.getJobState().isPresent() && (request.getActiveOnly().isPresent() && request.getActiveOnly().get())) {
            if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:getFilteredTerminalJobList with empty"); }
            return Observable.empty();
        }
        List<CompletedJob> jobInfoList;
        if(!jobIdSet.isEmpty()) {
            jobInfoList = jobIdSet.stream().map((jId) -> jobManager.getCompletedJob(jId))
                    .filter((compJobOp) -> compJobOp.isPresent()).map((compJobOp) -> compJobOp.get()).collect(Collectors.toList());
        } else {
            jobInfoList = jobManager.getCompletedJobsList(request.getLimit().orElse(DEFAULT_LIMIT), request.getStartJobIdExclusive().orElse(null));
        }

        List<CompletedJob> shortenedList =  jobInfoList.subList(0, Math.min(jobInfoList.size(), request.getLimit().orElse(DEFAULT_LIMIT)));

        return Observable.from(shortenedList)
                // terminatedAt comes from completed Job hence the different structure
                .flatMap((cJob) -> {
                    try {
                        if(logger.isDebugEnabled()) { logger.debug("Fetching details for completed job {}", cJob); }

                        Optional<IMantisJobMetadata> metaOp = jobManager.getJobDataForCompletedJob(cJob.getJobId());

                        if(metaOp.isPresent()) {
                            if(logger.isDebugEnabled()) { logger.debug ("Fetched details for completed job {} -> {}", cJob, metaOp.get()); }
                            return Observable.just(new MantisJobMetadataView(metaOp.get(),cJob.getTerminatedAt(), request.getStageNumberList(),
                                        request.getWorkerIndexList(), request.getWorkerNumberList(), request.getWorkerStateList(),false));

                        }
                    } catch(Exception e) {
                        logger.error("caught exception", e);
                        return Observable.empty();
                    }
                    return Observable.empty();
                });
    }


    @Override
    public void onJobListCompleted(final ListCompletedJobsInClusterRequest request) {
        if(logger.isTraceEnabled()) { logger.trace ("Enter onJobListCompleted {}", request); }
        final ActorRef sender = getSender();
        List<CompletedJob> completedJobsList = jobManager.getCompletedJobsList(request.getLimit(), null);
        sender.tell(new ListCompletedJobsInClusterResponse(request.requestId, SUCCESS, "", completedJobsList), sender);
        if(logger.isTraceEnabled()) { logger.trace ("Exit onJobListCompleted {}", completedJobsList.size()); }
    }
    @Override
    public void onJobClusterDisable(final DisableJobClusterRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobClusterDisable {}", req); }
        ActorRef sender = getSender();
        try {
            IJobClusterMetadata  jobClusterMetadata = new JobClusterMetadataImpl.Builder().withIsDisabled(true)
                    .withLastJobCount(this.jobClusterMetadata.getLastJobCount())
                    .withJobClusterDefinition((JobClusterDefinitionImpl)this.jobClusterMetadata.getJobClusterDefinition())
                    .build();
            //update store
            jobStore.updateJobCluster(jobClusterMetadata);
            this.jobClusterMetadata = jobClusterMetadata;
            cronManager.destroyCron();
            // change behavior to disabled
            getContext().become(disabledBehavior);

            // send kill requests for all non terminal jobs
            List<JobInfo> jobsToKill = new ArrayList<>();
            jobsToKill.addAll(jobManager.getAcceptedJobsList());
            jobsToKill.addAll(jobManager.getActiveJobsList());

            for(JobInfo jobInfo : jobsToKill) {
                jobInfo.jobActor.tell(
                        new KillJobRequest(
                                jobInfo.jobId, "Job cluster disabled", JobCompletedReason.Killed, req.getUser(), ActorRef.noSender()),
                        getSelf());
            }
            // disable SLA check timers
            getTimers().cancel(BOOKKEEPING_TIMER_KEY);
            eventPublisher.publishAuditEvent(
                    new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_DISABLED,
                        jobClusterMetadata.getJobClusterDefinition().getName(),
                        name + " disabled")
            );
            sender.tell(new DisableJobClusterResponse(req.requestId, SUCCESS, String.format("%s disabled", name)), getSelf());
            numJobClusterDisable.increment();
            logger.info("Job Cluster {} is disabbled", this.name);
        } catch (Exception e) {
            String errorMsg = "Exception disabling cluster " + name + " due to " + e.getMessage();
            logger.error(errorMsg,e);
            sender.tell(new DisableJobClusterResponse(req.requestId, SERVER_ERROR, errorMsg), getSelf());
            numJobClusterDisableErrors.increment();
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClusterDisable"); }

    }
    @Override
    public void onJobClusterEnable(final EnableJobClusterRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobClusterEnable"); }
        ActorRef sender = getSender();
        try {
            IJobClusterMetadata  jobClusterMetadata = new JobClusterMetadataImpl.Builder().withIsDisabled(false)
                    .withLastJobCount(this.jobClusterMetadata.getLastJobCount())
                    .withJobClusterDefinition((JobClusterDefinitionImpl)this.jobClusterMetadata.getJobClusterDefinition())
                    .build();
            if (cronManager == null) {
                cronManager = new CronManager(name, getSelf(), jobClusterMetadata.getJobClusterDefinition().getSLA());
            }
            this.cronManager.initCron();
            //update store after cron init
            jobStore.updateJobCluster(jobClusterMetadata);
            this.jobClusterMetadata = jobClusterMetadata;
            // change behavior to enabled
            getContext().become(initializedBehavior);

            //start SLA timer
            setBookkeepingTimer(BOOKKEEPING_INTERVAL_SECS);

            eventPublisher.publishAuditEvent(
                    new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_ENABLED,
                        this.jobClusterMetadata.getJobClusterDefinition().getName(), name + " enabled")
            );
            sender.tell(new EnableJobClusterResponse(req.requestId, SUCCESS, String.format("%s enabled", name)), getSelf());
            numJobClusterEnable.increment();
            logger.info("Job Cluster {} is Enabled", this.name);
        } catch(Exception e) {
            String errorMsg = String.format("Exception enabling cluster %s due to %s", name, e.getMessage());
            logger.error(errorMsg,e);
            sender.tell(new EnableJobClusterResponse(req.requestId, e instanceof SchedulerException?CLIENT_ERROR:SERVER_ERROR, errorMsg), getSelf());
            numJobClusterEnableErrors.increment();
        }
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobClusterEnable"); }
    }
    @Override
    public void onJobClusterGet(final GetJobClusterRequest request) {
        final ActorRef sender = getSender();
        if(logger.isTraceEnabled()) { logger.trace("In JobCluster Get " + jobClusterMetadata); }
        if(this.name.equals(request.getJobClusterName())) {
            MantisJobClusterMetadataView clusterView = generateJobClusterMetadataView(this.jobClusterMetadata, this.jobClusterMetadata.isDisabled(), ofNullable(this.cronManager).map(x -> x.isCronActive).orElse(false));
            sender.tell(new GetJobClusterResponse(request.requestId, SUCCESS, "", of(clusterView)), getSelf());
        } else {
            sender.tell(new GetJobClusterResponse(request.requestId, CLIENT_ERROR, "Cluster Name " + request.getJobClusterName() + " in request Does not match cluster Name " + this.name + " of Job Cluster Actor", Optional.empty()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClusterGet"); }
    }

    private MantisJobClusterMetadataView generateJobClusterMetadataView(IJobClusterMetadata jobClusterMetadata, boolean isDisabled, boolean cronActive) {
        return new MantisJobClusterMetadataView.Builder()
                .withName(jobClusterMetadata.getJobClusterDefinition().getName())
                .withDisabled(isDisabled)
                .withIsReadyForJobMaster(jobClusterMetadata.getJobClusterDefinition().getIsReadyForJobMaster())
                .withJars(jobClusterMetadata.getJobClusterDefinition().getJobClusterConfigs())
                .withJobOwner(jobClusterMetadata.getJobClusterDefinition().getOwner())
                .withLabels(jobClusterMetadata.getJobClusterDefinition().getLabels())
                .withLastJobCount(jobClusterMetadata.getLastJobCount())
                .withSla(jobClusterMetadata.getJobClusterDefinition().getSLA())
                .withMigrationConfig(jobClusterMetadata.getJobClusterDefinition().getWorkerMigrationConfig())
                .withParameters(jobClusterMetadata.getJobClusterDefinition().getParameters())
                .isCronActive(cronActive)
                .withLatestVersion(jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig().getVersion())

                .build();
    }

    @Override
    public void onJobSubmit(final SubmitJobRequest request) {
        final ActorRef sender = getSender();
        // if the job is submitted with a userDefinedType check to see if such a job is already running. If so just reply with a reference to it.
        if (request.getJobDefinition().isPresent()) {
            String uniq = request.getJobDefinition().get().getJobSla().getUserProvidedType();
            if(uniq != null && !uniq.isEmpty()) {
                Optional<JobInfo> existingJob = jobManager.getJobInfoByUniqueId(uniq);
                if(existingJob.isPresent()) {
                    logger.info("Job with unique {} already exists, returning its job Id {}", uniq, existingJob.get().jobId);
                    sender.tell(new SubmitJobResponse(request.requestId, SUCCESS, existingJob.get().jobId.getId(), of(existingJob.get().jobId)), getSelf());
                    return;
                }
            }
        }

        logger.info("Submitting job {}", request);
        try {
            if (requireJobActorProcess(request)) {
                logger.info("Sending job submit request to job actor for inheritance: {}", request.requestId);
                return;
            }

            JobDefinition resolvedJobDefn;
            if (request.isSubmitLatest()) {
                resolvedJobDefn = fromJobClusterDefinition(request.getSubmitter(), jobClusterMetadata.getJobClusterDefinition());
            } else {
                resolvedJobDefn = getResolvedJobDefinition(request.getSubmitter(), request.getJobDefinition());
            }
            eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobClusterStatusEvent(LifecycleEventsProto.StatusEvent.StatusEventType.INFO,
                "Job submit request received", jobClusterMetadata.getJobClusterDefinition().getName()));
            resolvedJobDefn = LabelManager.insertSystemLabels(resolvedJobDefn, request.isAutoResubmit());

            submitJob(resolvedJobDefn, sender, request.getSubmitter());

            numJobSubmissions.increment();

        }  catch (PersistException pe) {
            logger.error("Exception submitting job {} from {}", request.getClusterName(), request.getSubmitter(), pe);
            numJobSubmissionFailures.increment();
            sender.tell(new SubmitJobResponse(request.requestId, SERVER_ERROR, pe.getMessage(), empty()), getSelf());
        } catch (Exception e) {
            logger.error("Exception submitting job {} from {}", request.getClusterName(), request.getSubmitter(), e);
            numJobSubmissionFailures.increment();
            sender.tell(new SubmitJobResponse(request.requestId, CLIENT_ERROR, e.getMessage(), empty()), getSelf());
        }
    }

    public void onGetJobDefinitionUpdatedFromJobActorResponse(GetJobDefinitionUpdatedFromJobActorResponse request) {
        logger.info("Resuming job submission from job actor");

        // this request is returned by job actor but the response needs to be replied to the original job request sender (from API routes).
        ActorRef originalSender = request.getOriginalSender();
        if (request.responseCode == SERVER_ERROR || request.getJobDefinition() == null) {
            logger.error("Failed to retrieve job definition from job actor");
            numJobSubmissionFailures.increment();
            originalSender.tell(new SubmitJobResponse(request.requestId, SERVER_ERROR, request.message, empty()), getSelf());
            return;
        }

        try {
            JobDefinition resolvedJobDefn = request.getJobDefinition();

            // for quick submit the artifact version/name needs to be reset using the fork method below.
            if (request.isQuickSubmitMode()) {
                Optional<JobDefinition> jobDefinitionCloneO = cloneToNewJobDefinitionWithoutArtifactNameAndVersion(
                        request.getJobDefinition());
                if (jobDefinitionCloneO.isPresent()) {
                    resolvedJobDefn = jobDefinitionCloneO.get();
                }
            }

            resolvedJobDefn = this.jobDefinitionResolver.getResolvedJobDefinition(
                    request.getUser(), resolvedJobDefn, this.jobClusterMetadata);
            eventPublisher.publishStatusEvent(
                    new LifecycleEventsProto.JobClusterStatusEvent(LifecycleEventsProto.StatusEvent.StatusEventType.INFO,
                    "Job submit request received", jobClusterMetadata.getJobClusterDefinition().getName()));
            resolvedJobDefn = LabelManager.insertSystemLabels(resolvedJobDefn, request.isAutoResubmit());

            submitJob(resolvedJobDefn, originalSender, request.getUser());
            numJobSubmissions.increment();

        }  catch (PersistException pe) {
            logger.error("Exception submitting job {} from {}", this.name, request.getUser(), pe);
            numJobSubmissionFailures.increment();
            originalSender.tell(new SubmitJobResponse(request.requestId, SERVER_ERROR, pe.getMessage(), empty()), getSelf());
        } catch (Exception e) {
            logger.error("Exception submitting job {} from {}", this.name, request.getUser(), e);
            numJobSubmissionFailures.increment();
            originalSender.tell(new SubmitJobResponse(request.requestId, CLIENT_ERROR, e.getMessage(), empty()), getSelf());
        }
    }

    /**
     * If the job request requires process via job actor, inform the target job actor and return true to stop further
     * processing till the job actor replies.
     * Two cases require job actor level process:
     * 1. Quick submit (no job definition given) + valid active last job id.
     * 2. Regular submit with inheritance requirement + valid active last job id.
     * @param request job submission request.
     * @return true if further processing should stop.
     */
    private boolean requireJobActorProcess(final SubmitJobRequest request) {
        String user = request.getSubmitter();
        Optional<JobDefinition> givenJobDefn = request.getJobDefinition();
        List<JobInfo> existingJobsList = jobManager.getAllNonTerminalJobsList();

        Optional<JobId> lastJobId = JobListHelper.getLastSubmittedJobId(existingJobsList, Collections.emptyList());
        if (!lastJobId.isPresent()) {
            logger.info("No valid last job id found for inheritance. Skip job actor process step.");
            return false;
        }

        Optional<JobInfo> jobInfoForNonTerminalJob = jobManager.getJobInfoForNonTerminalJob(lastJobId.get());
        if (!jobInfoForNonTerminalJob.isPresent()) {
            logger.info("Last job id doesn't map to job info instance, skip job actor process. {}", lastJobId.get());
            return false;
        } else if (request.isSubmitLatest()) {
            logger.info("Submit latest job request, skip job actor process. {}", request);
            return false;
        } else if (!givenJobDefn.isPresent()) {
            logger.info("[QuickSubmit] pass to job actor to process job definition: {}", lastJobId.get());
            jobInfoForNonTerminalJob.get().jobActor.tell(
                    new GetJobDefinitionUpdatedFromJobActorRequest(
                            user, lastJobId.get(), jobInfoForNonTerminalJob.get().jobDefinition,
                            true, request.isAutoResubmit(), getSender()),
                    getSelf());
            return true;
        } else if (givenJobDefn.get().requireInheritInstanceCheck()) {
            logger.info("[Inherit request] pass to job actor to process job definition: {}", lastJobId.get());
            jobInfoForNonTerminalJob.get().jobActor.tell(
                    new GetJobDefinitionUpdatedFromJobActorRequest(
                            user, lastJobId.get(), givenJobDefn.get(),
                            false, request.isAutoResubmit(), getSender()),
                    getSelf());
            return true;
        }

        logger.info("request doesn't require job actor process, skip job actor and continue.");
        return false;
    }

    /**
     * Two cases
     * 1. JobDefinition provided by user: In this case check if labels / parameters or schedulingInfo was not provided
     * if that is the case inherit from the Cluster
     * 2. If JobDefinition is not provided, find the last submitted job and use its config (quick submit)
     * @param user submitter
     * @param givenJobDefnOp job defn provided by user in job submit
     * @return job definition to be used by the actual submit
     * @throws Exception If jobDefinition could not be resolved
     */
    private JobDefinition getResolvedJobDefinition(final String user, final Optional<JobDefinition> givenJobDefnOp) throws Exception {
        JobDefinition resolvedJobDefn;
        if (givenJobDefnOp.isPresent()) {
            if (givenJobDefnOp.get().getSchedulingInfo() != null && givenJobDefnOp.get().requireInheritInstanceCheck()) {
                logger.warn("Job requires inheriting instance count but has no active non-terminal job.");
            }
            resolvedJobDefn = givenJobDefnOp.get();
        }
        else {
            // no job definition specified , this is quick submit which is supposed to inherit from last job submitted
            // for request inheriting from non-terminal jobs, it has been sent to job actor instead.
            Optional<JobDefinition> jobDefnOp = cloneJobDefinitionForQuickSubmitFromArchivedJobs(
                    jobManager.getCompletedJobsList(1, null), empty(), jobStore);
            if (jobDefnOp.isPresent()) {
                logger.info("Inherited scheduling Info and parameters from previous job");
                resolvedJobDefn = jobDefnOp.get();
            } else if (this.jobClusterMetadata != null
                    && this.jobClusterMetadata.getJobClusterDefinition() != null &&
                    this.jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig() != null) {
                logger.info("No previous job definition found. Fall back to cluster definition: {}", this.name);
                IJobClusterDefinition clusterDefinition = this.jobClusterMetadata.getJobClusterDefinition();
                JobClusterConfig clusterConfig =
                        this.jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig();

                resolvedJobDefn = fromJobClusterDefinition(user, clusterDefinition);
                logger.info("Built job definition from cluster definition: {}", resolvedJobDefn);
            }
            else {
                    throw new Exception("Job Definition could not retrieved from a previous submission (There may "
                            + "not be a previous submission)");
            }
        }

        logger.info("Resolved JobDefn {}", resolvedJobDefn);
        return this.jobDefinitionResolver.getResolvedJobDefinition(user,resolvedJobDefn,this.jobClusterMetadata);
    }

    private JobDefinition fromJobClusterDefinition(String user, IJobClusterDefinition clusterDefinition) throws InvalidJobException {
        JobClusterConfig clusterConfig = clusterDefinition.getJobClusterConfig();
        return
            new JobDefinition.Builder()
                .withJobSla(new JobSla.Builder().build())
                .withJobJarUrl(clusterConfig.getJobJarUrl())
                .withArtifactName(clusterConfig.getArtifactName())
                .withVersion(clusterConfig.getVersion())
                .withLabels(clusterDefinition.getLabels())
                .withName(this.name)
                .withParameters(clusterDefinition.getParameters())
                .withSchedulingInfo(clusterConfig.getSchedulingInfo())
                .withUser(user)
                .build();
    }


    private void submitJob(JobDefinition jobDefinition, ActorRef sender, String user) throws PersistException {
        if (logger.isTraceEnabled()) { logger.trace("Enter submitJobb"); }
        JobId jId = null;
        try {
            validateJobDefinition(jobDefinition);
            long lastJobIdNumber = jobClusterMetadata.getLastJobCount();
            jId = new JobId(name, ++lastJobIdNumber);
            final int heartbeatIntervalSecs = jobDefinition.getIntSystemParameter(JOB_WORKER_HEARTBEAT_INTERVAL_SECS, 0);
            final int workerTimeoutSecs = jobDefinition.getIntSystemParameter(JOB_WORKER_TIMEOUT_SECS, 0);
            logger.info("Creating new job id: {} with job defn {}, with heartbeat {} and workertimeout {}",
                jId, jobDefinition, heartbeatIntervalSecs, workerTimeoutSecs);
            MantisJobMetadataImpl mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .withJobId(jId)
                    .withSubmittedAt(Instant.now())
                    .withJobState(JobState.Accepted)
                    .withNextWorkerNumToUse(1)
                    .withJobDefinition(jobDefinition)
                    .withHeartbeatIntervalSecs(heartbeatIntervalSecs)
                    .withWorkerTimeoutSecs(workerTimeoutSecs)
                    .build();

            eventPublisher.publishAuditEvent(
                new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_SUBMIT,
                    jId.getId(), jId + " submitter: " + user)
            );
            jobManager.initJob(mantisJobMetaData, jobClusterMetadata, sender);

            numJobActorCreationCounter.increment();
            jobClusterMetadata = new JobClusterMetadataImpl.Builder().withJobClusterDefinition((JobClusterDefinitionImpl)this.jobClusterMetadata.getJobClusterDefinition())
                    .withLastJobCount(lastJobIdNumber)
                    .withIsDisabled(jobClusterMetadata.isDisabled())
                    .build();
            try {
                jobStore.updateJobCluster(jobClusterMetadata);
            } catch (Exception e) {
                logger.error("Failed to persist job cluster {} error {}", jobClusterMetadata, e.getMessage(), e);
                numJobSubmissionFailures.increment();
                cleanUpOnJobSubmitFailure(jId);
                throw new PersistException(e);
            }

            numJobSubmissions.increment();
        } catch (PersistException pe) {
            throw pe;
        } catch (InvalidJobRequestException e) {
            logger.error( "Invalid jobcluster : {} error {}", jobClusterMetadata, e.getMessage(), e);
            numJobSubmissionFailures.increment();
            throw new IllegalArgumentException(e);

        } catch (Exception e) {
            logger.error("Exception persisting job in store", e);
            numJobSubmissionFailures.increment();
            throw new IllegalStateException(e);
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit submitJob"); }

    }
    @Override
    public void onJobInitialized(JobProto.JobInitialized jobInited) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobInitialized"); }
        jobManager.markJobInitialized(jobInited.jobId, System.currentTimeMillis());
        if(jobInited.responseCode == SUCCESS) {

            jobInited.requestor.tell(new SubmitJobResponse(jobInited.requestId, SUCCESS, jobInited.jobId.getId(), of(jobInited.jobId)), getSelf());
            numJobsInitialized.increment();
        } else {
            logger.warn("Job was not initialized {}" , jobInited);
            Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(jobInited.jobId);
            if(jobInfo.isPresent()) {
                cleanUpOnJobSubmitFailure(jobInfo.get().jobId);
                // if this is not a cron submission inform the caller
                if(jobInited.requestor != null)
                    jobInited.requestor.tell(new SubmitJobResponse(jobInited.requestId, jobInited.responseCode, "Job " + jobInited.jobId + " submission failed", ofNullable(jobInited.jobId)), getSelf());
            } else {
                logger.warn("No such job found {}", jobInited.jobId);
            }

        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobInitialized"); }
    }

    /**
     * When a Job starts evaluate SLA to ensure the number of running jobs satisfies the SLA
     * @param startedEvent JobStarted Event
     */
    @Override
    public void onJobStarted(final JobStartedEvent startedEvent) {
        logger.info("job {} started event", startedEvent.jobid);

        Optional<JobInfo> jobInfoOp = jobManager.getJobInfoForNonTerminalJob(startedEvent.jobid);

        if(jobInfoOp.isPresent()) {
            // enforce SLA
            jobManager.markJobStarted(jobInfoOp.get());
            this.jobIdStartedSubject.onNext(startedEvent.jobid);
            getSelf().tell(new JobClusterProto.EnforceSLARequest(Instant.now(), of(jobInfoOp.get().jobDefinition)), getSelf());
        }

    }

    private void cleanUpOnJobSubmitFailure(JobId jId) {
        if(logger.isTraceEnabled()) { logger.trace("Enter cleanUpOnJobSubmitFailure {}", jId); }
        if(jId != null) {
            Optional<JobInfo> jobInfoOp = jobManager.getJobInfoForNonTerminalJob(jId);
            if (jobInfoOp.isPresent()) { // ensure there is a record of this job
                JobInfo jobInfo = jobInfoOp.get();
                if (jobManager.markJobTerminating(jobInfo, JobState.Failed)) { // mark job as terminating
                    getContext().unwatch(jobInfo.jobActor);
                    getContext().stop(jobInfo.jobActor);
                    try {
                        jobManager.markCompleted(jId, System.currentTimeMillis(), JobState.Failed);
                    } catch (IOException e) {
                        logger.error("Failed to store the completed job {}", jId, e);
                    }
                    // clear it from initializing table if present
                    jobManager.markJobInitialized(jId, System.currentTimeMillis());
                } else {
                    logger.warn("cleanup on Job Submit failure failed for job {}", jId);
                }
            }
        } else {
            logger.warn("cleanup on Job Submit failure failed as there was no JobId");
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit cleanUpOnJobSubmitFailure {}", jId); }

    }

    /**
     *
     * @param definition Job Definition to be validated
     * @throws InvalidJobRequestException If the job definition is invalid
     */
    private void validateJobDefinition(JobDefinition definition) throws InvalidJobRequestException {
        if (definition == null) {
            throw new InvalidJobRequestException("MantisJobDefinition cannot be null");
        }
        if (definition.getJobJarUrl() == null) {
            throw new InvalidJobRequestException("MantisJobDefinition job jobJarUrl attribute cannot be null");
        }
        if (definition.getArtifactName() == null) {
            throw new InvalidJobRequestException("MantisJobDefinition job artifactName attribute cannot be null");
        }
        if (definition.getName() == null) {
            throw new InvalidJobRequestException("MantisJobDefinition name attribute cannot be null");
        }

        if (definition.getSchedulingInfo() == null) {
            throw new InvalidJobRequestException("MantisJobDefinition schedulingInfo cannot be null");
        }
    }

    @Override
    public void onWorkerEvent(WorkerEvent r) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onWorkerEvent {}", r); }
        Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(r.getWorkerId().getJobId());

        if(jobInfo.isPresent()) {
            jobInfo.get().jobActor.forward(r, getContext());
        } else {

            if(!JobHelper.isTerminalWorkerEvent(r)) {
                logger.warn("Event from worker {} has no valid running job. Terminating worker ", r.getWorkerId());
                Optional<String> host = JobHelper.getWorkerHostFromWorkerEvent(r);
                Optional<IMantisJobMetadata> completedJobOptional =
                    jobManager.getJobDataForCompletedJob(r.getWorkerId().getJobId());
                if (completedJobOptional.isPresent()) {
                    JobDefinition jobDefinition =
                        completedJobOptional.get().getJobDefinition();
                    mantisSchedulerFactory
                        .forJob(jobDefinition)
                        .unscheduleAndTerminateWorker(r.getWorkerId(), host);
                } else {
                    logger.warn("Non-terminal Event from worker {} has no completed job. Sending event to default cluster", r.getWorkerId());
                    mantisSchedulerFactory.forClusterID(null).unscheduleAndTerminateWorker(r.getWorkerId(), host);
                }
            } else {
                logger.warn("Terminal Event from worker {} has no valid running job. Ignoring event ", r.getWorkerId());
            }
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onWorkerEvent {}", r); }

    }

    /**
     * @param req Resubmit worker message
     */
    @Override
    public void onResubmitWorkerRequest(ResubmitWorkerRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onResubmitWorkerRequest {}", req); }
        onResubmitWorker(req);
        if(logger.isTraceEnabled()) { logger.trace("Exit onResubmitWorkerRequest {}", req); }
    }

    /**
     * Can be invoked in two ways
     * 1. User requests a job termination
     * 2. The job itself requests a termination due to
     *   a. Too many worker resubmits
     *   b. Max runtime limit has reached
     *   c. Subscription timeout reached
     * @param req Kill job request message
     */
    @Override
    public void onJobKillRequest(KillJobRequest req) {
        logger.info("JobClusterActor.onKillJobRequest {}", req);
        Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(req.jobId);
        ActorRef sender = getSender();
        if(jobInfo.isPresent() && jobManager.markJobTerminating(jobInfo.get(), JobState.Failed)) {
            jobInfo.get().jobActor.tell(req, getSelf());
        } else {
            logger.info("Job {} not found", req.jobId.getId() );
            if (req.requestor != null) {
                req.requestor.tell(
                        new JobClusterManagerProto.KillJobResponse(
                                req.requestId,
                                CLIENT_ERROR_NOT_FOUND,
                                JobState.Noop,
                                "Job " + req.jobId + " not found", req.jobId, req.user),
                        getSelf());
            }
        }
    }


    /**
     * Sent by job actor when the job shutdown is initiated.
     * @param resp Kill job response message
     */
    @Override
    public void onKillJobResponse(JobClusterProto.KillJobResponse resp) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onKillJobResponse {}", resp); }
        if (resp.responseCode == SUCCESS) {

            Optional<JobInfo> jInfo = jobManager.getJobInfoForNonTerminalJob(resp.jobId);
            if(jInfo.isPresent() ) {
                // stop watching actor
                getContext().unwatch(jInfo.get().jobActor);
                numJobShutdowns.increment();

                logger.info("Marking job {} as terminated", jInfo.get().jobId);
                // check requestor is not self to avoid an infinite loop
                if (resp.requestor != null && !getSelf().equals(resp.requestor)) {
                    resp.requestor.tell(
                            new KillJobResponse(resp.requestId, resp.responseCode, resp.state, resp.message, resp.jobId, resp.user),
                            getSelf());
                }

                try {
                    Optional<CompletedJob> completedJob = jobManager.markCompleted(resp.jobMetadata);
                    if(completedJob.isPresent()) {
                        logger.info("In cleanupAfterJobKill for Job {} in state {} and metadata {} ", resp.jobId, resp.state,resp.jobMetadata);

                        // enforce SLA
                        if(!jobClusterMetadata.isDisabled()) {
                            SLA sla = this.jobClusterMetadata.getJobClusterDefinition().getSLA();
                            if(sla.getMin() == 0 && sla.getMax() == 0) {
                                logger.info("{} No SLA specified nothing to enforce {}",
                                    completedJob.get().getJobId(), sla);
                            } else {
                                try {
                                    // first check if response has job meta for last job
                                    Optional<IMantisJobMetadata> cJob = Optional.of(resp.jobMetadata);

                                    if (cJob == null || !cJob.isPresent()) {
                                        // else check archived jobs
                                        cJob = jobStore.getArchivedJob(completedJob.get().getJobId());

                                    }
                                    if( cJob != null && cJob.isPresent()) {
                                        getSelf().tell(new JobClusterProto.EnforceSLARequest(Instant.now(), of(cJob.get().getJobDefinition())), ActorRef.noSender());
                                    } else {
                                        logger.warn("Could not load last terminated job to use for triggering enforce SLA");
                                    }
                                } catch (Exception e) {
                                    // should not get here
                                    logger.warn("Exception {} loading completed Job {} to enforce SLA due", e.getMessage(), completedJob.get().getJobId(), e);
                                }
                            }

                        }
                    } else {
                        logger.warn("Unable to mark job {} completed. ", resp.jobId);
                    }
                } catch (IOException e) {
                    logger.error("Unable to mark job {} completed. ", resp.jobId, e);
                }


            } else {
                // should not get here
                if (resp.requestor != null && !getSelf().equals(resp.requestor)) {
                    resp.requestor.tell(
                            new KillJobResponse(resp.requestId, CLIENT_ERROR, JobState.Noop, "Job not found", resp.jobId, resp.user),
                            getSelf());
                }
            }
        } else {
            if (resp.requestor != null && !getSelf().equals(resp.requestor)) {
                // kill job was not successful relay to caller
                resp.requestor.tell(
                        new KillJobResponse(resp.requestId, resp.responseCode, resp.state, resp.message, resp.jobId, resp.user),
                        getSelf());
            }
        }

        if(logger.isTraceEnabled()) { logger.trace("Exit onKillJobResponse {}", resp); }

    }


    @Override
    public void onGetJobDetailsRequest(GetJobDetailsRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Enter GetJobDetails {}", req); }
        GetJobDetailsResponse response = new GetJobDetailsResponse(req.requestId, CLIENT_ERROR_NOT_FOUND, "Job " + req.getJobId() + "  not found", empty());
        Optional<JobInfo> jInfo = jobManager.getJobInfoForNonTerminalJob(req.getJobId());
        if(jInfo.isPresent()) {
            if(logger.isDebugEnabled()) { logger.debug("Forwarding getJobDetails to job actor for {}", req.getJobId()); }
            jInfo.get().jobActor.forward(req, getContext());
            return;
        } else {
            // Could be a terminated job
            Optional<CompletedJob> completedJob = jobManager.getCompletedJob(req.getJobId());

            if(completedJob.isPresent()) {
                if(logger.isDebugEnabled()) { logger.debug("Found Job {} in completed state ", req.getJobId()); }
                try {
                    Optional<IMantisJobMetadata> jobMetaOp = jobStore.getArchivedJob(req.getJobId().getId());
                    if(jobMetaOp.isPresent()) {
                        response = new GetJobDetailsResponse(req.requestId, SUCCESS, "", jobMetaOp);

                    } else {
                        response = new GetJobDetailsResponse(req.requestId, CLIENT_ERROR_NOT_FOUND, "Job " + req.getJobId() + "  not found", empty());
                    }
                } catch (Exception e) {
                    logger.warn("Exception {} reading Job {} from Storage ", e.getMessage(), req.getJobId(), e);
                    response = new GetJobDetailsResponse(req.requestId, CLIENT_ERROR, "Exception reading Job " + req.getJobId() + "  " + e.getMessage(), empty());

                }
            } else {
                logger.debug("No such job {} ", req.getJobId());
            }
        }
        getSender().tell(response, getSelf());
        if(logger.isTraceEnabled()) { logger.trace("Exit GetJobDetails {}", req); }
    }

    @Override
    public void onGetLatestJobDiscoveryInfo(JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onGetLatestJobDiscoveryInfo {}", request); }
        ActorRef sender = getSender();
        if(this.name.equals(request.getJobCluster())) {
            JobId latestJobId = this.jobIdStartedSubject.getValue();
            logger.debug("[{}] latest job Id for cluster: {}", name, latestJobId);
            if (latestJobId != null) {
                Optional<JobInfo> jInfo = jobManager.getJobInfoForNonTerminalJob(latestJobId);
                if (jInfo.isPresent()) {
                   // ask job actor for discovery info
                    jInfo.get().jobActor.forward(request, getContext());
                } else {
                    logger.info("job info not found for job ID when looking up discovery info: {}", latestJobId);
                    sender.tell(new GetLatestJobDiscoveryInfoResponse(request.requestId,
                                                                      SERVER_ERROR,
                                                                      "JobInfo not found when looking up discovery info for " + latestJobId,
                                                                      empty()), getSelf());
                }
            } else {
                // no latest job ID found for this job cluster
                logger.debug("no latest Job ID found for job cluster {}", name);
                sender.tell(new GetLatestJobDiscoveryInfoResponse(request.requestId,
                                                                  CLIENT_ERROR_NOT_FOUND,
                                                                  "No latest jobId found for job cluster " + name,
                                                                  empty()), getSelf());
            }

        } else {
            String msg = "Job Cluster " + request.getJobCluster() + " In request does not match the name of this actor " + this.name;
            logger.warn(msg);
            sender.tell(new JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse(request.requestId, SERVER_ERROR, msg, empty()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onGetLatestJobDiscoveryInfo {}", request); }

    }

    @Override
    public void onGetJobStatusSubject(GetJobSchedInfoRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onGetJobStatusSubject {}", request); }

        Optional<JobInfo> jInfo = jobManager.getJobInfoForNonTerminalJob(request.getJobId());
        if(jInfo.isPresent()) {
            if(logger.isDebugEnabled()) { logger.debug("Forwarding getJobDetails to job actor for {}", request.getJobId()); }
            jInfo.get().jobActor.forward(request, getContext());

        } else {
            // Could be a terminated job
            GetJobSchedInfoResponse response = new GetJobSchedInfoResponse(request.requestId, CLIENT_ERROR, "Job " + request.getJobId() + "  not found or not active", empty());
            getSender().tell(response, getSelf());
        }

        if(logger.isTraceEnabled()) { logger.trace("Exit onGetJobStatusSubject "); }

    }
    @Override
    public void onGetLastSubmittedJobIdSubject(GetLastSubmittedJobIdStreamRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onGetLastSubmittedJobIdSubject {}", request); }
        ActorRef sender = getSender();
        if(this.name.equals(request.getClusterName())) {
            sender.tell(new GetLastSubmittedJobIdStreamResponse(request.requestId,SUCCESS,"",of(this.jobIdStartedSubject)),getSelf());
        } else {
            String msg = "Job Cluster " + request.getClusterName() + " In request does not match the name of this actor " + this.name;
            logger.warn(msg);
            sender.tell(new GetLastSubmittedJobIdStreamResponse(request.requestId,CLIENT_ERROR ,msg,empty()),getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onGetLastSubmittedJobIdSubject {}", request); }

    }

    @Override
    public void onBookkeepingRequest(JobClusterProto.BookkeepingRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onBookkeepingRequest for JobCluster {}", this.name); }
        // Enforce SLA if exists
        onEnforceSLARequest(new JobClusterProto.EnforceSLARequest());
        // Tell all child jobs to migrate workers on disabled VMs (if any)
        jobManager.actorToJobIdMap.keySet().forEach(actorRef -> actorRef.tell(new JobProto.MigrateDisabledVmWorkersRequest(request.time), ActorRef.noSender()));
        if(logger.isTraceEnabled()) { logger.trace("Exit onBookkeepingRequest for JobCluster {}", name); }
    }

    @Override
    public void onEnforceSLARequest(JobClusterProto.EnforceSLARequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onEnforceSLA for JobCluster {} with request", this.name, request); }
        numSLAEnforcementExecutions.increment();
        long now = request.timeOfEnforcement.toEpochMilli();
        List<JobInfo> pendingInitializationJobsPriorToCutoff = jobManager.getJobActorsStuckInInit(now, getExpirePendingInitializeDelayMs());

        List<JobInfo> jobsStuckInAcceptedList = jobManager.getJobsStuckInAccepted(now, getExpireAcceptedDelayMs());

        List<JobInfo> jobsStuckInTerminatingList = jobManager.getJobsStuckInTerminating(now, getExpireAcceptedDelayMs());

        if(!slaEnforcer.hasSLA()) {
            return;
        }

        int activeJobsCount = jobManager.activeJobsCount();
        int acceptedJobsCount = jobManager.acceptedJobsCount();
        // enforcing min
        int noOfJobsToLaunch = slaEnforcer.enforceSLAMin(activeJobsCount, acceptedJobsCount);
        if(noOfJobsToLaunch > 0) {
            logger.info("Submitting {} jobs for job name {} as active count is {} and accepted count is {}", noOfJobsToLaunch, name, activeJobsCount, acceptedJobsCount);
            String user = MANTIS_MASTER_USER;
            if(request.jobDefinitionOp.isPresent()) {
                user = request.jobDefinitionOp.get().getUser();
            }

            for(int i=0; i< noOfJobsToLaunch; i++) {

                getSelf().tell(new SubmitJobRequest(name, user, true,request.jobDefinitionOp), getSelf());
            }


            // enforce max.
        } else  {
            List<JobInfo> listOfJobs = new ArrayList<>(activeJobsCount + acceptedJobsCount);
            listOfJobs.addAll(jobManager.getActiveJobsList());
            listOfJobs.addAll(jobManager.getAcceptedJobsList());

            List<JobId> jobsToKill = slaEnforcer.enforceSLAMax(Collections.unmodifiableList(listOfJobs), this.slaHeadroomForAcceptedJobs);

            for (JobId jobId : jobsToKill) {
                logger.info("Request termination for job {}", jobId);
                getSelf().tell(
                        new KillJobRequest(
                                jobId, "SLA enforcement", JobCompletedReason.Killed, MANTIS_MASTER_USER, ActorRef.noSender()), getSelf());
            }

        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onEnforceSLA for JobCluster {}", name); }
    }

    private long getExpireAcceptedDelayMs() {
        // stuck in accepted for more than 10mins
        // TODO make part of config
        return 10*60*1000;
    }


    /**
     * Create a new JobDefinition using the given job definition. Inherit everything except the artifact name and version.
     * @param jobDefinition
     * @return Optional JobDefinition
     */
    private Optional<JobDefinition> cloneToNewJobDefinitionWithoutArtifactNameAndVersion(JobDefinition jobDefinition) {

        try {
            JobDefinition clonedJobDefn = new JobDefinition.Builder().withJobSla(jobDefinition.getJobSla())
                                                                    .withLabels(jobDefinition.getLabels())
                                                                    .withName(jobDefinition.getName())
                                                                    .withParameters(jobDefinition.getParameters())
                                                                    .withSchedulingInfo(jobDefinition.getSchedulingInfo())
                                                                    .withNumberOfStages(jobDefinition.getNumberOfStages())
                                                                    .withSubscriptionTimeoutSecs(jobDefinition.getSubscriptionTimeoutSecs())
                                                                    .withUser(jobDefinition.getUser())
                                                                    .build();
            return of(clonedJobDefn);
        } catch (Exception e) {
            logger.warn("Could not clone JobDefinition {} due to {}", jobDefinition, e.getMessage(), e);
            e.printStackTrace();
        }
        // should not get here

        return empty();


    }

    /**
     * Fetch JobDefn of last job and clone it to a create a new one. Inherit the schedulingInfo and parameters
     * @param completedJobs
     * @param jobDefinitionOp
     * @param store
     * @return
     */
    private Optional<JobDefinition> cloneJobDefinitionForQuickSubmitFromArchivedJobs(final List<CompletedJob> completedJobs,
                                                                                     Optional<JobDefinition> jobDefinitionOp,
                                                                                     MantisJobStore store) {
        if(logger.isTraceEnabled()) { logger.trace("Enter createNewJobDefinitionFromLastSubmittedInheritSchedInfoAndParameters"); }
        Optional<JobDefinition> lastSubmittedJobDefn = getLastSubmittedJobDefinition(completedJobs, jobDefinitionOp, store);

        if(lastSubmittedJobDefn.isPresent()) {
            if(logger.isTraceEnabled()) { logger.trace("Exit createNewJobDefinitionFromLastSubmittedInheritSchedInfoAndParameters"); }
            return cloneToNewJobDefinitionWithoutArtifactNameAndVersion(lastSubmittedJobDefn.get());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit createNewJobDefinitionFromLastSubmittedInheritSchedInfoAndParameters empty"); }
        return empty();
    }

    private long getExpirePendingInitializeDelayMs() {

        // jobs older than 60 secs
        return 60*1000;
    }

    /**
     * When cron fires
     * if a cron policy is keep_new then submit a new job
     * else skip if a job is running at the moment, if not then submit a new job
     * @param request Cron fired event
     */
    @Override
    public void onTriggerCron(JobClusterProto.TriggerCronRequest request) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onTriggerCron for Job Cluster {}", this.name);}
        if(jobClusterMetadata.getJobClusterDefinition().getSLA().getCronPolicy() != null) {

            if(jobClusterMetadata.getJobClusterDefinition().getSLA().getCronPolicy() == CronPolicy.KEEP_NEW ||
                    this.jobManager.getAllNonTerminalJobsList().size() == 0) {
                getSelf().tell(new SubmitJobRequest(name, MANTIS_MASTER_USER, empty(), false), getSelf());
            } else {

                    // A job is already running skip resubmiting
                logger.info(name + ": Skipping submitting new job upon cron trigger, one exists already");
            }
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onTriggerCron Triggered for Job Cluster {}", this.name);}
    }

    private long getTerminatedJobToDeleteDelayHours() {
        return ConfigurationProvider.getConfig().getTerminatedJobToDeleteDelayHours();
    }

    @Override
    public void onJobClusterUpdateSLA(UpdateJobClusterSLARequest slaRequest) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobClusterUpdateSLA {}", slaRequest); }
        ActorRef sender = getSender();
        try {
            SLA newSla = new SLA(slaRequest.getMin(), slaRequest.getMax(), slaRequest.getCronSpec(), slaRequest.getCronPolicy());
            JobClusterDefinitionImpl updatedDefn = new JobClusterDefinitionImpl.Builder().from(jobClusterMetadata.getJobClusterDefinition())
                    .withSla(newSla)
                    .build();
            boolean isDisabled = jobClusterMetadata.isDisabled();
            if(slaRequest.isForceEnable() && jobClusterMetadata.isDisabled()) {
                isDisabled = false;
            }
            IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder()
                    .withIsDisabled(isDisabled)
                    .withLastJobCount(jobClusterMetadata.getLastJobCount())
                    .withJobClusterDefinition(updatedDefn)
                    .build();

            if(cronManager != null)
                cronManager.destroyCron();
            this.cronManager = new CronManager(name, getSelf(), newSla);
            updateAndSaveJobCluster(jobCluster); //update after cron succeeds
            sender.tell(new UpdateJobClusterSLAResponse(slaRequest.requestId, SUCCESS, name + " SLA updated"), getSelf());

            eventPublisher.publishAuditEvent(
                    new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_UPDATE,
                        jobClusterMetadata.getJobClusterDefinition().getName(), name+" SLA update")
            );
        } catch(IllegalArgumentException | SchedulerException e) {
            logger.error("Invalid arguement job cluster not updated ", e);
            sender.tell(new UpdateJobClusterSLAResponse(slaRequest.requestId, CLIENT_ERROR, name + " Job cluster SLA update failed " + e.getMessage()), getSelf());

        } catch(Exception e) {
            logger.error("job cluster not updated ", e);
            sender.tell(new UpdateJobClusterSLAResponse(slaRequest.requestId, SERVER_ERROR, name + " Job cluster SLA update failed " + e.getMessage()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClusterUpdateSLA {}", slaRequest); }
    }

    @Override
    public void onJobClusterUpdateLabels(UpdateJobClusterLabelsRequest labelRequest) {
        if(logger.isTraceEnabled()) { logger.trace("Enter onJobClusterUpdateLabels {}", labelRequest); }
        ActorRef sender = getSender();
        try {
            JobClusterConfig newConfig = new JobClusterConfig.Builder().from(jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig())

                    .build();
            JobClusterDefinitionImpl updatedDefn = new JobClusterDefinitionImpl.Builder().from(jobClusterMetadata.getJobClusterDefinition())
                    .withJobClusterConfig(newConfig)
                    .withLabels(labelRequest.getLabels())
                    .build();
            IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder()
                    .withIsDisabled(jobClusterMetadata.isDisabled())
                    .withLastJobCount(jobClusterMetadata.getLastJobCount())
                    .withJobClusterDefinition(updatedDefn)
                    .build();

            updateAndSaveJobCluster(jobCluster);

            sender.tell(new UpdateJobClusterLabelsResponse(labelRequest.requestId, SUCCESS, name + " labels updated"), getSelf());

            eventPublisher.publishAuditEvent(
                    new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_UPDATE,
                        jobClusterMetadata.getJobClusterDefinition().getName(),
                        name + " update labels")
            );
        } catch(Exception e) {
            logger.error("job cluster labels not updated ", e);
            sender.tell(new UpdateJobClusterLabelsResponse(labelRequest.requestId, SERVER_ERROR, name + " labels updation failed " + e.getMessage()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onJobClusterUpdateLabels {}", labelRequest); }
    }

    @Override
    public void onJobClusterUpdateArtifact(UpdateJobClusterArtifactRequest artifactReq) {
        if(logger.isTraceEnabled()) { logger.trace("Entering JobClusterActor:onJobClusterUpdateArtifact"); }
        ActorRef sender = getSender();
        try {
            if(!isVersionUnique(artifactReq.getVersion(), jobClusterMetadata.getJobClusterDefinition().getJobClusterConfigs())) {
                String msg = String.format("job cluster %s not updated as the version %s is not unique", name,artifactReq.getVersion());
                logger.error(msg);
                sender.tell(new UpdateJobClusterArtifactResponse(artifactReq.requestId, CLIENT_ERROR, msg), getSelf());
                return;
            }
            JobClusterConfig newConfig = new JobClusterConfig.Builder().from(jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig())
                    .withJobJarUrl(artifactReq.getjobJarUrl())
                    .withArtifactName(artifactReq.getArtifactName())
                    .withVersion(artifactReq.getVersion())
                    .withUploadedAt(System.currentTimeMillis())
                    .build();

            updateJobClusterConfig(newConfig);
            if(!artifactReq.isSkipSubmit()) {
                getSelf().tell(new SubmitJobRequest(name,artifactReq.getUser(), (empty()), false), getSelf());
            }
            sender.tell(new UpdateJobClusterArtifactResponse(artifactReq.requestId, SUCCESS, name + " artifact updated"), getSelf());
        } catch(Exception e) {
            logger.error("job cluster not updated ", e);
            sender.tell(new UpdateJobClusterArtifactResponse(artifactReq.requestId, SERVER_ERROR, name + " Job cluster artifact updation failed " + e.getMessage()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:onJobClusterUpdateArtifact"); }
    }

    private void updateJobClusterConfig(JobClusterConfig newConfig) throws Exception {
        JobClusterDefinitionImpl updatedDefn = new JobClusterDefinitionImpl.Builder().from(jobClusterMetadata.getJobClusterDefinition())
            .withJobClusterConfig(newConfig)
            .build();
        IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder()
            .withIsDisabled(jobClusterMetadata.isDisabled())
            .withLastJobCount(jobClusterMetadata.getLastJobCount())
            .withJobClusterDefinition(updatedDefn)
            .build();

        updateAndSaveJobCluster(jobCluster);

        eventPublisher.publishAuditEvent(
            new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_UPDATE,
                jobClusterMetadata.getJobClusterDefinition().getName(),
                name + " artifact update")
        );
    }

    @Override
    public void onJobClusterUpdateSchedulingInfo(UpdateSchedulingInfo request) {
        ActorRef sender = getSender();
        try {
            if (!isVersionUnique(request.getVersion(), jobClusterMetadata.getJobClusterDefinition().getJobClusterConfigs())) {
                String msg = String.format(
                    "job cluster %s not updated as the version %s is not unique", name,
                    request.getVersion());
                logger.error(msg);
                sender.tell(
                    new UpdateSchedulingInfoResponse(request.getRequestId(), CLIENT_ERROR, msg),
                    getSelf());
                return;
            }

            JobClusterConfig newConfig = new JobClusterConfig.Builder().from(
                    jobClusterMetadata.getJobClusterDefinition().getJobClusterConfig())
                .withVersion(request.getVersion())
                .withSchedulingInfo(request.getSchedulingInfo())
                .withUploadedAt(System.currentTimeMillis())
                .build();
            updateJobClusterConfig(newConfig);
            sender.tell(
                new UpdateSchedulingInfoResponse(request.getRequestId(), SUCCESS,
                    name + " schedulingInfo updated"), getSelf());
        } catch (Exception e) {
            logger.error("job cluster not updated ", e);
            sender.tell(new UpdateSchedulingInfoResponse(request.getRequestId(), SERVER_ERROR, name + " Job cluster schedulingInfo update failed " + e.getMessage()), getSelf());
        }
    }

    boolean isVersionUnique(String artifactVersion, List<JobClusterConfig> existingConfigs) {
        if(logger.isTraceEnabled()) { logger.trace("Enter JobClusterActor {} isVersionnique {} existing versions {}",name,artifactVersion,existingConfigs);}
        for(JobClusterConfig config : existingConfigs) {
            if(config.getVersion().equals(artifactVersion)) {
                logger.info("Given Version {} is not unique during UpdateJobCluster {}",artifactVersion, name);
                return false;
            }
        }
        return true;
    }

    //TODO validate the migration config json
    @Override
    public void onJobClusterUpdateWorkerMigrationConfig(UpdateJobClusterWorkerMigrationStrategyRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Entering JobClusterActor:onJobClusterUpdateWorkerMigrationConfig {}", req); }
        ActorRef sender = getSender();
        try {

            JobClusterDefinitionImpl updatedDefn = new JobClusterDefinitionImpl.Builder().from(jobClusterMetadata.getJobClusterDefinition())
                    .withMigrationConfig(req.getMigrationConfig())
                    .build();
            IJobClusterMetadata jobCluster = new JobClusterMetadataImpl.Builder()
                    .withIsDisabled(jobClusterMetadata.isDisabled())
                    .withLastJobCount(jobClusterMetadata.getLastJobCount())
                    .withJobClusterDefinition(updatedDefn)
                    .build();

            updateAndSaveJobCluster(jobCluster);

            sender.tell(new UpdateJobClusterWorkerMigrationStrategyResponse(req.requestId, SUCCESS, name + " worker migration config updated"), getSelf());
            eventPublisher.publishAuditEvent(
                    new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.JOB_CLUSTER_UPDATE,
                        jobClusterMetadata.getJobClusterDefinition().getName(),
                        name + " worker migration config update")
            );
        } catch(Exception e) {
            logger.error("job cluster migration config not updated ", e);
            sender.tell(new UpdateJobClusterWorkerMigrationStrategyResponse(req.requestId, SERVER_ERROR, name + " Job cluster worker migration config updation failed " + e.getMessage()), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:onJobClusterUpdateWorkerMigrationConfig {}", req); }
    }

    private void updateAndSaveJobCluster(IJobClusterMetadata jobCluster) throws Exception {
        if(logger.isTraceEnabled()) { logger.trace("Entering JobClusterActor:updateAndSaveJobCluster {}", jobCluster.getJobClusterDefinition().getName()); }
        jobStore.updateJobCluster(jobCluster);
        jobClusterMetadata = jobCluster;
        // enable cluster if
        if(!jobClusterMetadata.isDisabled()) {
            getContext().become(initializedBehavior);
        }
        slaEnforcer = new SLAEnforcer(jobClusterMetadata.getJobClusterDefinition().getSLA());
        logger.info("successfully saved job cluster");
        if(logger.isTraceEnabled()) { logger.trace("Exit JobClusterActor:updateAndSaveJobCluster {}", jobCluster.getJobClusterDefinition().getName()); }
    }

    /**
     * Fetch job definition for quick submit mode.
     * If a job definition is passed return it immediately
     * Else find the last submitted job and look in completed job.
     * (For quick submit with active job, the request is passed to the active job actor to process instead).
     * @param completedJobs completed job list
     * @param jobDefinitionOp optional job definition
     * @param store store reference if required to load from store
     * @return JobDefinition of last submitted job if found
     */
    /*package protected*/
    private Optional<JobDefinition> getLastSubmittedJobDefinition(final List<CompletedJob> completedJobs,
                                                                  Optional<JobDefinition> jobDefinitionOp,
                                                                  MantisJobStore store) {
        if(logger.isTraceEnabled()) { logger.trace("Entering getLastSubmittedJobDefinition"); }
        if(jobDefinitionOp.isPresent()) {
            return jobDefinitionOp;
        }

        Optional<JobId> lastJobId = JobListHelper.getLastSubmittedJobId(Collections.emptyList(), completedJobs);
        if(lastJobId.isPresent()) {
            Optional<CompletedJob> completedJob = jobManager.getCompletedJob(lastJobId.get());
            if (completedJob.isPresent()) {
                try {
                    Optional<IMantisJobMetadata> archivedJob = store.getArchivedJob(completedJob.get().getJobId());
                    if(archivedJob.isPresent()) {
                        if(logger.isTraceEnabled()) { logger.trace("Exit getLastSubmittedJobDefinition returning job {} with defn {}", archivedJob.get().getJobId(), archivedJob.get().getJobDefinition()); }
                        return of(archivedJob.get().getJobDefinition());
                    } else {
                        logger.warn("Could not find load archived Job {} for cluster {}", completedJob.get().getJobId(), name);
                    }
                } catch (Exception e) {
                    logger.warn("Archived Job {} could not be loaded from the store due to {} ", completedJob.get().getJobId(), e.getMessage());
                }
            } else {
                logger.warn("Could not find any previous submitted/completed Job for cluster {}", name);
            }
        } else {
            logger.warn("Could not find any previous submitted Job for cluster {}", name);
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit getLastSubmittedJobDefinition empty"); }
        return empty();
    }

    /**
     * 2 cases this can occur
     * 1. Graceful shutdown : Where the job cluster actor requests the job actor to terminate. In this case we simply clear the pending
     * delete jobs map
     *
     *  2. Unexpected shutdown : The job actor terminated unexpectedly in which case we need to relaunch the actor.
     * @param terminatedEvent Event describing a job actor was terminated
     */
    private void onTerminated(Terminated terminatedEvent) {
        if(logger.isDebugEnabled()) { logger.debug("onTerminatedEvent {} ", terminatedEvent); }
        // TODO relaunch actor ?
    }

    @Override
    public void onScaleStage(ScaleStageRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Exit onScaleStage {}", req); }
        Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(req.getJobId());
        ActorRef sender = getSender();
        if(jobInfo.isPresent()) {
            jobInfo.get().jobActor.forward(req, getContext());
        } else {
            sender.tell(new ScaleStageResponse(req.requestId, CLIENT_ERROR,  "Job " + req.getJobId() + " not found. Could not scale stage to " + req.getNumWorkers(), 0), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit onScaleStage {}", req); }
    }

    @Override
    public void onResubmitWorker(ResubmitWorkerRequest req) {
        if(logger.isTraceEnabled()) { logger.trace("Exit JCA:onResubmitWorker {}", req); }
        Optional<JobInfo> jobInfo = jobManager.getJobInfoForNonTerminalJob(req.getJobId());
        ActorRef sender = getSender();
        if(jobInfo.isPresent()) {
            jobInfo.get().jobActor.forward(req, getContext());
        } else {
            sender.tell(new ResubmitWorkerResponse(req.requestId, CLIENT_ERROR,  "Job " + req.getJobId() + " not found. Could not resubmit worker"), getSelf());
        }
        if(logger.isTraceEnabled()) { logger.trace("Exit JCA:onResubmitWorker {}", req); }
    }


    static final class JobInfo  {

        final long submittedAt;
        public String version;
        volatile long initializeInitiatedAt = -1;
        volatile long initializedAt = -1;


        volatile long terminationInitiatedAt = -1;
        volatile long terminatedAt = -1;
        final JobId jobId;
        final ActorRef jobActor;
        volatile JobState state;
        final String user;
        final JobDefinition jobDefinition;

        JobInfo(JobId jobId, JobDefinition jobDefinition, long submittedAt, ActorRef jobActor, JobState state, String user, long initializeInitiatedAt, long initedAt) {
            this.submittedAt = submittedAt;
            this.jobActor = jobActor;
            this.jobId = jobId;
            this.state = state;
            this.user = user;
            this.jobDefinition = jobDefinition;
            this.initializeInitiatedAt = initializeInitiatedAt;
            this.initializedAt = initedAt;
        }

        @Override
        public String toString() {
            return "JobInfo{" +
                    "submittedAt=" + submittedAt +
                    ", initializeInitiatedAt=" + initializeInitiatedAt +
                    ", initializedAt=" + initializedAt +
                    ", terminationInitiatedAt=" + terminationInitiatedAt +
                    ", terminatedAt=" + terminatedAt +
                    ", jobId=" + jobId +
                    ", jobActor=" + jobActor +
                    ", state=" + state +
                    ", user='" + user + '\'' +
                    ", jobDefinition=" + jobDefinition +
                    '}';
        }

        void setInitializeInitiatedAt(long t) {
            this.initializeInitiatedAt = t;
        }

        void setInitializedAt(long t) {
            this.initializedAt = t;
        }

        void setState(JobState state) {
            this.state = state;
        }

        void setTerminationInitiatedAt(long terminationInitiatedAt) {
            this.terminationInitiatedAt = terminationInitiatedAt;
        }

        public void setTerminatedAt(long terminatedAt) {
            this.terminatedAt = terminatedAt;
        }



        JobInfo(JobId jobId, JobDefinition jobDefinition, long submittedAt, ActorRef jobActor, JobState state, String user) {
            this(jobId, jobDefinition, submittedAt, jobActor, state, user, -1, -1);
        }
        static class Builder {
             long submittedAt = -1;
             long initializeInitiatedAt = -1;
             long initializedAt = -1;
             JobId jobId = null;
             ActorRef jobActor = null;
             JobState state = null;
             String user = "";
             JobDefinition jobDefinition = null;

             Builder withSubmittedAt(long submittedAt) {
                 this.submittedAt = submittedAt;
                 return this;
             }

             Builder withInitializeInitiatedAt(long t) {
                 this.initializeInitiatedAt = t;
                 return this;
             }

             Builder withInitializedAt(long t) {
                 this.initializedAt = t;
                 return this;
             }

             Builder withJobId(JobId jId) {
                 this.jobId = jId;
                 return this;
             }

             Builder withJobActor(ActorRef actor) {
                 this.jobActor = actor;
                 return this;
             }

             Builder withJobDefinition(JobDefinition jd) {
                 this.jobDefinition = jd;
                 return this;
             }

             Builder withUser(String user) {
                 this.user = user;
                 return this;
             }

             Builder withState(JobState state) {
                 this.state = state;
                 return this;
             }


            Builder usingJobMetadata(MantisJobMetadataImpl jobMeta, ActorRef actor) {
                this.jobId = jobMeta.getJobId();
                this.jobDefinition = jobMeta.getJobDefinition();
                this.submittedAt = jobMeta.getSubmittedAtInstant().toEpochMilli();

                this.state = jobMeta.getState();
                this.user = jobMeta.getUser();

                this.jobActor = actor;

                 return this;
            }

            JobInfo build() {

                Preconditions.checkNotNull(jobId, "JobId cannot be null");
                Preconditions.checkNotNull(jobDefinition, "JobDefinition cannot be null");
                Preconditions.checkNotNull(state, "state cannot be null");
                Preconditions.checkNotNull(jobActor, "Job Actor cannot be null");

                return new JobInfo(jobId,jobDefinition,submittedAt,jobActor,state,user,initializeInitiatedAt,initializedAt);
            }

        }

    }

    /**
     * Responsible of keeping track of Jobs Belonging to this cluster.
     * As a job moves from Accepted -> Launched -> Terminating -> Completed states it is moved between
     * the corresponding maps.
     * This class is NOT ThreadSafe the caller should ensure it is not accessed concurrently
     * @author njoshi
     *
     */
    final static class JobManager {
        private final Logger logger = LoggerFactory.getLogger(JobManager.class);

        private final String name;
        // Map of Actor ref to JobId
        private final Map<ActorRef, JobId> actorToJobIdMap = new HashMap<>();

        // Map of Job Actors pending initialization
        private final ConcurrentMap<JobId, JobInfo> pendingInitializationJobsMap = new ConcurrentHashMap<>();

        // Map of Jobs in Launched state
        private final ConcurrentMap<JobId, JobInfo> activeJobsMap = new ConcurrentHashMap<>();

        // Map of Jobs in accepted state
        private final ConcurrentMap<JobId, JobInfo> acceptedJobsMap = new ConcurrentHashMap<>();

        private final Set<JobInfo> nonTerminalSortedJobSet = new TreeSet<>((o1, o2) -> {
            if (o1.submittedAt < o2.submittedAt) {
                return 1;
            } else if (o1.submittedAt > o2.submittedAt) {
                return -1;
            } else {
                return 0;
            }
        });

        // Cache that deals with completed job
        private final CompletedJobStore completedJobStore;

        // Map of Jobs in terminating state
        private final Map<JobId, JobInfo> terminatingJobsMap = new HashMap<>();


        private final ActorContext context;
        private final MantisSchedulerFactory scheduler;
        private final LifecycleEventPublisher publisher;

        private final MantisJobStore jobStore;
        private final CostsCalculator costsCalculator;

        private final LabelCache labelCache = new LabelCache();


        JobManager(String clusterName, ActorContext context, MantisSchedulerFactory schedulerFactory, LifecycleEventPublisher publisher, MantisJobStore jobStore, CostsCalculator costsCalculator) {
            this.name = clusterName;
            this.jobStore = jobStore;
            this.context = context;
            this.scheduler = schedulerFactory;
            this.publisher = publisher;
            this.completedJobStore = new CompletedJobStore(name, labelCache, jobStore);
            this.costsCalculator = costsCalculator;
        }

        void initialize() throws IOException {
            logger.debug("Loading completed jobs for cluster {}", name);
            completedJobStore.initialize();
            logger.debug("Initialized completed job store for cluster {}", name);
        }

        public void onJobClusterDeletion() throws IOException {
            completedJobStore.onJobClusterDeletion();
        }

        Observable<JobProto.JobInitialized> bootstrapJob(MantisJobMetadataImpl jobMeta, IJobClusterMetadata jobClusterMetadata) {

            // create jobInfo
            JobInfo jobInfo = createJobInfoAndActorAndWatchActor(jobMeta, jobClusterMetadata);

            // add to appropriate map
            actorToJobIdMap.put(jobInfo.jobActor, jobInfo.jobId);
            if (jobInfo.state.equals(JobState.Accepted)) {
                acceptedJobsMap.put(jobInfo.jobId, jobInfo);
                nonTerminalSortedJobSet.add(jobInfo);
            } else if (jobInfo.state.equals(JobState.Launched)) {
                activeJobsMap.put(jobInfo.jobId, jobInfo);
                nonTerminalSortedJobSet.add(jobInfo);
            } else if (jobInfo.state.equals(JobState.Terminating_abnormal) || jobInfo.state.equals(JobState.Terminating_normal)) {
                terminatingJobsMap.put(jobInfo.jobId, jobInfo);
                nonTerminalSortedJobSet.add(jobInfo);
            } else {
                logger.warn("Unexpected job state {}", jobInfo.state);
            }
            long masterInitTimeoutSecs = ConfigurationProvider.getConfig().getMasterInitTimeoutSecs();
            long timeout = ((masterInitTimeoutSecs - 60)) > 0 ? (masterInitTimeoutSecs - 60) : masterInitTimeoutSecs;
            Duration t = Duration.ofSeconds(timeout);

            // mark it as pending actor init
            markJobInitializeInitiated(jobInfo, System.currentTimeMillis());

            CompletionStage<JobProto.JobInitialized> respCS = ask(jobInfo.jobActor, new JobProto.InitJob(ActorRef.noSender(), false), t)
                    .thenApply(JobProto.JobInitialized.class::cast);
            return Observable.from(respCS.toCompletableFuture(), Schedulers.io())
                    .onErrorResumeNext(ex -> {
                        logger.warn("caught exception {}", ex.getMessage(), ex);
                        return Observable.just(new JobProto.JobInitialized(1, SERVER_ERROR, "Timeout initializing Job " + jobInfo.jobId + " exception -> " + ex.getMessage(), jobInfo.jobId, ActorRef.noSender()));
                    })
                    .map((jobInited) -> {
                        // once init response received remove from pending init map.
                        markJobInitialized(jobInited.jobId, System.currentTimeMillis());

                        return jobInited;
                    })
                    ;

        }

        JobInfo initJob(MantisJobMetadataImpl jobMeta, IJobClusterMetadata jobClusterMetadata, ActorRef sender) {

            JobInfo jobInfo = createJobInfoAndActorAndWatchActor(jobMeta, jobClusterMetadata);

            markJobAccepted(jobInfo);
            jobInfo.jobActor.tell(new JobProto.InitJob(sender, true), context.self());

            markJobInitializeInitiated(jobInfo, System.currentTimeMillis());

            return jobInfo;
        }

        JobInfo createJobInfoAndActorAndWatchActor(MantisJobMetadataImpl jobMeta, IJobClusterMetadata jobClusterMetadata) {

            MantisScheduler scheduler1 = scheduler.forJob(jobMeta.getJobDefinition());
            ActorRef jobActor = context.actorOf(JobActor.props(jobClusterMetadata.getJobClusterDefinition(),
                    jobMeta, jobStore, scheduler1, publisher, costsCalculator), "JobActor-" + jobMeta.getJobId().getId());


            context.watch(jobActor);
            // Add to label cache
            labelCache.addJobIdToLabelCache(jobMeta.getJobId(), jobMeta.getLabels());
            return new JobInfo.Builder()
                    .usingJobMetadata(jobMeta, jobActor)
                    .build();
        }

        void markJobInitialized(JobId jobId, long ts) {

            JobInfo removed = this.pendingInitializationJobsMap.remove(jobId);
            if (removed != null) {
                removed.setInitializedAt(ts);
            }
        }

        void markJobInitializeInitiated(JobInfo jobInfo, long ts) {
            jobInfo.setInitializeInitiatedAt(ts);
            // mark it as pending actor init
            pendingInitializationJobsMap.put(jobInfo.jobId, jobInfo);
        }

        /**
         * Called on Job Submit. Updates the acceptedJobsMap & actorMap
         *
         * @param jobInfo job info of accepted job
         * @return true if successful
         */
        boolean markJobAccepted(JobInfo jobInfo) {
            boolean isSuccess = false;

            if (!jobInfo.state.isValidStateChgTo(JobState.Accepted) || activeJobsMap.containsKey(jobInfo.jobId) || terminatingJobsMap.containsKey(jobInfo.jobId)) {
                String warn = String.format("Job %s already exists", jobInfo.jobId);
                logger.warn(warn);

            } else {
                this.acceptedJobsMap.put(jobInfo.jobId, jobInfo);
                this.actorToJobIdMap.put(jobInfo.jobActor, jobInfo.jobId);
                nonTerminalSortedJobSet.add(jobInfo);
                isSuccess = true;
            }
            return isSuccess;
        }

        List<JobInfo> getPendingInitializationJobsPriorToCutoff(long ts) {
            return this.pendingInitializationJobsMap.values().stream().filter((jInfo) -> {
                if (jInfo.initializedAt == -1 && jInfo.initializeInitiatedAt < ts) {
                    return true;
                }
                return false;
            })
                    .collect(Collectors.toList());
        }

        /**
         * Transition job to terminating state.
         *
         * @param jobInfo  For the job which is terminating
         * @param newState whether it is normal or abnormal termination
         * @return true if successful
         */

        boolean markJobTerminating(JobInfo jobInfo, JobState newState) {
            boolean isSuccess = false;

            if (JobState.isTerminalState(newState) && jobInfo.state.isValidStateChgTo(newState)) {
                this.activeJobsMap.remove(jobInfo.jobId);
                this.acceptedJobsMap.remove(jobInfo.jobId);
                nonTerminalSortedJobSet.add(jobInfo);
                jobInfo.setState(newState);

                this.terminatingJobsMap.put(jobInfo.jobId, jobInfo);
                jobInfo.setTerminationInitiatedAt(System.currentTimeMillis());
                isSuccess = true;
            } else {
                String warn = "Unexpected job terminating event " + jobInfo.jobId + " Invalid transition from state " + jobInfo.state + " to state " + newState + " ";
                logger.warn(warn);

            }
            return isSuccess;
        }


        /**
         * Marks the job as started by putting it into the activejobsmap
         * in case of a valid  transition
         *
         * @param jobInfo job info for the job that just started
         * @return true if successful and false if failed due to an invalid transition
         */

        boolean markJobStarted(JobInfo jobInfo) {
            boolean success = false;
            if (jobInfo.state.isValidStateChgTo(JobState.Launched)) {

                jobInfo.setState(JobState.Launched);

                // remove from accepted jobs map
                this.acceptedJobsMap.remove(jobInfo.jobId);
                // add to active jobs map
                this.activeJobsMap.put(jobInfo.jobId, jobInfo);

                nonTerminalSortedJobSet.add(jobInfo);
                success = true;

            } else {
                String warn = String.format("Unexpected job started event %s Invalid transition from state %s to state %s", jobInfo.jobId, jobInfo.state, JobState.Launched);
                logger.warn(warn);
            }
            return success;
        }

        Optional<CompletedJob> markCompleted(IMantisJobMetadata jobMetadata) throws IOException {
            JobId jId = jobMetadata.getJobId();
            if (logger.isTraceEnabled()) {
                logger.trace("Enter markCompleted job {}", jId);
            }
            Optional<JobInfo> jobInfoOp = getJobInfoForNonTerminalJob(jId);

            if (jobInfoOp.isPresent()) {
                JobInfo jInfo = jobInfoOp.get();
                jInfo.state = jobMetadata.getState();
                jInfo.setTerminatedAt(jobMetadata.getEndedAtInstant().get().toEpochMilli());
                this.acceptedJobsMap.remove(jId);
                this.terminatingJobsMap.remove(jId);
                this.activeJobsMap.remove(jId);
                this.actorToJobIdMap.remove(jobInfoOp.get().jobActor);
                this.nonTerminalSortedJobSet.remove(jInfo);

                if (logger.isTraceEnabled()) {
                    logger.trace("Exit markCompleted job {}", jId);
                }

                return Optional.of(this.completedJobStore.onJobCompletion(jobMetadata));
            } else {
                logger.warn("No such job {}", jId);
                return empty();
            }
//            return markCompleted(jId, System.currentTimeMillis(), jobMetadata, state);
        }

        /**
         * Invoked during clean up phase when the Job Actor has informed the Cluster that all workers have been terminated
         *
         * @param jId job id of the job that completed
         * @return An instance of CompletedJob that would be used to persist to storage.
         */
        Optional<CompletedJob> markCompleted(JobId jId, long completionTime, JobState state) throws IOException {
            if (logger.isTraceEnabled()) {
                logger.trace("Enter markCompleted job {}", jId);
            }
            Optional<JobInfo> jobInfoOp = getJobInfoForNonTerminalJob(jId);

            if (jobInfoOp.isPresent()) {
                JobInfo jInfo = jobInfoOp.get();
                jInfo.state = state;
                jInfo.setTerminatedAt(completionTime);
                this.acceptedJobsMap.remove(jId);
                this.terminatingJobsMap.remove(jId);
                this.activeJobsMap.remove(jId);
                this.actorToJobIdMap.remove(jobInfoOp.get().jobActor);
                this.nonTerminalSortedJobSet.remove(jInfo);

                if (logger.isTraceEnabled()) {
                    logger.trace("Exit markCompleted job {}", jId);
                }

                String version = null;
                return Optional.ofNullable(this.completedJobStore.onJobCompletion(jId, jInfo.submittedAt, completionTime, jInfo.user,
                    null,
                    state, jInfo.jobDefinition.getLabels()));

            } else {
                logger.warn("No such job {}", jId);
                return empty();
            }

        }

        List<JobInfo> getAllNonTerminalJobsList() {

            List<JobInfo> allJobsList = new ArrayList<>(this.nonTerminalSortedJobSet);
            if(logger.isTraceEnabled()) { logger.trace("Exiting JobClusterActor:getAllNonTerminatlJobsList {}", allJobsList); }
            return allJobsList;
        }


        /**
         * List of Jobs in accepted state.
         * @return list of accepted job info
         */

        List<JobInfo> getAcceptedJobsList() {
            List<JobInfo> acceptedJobsList = Lists.newArrayListWithExpectedSize(this.acceptedJobsCount());
            acceptedJobsList.addAll(this.acceptedJobsMap.values());
            return Collections.unmodifiableList(acceptedJobsList);
        }

        /**
         * List of Jobs in active state
         * @return list of active job info
         */

        List<JobInfo> getActiveJobsList() {
            List<JobInfo> activeJobList = Lists.newArrayListWithExpectedSize(activeJobsMap.size());
            activeJobList.addAll(this.activeJobsMap.values());
            return Collections.unmodifiableList(activeJobList);
        }

        /**
         * List of jobs in completed state
         * @return list of completed jobs
         */
        List<CompletedJob> getCompletedJobsList(int limit, @Nullable JobId from) {
            try {
                if (from != null) {
                    return completedJobStore.getCompletedJobs(limit, from);
                } else {
                    return completedJobStore.getCompletedJobs(limit);
                }
            } catch (IOException e) {
                return Collections.emptyList();
            }
        }

        List<JobInfo> getTerminatingJobsList() {
            List<JobInfo> terminatingJobsList = Lists.newArrayListWithExpectedSize(terminatingJobsMap.size());
            terminatingJobsList.addAll(this.terminatingJobsMap.values());
            return Collections.unmodifiableList(terminatingJobsList);
        }

        /**
         * No. of jobs in accepted state
         * @return no of accepted jobs
         */

        int acceptedJobsCount() {

            return  this.acceptedJobsMap.size();
        }

        /**
         * No. of jobs in running state
         * @return no of active jobs
         */
        int activeJobsCount() {

            return this.activeJobsMap.size();
        }

        Optional<CompletedJob> getCompletedJob(JobId jId) {
            try {
                return completedJobStore.getCompletedJob(jId);
            } catch (IOException e) {
                logger.warn("Failed to get completed job {}", jId, e);
            }
            return Optional.empty();
        }

        Optional<IMantisJobMetadata> getJobDataForCompletedJob(String jId) {
            Optional<JobId> jobId = JobId.fromId(jId);
            if(jobId.isPresent()) {
                try {
                    return completedJobStore.getJobMetadata(jobId.get());
                } catch (IOException e) {
                    logger.warn("Failed to get completed job {}", jId, e);
                    return empty();
                }
            } else {

                logger.warn("Invalid Job Id {} in getJobDataForCompletedJob", jId);
                return empty();
            }
        }

        /**
         * Returns the JobInfo associated with the JobId. The Job could be in Accepted, Launched or Terminating states
         * But not terminated state.
         * @param jId JobId whose JobInfo is being lookedup
         * @return JobInfo corresponding to the jobId, empty if not found
         */


        Optional<JobInfo> getJobInfoForNonTerminalJob(JobId jId) {
            if(logger.isTraceEnabled() ) { logger.trace("In getJobInfo {}", jId); }
            if(acceptedJobsMap.containsKey(jId)) {
                if(logger.isDebugEnabled() ) { logger.debug("Found {} in accepted state", jId); }
                return of(acceptedJobsMap.get(jId));
            } else if(activeJobsMap.containsKey(jId)) {
                if(logger.isDebugEnabled() ) { logger.debug("Found {} in active state", jId); }
                return of(activeJobsMap.get(jId));
            } else if(this.terminatingJobsMap.containsKey(jId)) {
                if(logger.isDebugEnabled() ) { logger.debug("Found {} in terminating state", jId); }
                return of(terminatingJobsMap.get(jId));
            }
            return empty();
        }

        Optional<JobInfo> getJobInfoForNonTerminalJob(String jobId) {
            Optional<JobId> jId = JobId.fromId(jobId);
            if(jId.isPresent()) {
                return getJobInfoForNonTerminalJob(jId.get());
            }
            return empty();
        }

        Optional<JobInfo> getJobInfoByUniqueId(final String uniqueId) {
            return this.getAllNonTerminalJobsList().stream().filter((jobInfo) -> {
                String unq = jobInfo.jobDefinition.getJobSla().getUserProvidedType();
                return unq != null && !unq.isEmpty() && unq.equals(uniqueId);
            }).findFirst();

        }

        private List<JobInfo> getJobActorsStuckInInit(long now, long allowedDelay) {
            return getPendingInitializationJobsPriorToCutoff(now - allowedDelay)
                    .stream()
                    .peek((jobInfo) -> logger.warn("Job {} waiting for initialization since {}", jobInfo.jobId, jobInfo.initializeInitiatedAt))
                    .collect(Collectors.toList());
        }

        private List<JobInfo> getJobsStuckInAccepted(long now, long allowedDelay) {
            return getAcceptedJobsList().stream()
                    .filter((jobInfo -> jobInfo.submittedAt < now - allowedDelay))
                    .peek((jobInfo) -> logger.warn("Job {} stuck in accepted since {}", jobInfo.jobId, Instant.ofEpochMilli(jobInfo.submittedAt)))
                    .collect(Collectors.toList());
        }

        private List<JobInfo> getJobsStuckInTerminating(long now, long allowedDelay) {
            return getTerminatingJobsList().stream()
                    .filter((jobInfo -> jobInfo.terminationInitiatedAt < now - allowedDelay))
                    .peek((jobInfo) -> logger.warn("Job {} stuck in terminating since {}", jobInfo.jobId, Instant.ofEpochMilli(jobInfo.terminationInitiatedAt)))
                    .collect(Collectors.toList());
        }



        boolean isJobListEmpty() {
            return activeJobsMap.isEmpty() && acceptedJobsMap.isEmpty();
        }


        public Set<JobId> getJobsMatchingLabels(List<Label> labels, Optional<String> labelsOp) {
            boolean isAnd = false;
            if(labelsOp.isPresent()) {
                if(labelsOp.get().equalsIgnoreCase(LabelUtils.AND_OPERAND)) {
                    isAnd = true;
                }

            }
            return labelCache.getJobIdsMatchingLabels(labels, isAnd);


        }
    }

    /**
     * Maintains a map of label to JobbId. Note the map is Label to Job Id and not
     * Label.key to JobId.
     *
     */

    final static class LabelCache {
        final Map<Label, Set<JobId>> labelJobIdMap = new HashMap<>();
        final Map<JobId, List<Label>> jobIdToLabelMap = new HashMap<>();
        private final Logger logger = LoggerFactory.getLogger(LabelCache.class);

        /**
         * Invoked in the following ways
         * 1. During bootstrap of Job cluster when a Job Actor is created for an existing running job
         * 2. When a new Job Actor is created during job submission
         * 3. When the completed jobs list is being populated at bootstrap
         * @param jobId
         * @param labelList
         */
        void addJobIdToLabelCache(JobId jobId,List<Label> labelList) {
            if(logger.isTraceEnabled()) { logger.trace("addJobIdToLabelCache " + jobId + " labelList " + labelList + " current map " + labelJobIdMap); }
            if(labelList == null) {
                return;
            }
            for(Label label : labelList) {
                Set<JobId> jobIds = labelJobIdMap.get(label);
                if(jobIds != null) {
                    jobIds.add(jobId);
                } else {
                    Set<JobId> jobIdList = new HashSet<>();
                    jobIdList.add(jobId);
                    labelJobIdMap.put(label, jobIdList);
                }
            }
            jobIdToLabelMap.put(jobId, labelList);
            if(logger.isTraceEnabled()) { logger.trace("Exit addJobIdToLabelCache " + jobId + " labelList " + labelList + " new map " + labelJobIdMap); }
        }

        /**
         * Invoked when a job is completely purged from the system.
         * This happens after a completed job hits its expiry time.
         * @param jobId
         */

        void removeJobIdFromLabelCache(JobId jobId) {
            if(logger.isTraceEnabled()) { logger.trace("removeJobIdFromLabelCache " + jobId +  " current map " + labelJobIdMap);}
            List<Label> labels = jobIdToLabelMap.get(jobId);
            if(labels != null) {
                for(Label label : labels) {
                    Set<JobId> jobIds = labelJobIdMap.get(label);
                    jobIds.remove(jobId);
                    if(jobIds.isEmpty()) {
                        labelJobIdMap.remove(label);
                    }
                }
            }
            jobIdToLabelMap.remove(jobId);
            if(logger.isTraceEnabled()) { logger.trace("Exit removeJobIdFromLabelCache " + jobId +  " current map " + labelJobIdMap); }
        }

        /**
         * Invoked during jobList and jobIdList api calls.
         * 1. For each label find the Set of JobIds that have this label
         * 2. Then based on whether the query is an AND or OR perform a set
         * intersection or union and return the result.
         * @param labelList
         * @param isAnd
         * @return
         */
        Set<JobId> getJobIdsMatchingLabels(List<Label> labelList, boolean isAnd) {
            if(logger.isTraceEnabled()) { logger.trace("Entering getJobidsMatchingLabels " + labelList + " is and ? " + isAnd + " with map " + labelJobIdMap); }
            Set<JobId> matchingJobIds = new HashSet<>();
            List<Set<JobId>> matchingSubsets = new ArrayList<>();
            if(labelList == null) {
                return matchingJobIds;
            }
            for(Label label : labelList) {

                if(labelJobIdMap.containsKey(label)) {
                    Set<JobId> st = new HashSet<>();
                    st.addAll(labelJobIdMap.get(label));
                    matchingSubsets.add(st);
                } else {
                    // label not present add empty set
                    matchingSubsets.add(new HashSet<>());
                }


            }
            Set<JobId> resu = (isAnd) ? getSetIntersection(matchingSubsets) : getSetUnion(matchingSubsets);
            if(logger.isTraceEnabled()) { logger.trace("Exiting getJobidsMatchingLabels " + resu); }
            return resu;


        }

        /**
         * Uses the built in feature of Set API to perform a union of 'n' sets
         * @param listOfSets
         * @return
         */
        private Set<JobId> getSetUnion(List<Set<JobId>> listOfSets) {
            if(logger.isTraceEnabled()) { logger.trace("In getSetUnion " + listOfSets); }
            Set<JobId> unionSet = new HashSet<>();
            if(listOfSets == null || listOfSets.isEmpty()) return unionSet;
            int i=0;
            unionSet = listOfSets.get(i);
            i++;
            while(i < listOfSets.size()) {
                Set<JobId> jobIds = listOfSets.get(i);
                unionSet.addAll(jobIds);
                i++;

            }
            if(logger.isTraceEnabled()) { logger.trace("Exit  getSetUnion " + unionSet); }
            return unionSet;
        }

        /**
         * Uses the built in retainAll method to perform an intersection across
         * 'n' sets.
         * @param listOfSets
         * @return
         */
        private Set<JobId> getSetIntersection(List<Set<JobId>> listOfSets) {
            if(logger.isTraceEnabled()) { logger.trace("In getSetIntersection " + listOfSets); }
            Set<JobId> intersectionSet = new HashSet<>();
            if(listOfSets == null || listOfSets.isEmpty()) return intersectionSet;
            int i=0;
            intersectionSet = listOfSets.get(i);
            i++;
            while(i < listOfSets.size()) {
                Set<JobId> jobIds = listOfSets.get(i);
                intersectionSet.retainAll(jobIds);
                i++;

            }
            if(logger.isTraceEnabled()) { logger.trace("Return getSetIntersection " + intersectionSet); }
            return intersectionSet;
        }
    }

    static class CronManager {
        private static final TriggerOperator triggerOperator;
        private static final Logger logger = LoggerFactory.getLogger(CronManager.class);

        static {
            triggerOperator = new TriggerOperator(1);
            try {
                triggerOperator.initialize();
            } catch (SchedulerException e) {
                logger.error("Unexpected: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        private final String cronSpec;
        private final CronPolicy policy;
        private final ActorRef clusterActor;
        private String triggerId;
        private final String jobClusterName;
        private String triggerGroup = null;
        private CronTrigger<ActorRef> scheduledTrigger;
        private boolean isCronActive = false;
        CronManager(String jobClusterName, ActorRef clusterActor, SLA sla) throws Exception {
            this.jobClusterName = jobClusterName;
            cronSpec = sla.getCronSpec();
            policy = sla.getCronPolicy();
            this.clusterActor = clusterActor;
            if(cronSpec != null) {
                initCron();
            }
        }

        private void initCron() throws Exception{
            if(cronSpec == null || triggerId != null) {
                return;
            }
            logger.info("Init'ing cron for " + jobClusterName);
            triggerGroup = jobClusterName + "-"  + this;
            try {
                scheduledTrigger = new CronTrigger<>(cronSpec, jobClusterName, clusterActor, ActorRef.class, CronTriggerAction.class);
                triggerId = triggerOperator.registerTrigger(triggerGroup, scheduledTrigger);
                isCronActive = true;
            } catch (IllegalArgumentException e) {
                destroyCron();
                logger.error("Failed to start cron for {}: {}. The format of the cron schedule may be incorrect.", jobClusterName, e.getStackTrace());
                throw new SchedulerException(e.getMessage(), e);
            }

        }

        private void destroyCron() {
            try {
                if (triggerId != null) {
                    logger.info("Destroying cron " + triggerId);
                    isCronActive = false;
                    triggerOperator.deleteTrigger(triggerGroup, triggerId);
                    triggerId = null;
                }
            } catch (TriggerNotFoundException | SchedulerException e) {
                logger.warn("Couldn't delete trigger group " + triggerGroup + ", id " + triggerId);
            }
        }

        boolean isCronActive() {
            return isCronActive;
        }
    }

    public static class CronTriggerAction implements Action1<ActorRef> {

        @Override
        public void call(ActorRef jobClusterActor) {

           jobClusterActor.tell(new JobClusterProto.TriggerCronRequest(), ActorRef.noSender());

        }

    }


}
