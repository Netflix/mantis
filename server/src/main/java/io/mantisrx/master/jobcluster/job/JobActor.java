
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

package io.mantisrx.master.jobcluster.job;

import static io.mantisrx.master.StringConstants.MANTIS_MASTER_USER;
import static io.mantisrx.master.events.LifecycleEventsProto.StatusEvent.StatusEventType.ERROR;
import static io.mantisrx.master.events.LifecycleEventsProto.StatusEvent.StatusEventType.INFO;
import static io.mantisrx.master.events.LifecycleEventsProto.StatusEvent.StatusEventType.WARN;
import static io.mantisrx.master.jobcluster.job.worker.MantisWorkerMetadataImpl.MANTIS_SYSTEM_ALLOCATED_NUM_PORTS;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.akka.MantisActorSupervisorStrategy;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.WorkerInfoListHolder;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.JobWorker;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.master.jobcluster.job.worker.WorkerTerminate;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobDetailsResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetJobSchedInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.GetLatestJobDiscoveryInfoResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.KillJobResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ListWorkersResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ResubmitWorkerResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageRequest;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.ScaleStageResponse;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobProto;
import io.mantisrx.master.jobcluster.proto.JobProto.InitJob;
import io.mantisrx.master.jobcluster.proto.JobProto.JobInitialized;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.MigrationStrategy;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.ConstraintsEvaluators;
import io.mantisrx.server.master.InvalidJobRequest;
import io.mantisrx.server.master.agentdeploy.MigrationStrategyFactory;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidJobException;
import io.mantisrx.server.master.persistence.exceptions.InvalidWorkerStateChangeException;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.server.master.scheduler.WorkerUnscheduleable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;


/**
 * Actor responsible for handling all operations for a given JobID.
 * private static final String API_JOB_SUBMIT_PATH="/api/submit";
 * private static final String API_JOB_KILL="/api/jobs/kill";
 * private static final String API_JOB_STAGE_SCALE="/api/jobs/scaleStage";
 * private static final String API_JOB_RESUBMIT_WORKER="/api/jobs/resubmitWorker";
 *
 * @author njoshi
 */
public class JobActor extends AbstractActorWithTimers implements IMantisJobManager {

    private static final String CHECK_HB_TIMER_KEY = "CHECK_HB";
    private static final String REFRESH_SEND_STAGE_ASSIGNEMNTS_KEY = "REFRESH_SEND_STAGE_ASSIGNMENTS";
    private static final Logger LOGGER = LoggerFactory.getLogger(JobActor.class);
    private static final double DEFAULT_JOB_MASTER_CORES = 1;
    private static final double DEFAULT_JOB_MASTER_MEM = 1024;
    private static final double DEFAULT_JOB_MASTER_NW = 128;
    private static final double DEFAULT_JOB_MASTER_DISK = 1024;
    private final Metrics metrics;
    private final MetricGroupId metricsGroupId;

    private final Counter numWorkerResubmissions;
    private final Counter numWorkerResubmitLimitReached;
    private final Counter numWorkerTerminated;
    private final Counter numScaleStage;
    private final Counter numWorkersCompletedNotTerminal;
    private final Counter numSchedulingChangesRefreshed;
    private final Counter numMissingWorkerPorts;

    /**
     * Behavior after being initialized.
     */
    private Receive initializedBehavior;

    /**
     * Behavior once active.
     */
    private Receive activeBehavior;

    /**
     * Behavior during termination.
     */
    private Receive terminatingBehavior;

    /**
     * Behavior after termination waiting for JCA to terminate actor.
     */
    private Receive terminatedBehavior;

    private final String clusterName;
    private final JobId jobId;
    private final IJobClusterDefinition jobClusterDefinition;
    private volatile MantisJobMetadataImpl mantisJobMetaData;

    private final MantisJobStore jobStore;
    // load from config
    private int workerWritesBatchSize = 10;

    // Manages life cycle of worker
    private IWorkerManager workerManager = null;

    // Used to schedule and unschedule workers
    private final MantisScheduler mantisScheduler;
    private final LifecycleEventPublisher eventPublisher;
    private boolean hasJobMaster;
    private volatile boolean allWorkersCompleted = false;

    /**
     * Used by the JobCluster Actor to create this Job Actor.
     *
     * @param jobClusterDefinition The job cluster definition to be used while creating this job.
     * @param jobMetadata          The job metadata provided by the user.
     * @param jobStore             Reference to the persistence store {@link MantisJobStore}.
     * @param mantisScheduler      Reference to the {@link MantisScheduler} to be used to schedule work
     * @param eventPublisher       Reference to the event publisher {@link LifecycleEventPublisher} where
     *                             lifecycle events
     *                             are to be published.
     *
     * @return
     */
    public static Props props(final IJobClusterDefinition jobClusterDefinition,
                              final MantisJobMetadataImpl jobMetadata,
                              final MantisJobStore jobStore,
                              final MantisScheduler mantisScheduler,
                              final LifecycleEventPublisher eventPublisher) {
        return Props.create(JobActor.class, jobClusterDefinition, jobMetadata, jobStore,
                mantisScheduler, eventPublisher);
    }

    /**
     * This is invoked indirectly via props method to create an instance of this class.
     *
     * @param jobClusterDefinition
     * @param jobMetadata
     * @param jobStore
     * @param scheduler
     * @param eventPublisher
     */
    public JobActor(final IJobClusterDefinition jobClusterDefinition, final MantisJobMetadataImpl jobMetadata,
                    MantisJobStore jobStore, final MantisScheduler scheduler,
                    final LifecycleEventPublisher eventPublisher) {

        this.clusterName = jobMetadata.getClusterName();
        this.jobId = jobMetadata.getJobId();
        this.jobStore = jobStore;
        this.jobClusterDefinition = jobClusterDefinition;
        this.mantisScheduler = scheduler;
        this.eventPublisher = eventPublisher;
        this.mantisJobMetaData = jobMetadata;

        initializedBehavior = getInitializedBehavior();

        activeBehavior = getActiveBehavior();

        terminatingBehavior = getTerminatingBehavior();

        terminatedBehavior = getTerminatedBehavior();

        this.metricsGroupId = getMetricGroupId(jobId.getId());
        Metrics m = new Metrics.Builder()
                .id(metricsGroupId)
                .addCounter("numWorkerResubmissions")
                .addCounter("numWorkerResubmitLimitReached")
                .addCounter("numWorkerTerminated")
                .addCounter("numScaleStage")
                .addCounter("numWorkersCompletedNotTerminal")
                .addCounter("numSchedulingChangesRefreshed")
                .addCounter("numMissingWorkerPorts")
                .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.numWorkerResubmissions = metrics.getCounter("numWorkerResubmissions");
        this.numWorkerResubmitLimitReached = metrics.getCounter("numWorkerResubmitLimitReached");
        this.numWorkerTerminated = metrics.getCounter("numWorkerTerminated");
        this.numScaleStage = metrics.getCounter("numScaleStage");
        this.numWorkersCompletedNotTerminal = metrics.getCounter("numWorkersCompletedNotTerminal");
        this.numSchedulingChangesRefreshed = metrics.getCounter("numSchedulingChangesRefreshed");
        this.numMissingWorkerPorts = metrics.getCounter("numMissingWorkerPorts");
    }

    /**
     * Create a MetricGroupId using the given job Id.
     *
     * @param id
     *
     * @return
     */
    MetricGroupId getMetricGroupId(String id) {
        return new MetricGroupId("JobActor", new BasicTag("jobId", id));
    }

    /**
     * Validates the job definition, stores the job to persistence.
     * Instantiates the SubscriptionManager to keep track of subscription and runtime timeouts
     * Instantiates the WorkerManager which manages the worker life cycle
     *
     * @throws InvalidJobRequest
     * @throws InvalidJobException
     */
    void initialize(boolean isSubmit) throws Exception {
        LOGGER.info("Initializing Job {}", jobId);

        if (isSubmit) {
            eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                    "Job request received", getJobId(), getJobState()));

            // Ignore isReady flag, if the job is autoscaled it gets a Job Master
            // this.jobClusterDefinition.getIsReadyForJobMaster() &&
            if (isAutoscaled(mantisJobMetaData.getSchedulingInfo())) {
                LOGGER.info("Job is autoscaled, setting up Job Master");
                setupJobMasterStage(mantisJobMetaData.getSchedulingInfo());
            }
            LOGGER.info("Storing job");
            jobStore.storeNewJob(mantisJobMetaData);
        }
        LOGGER.info("Stored mantis job");

        this.workerManager = new WorkerManager(this, jobClusterDefinition.getWorkerMigrationConfig(),
                this.mantisScheduler, isSubmit);

        long checkAgainInSeconds = ConfigurationProvider.getConfig().getWorkerTimeoutSecs();
        long refreshStageAssignementsDurationMs = ConfigurationProvider.getConfig()
                .getStageAssignmentRefreshIntervalMs();
        getTimers().startPeriodicTimer(CHECK_HB_TIMER_KEY, new JobProto.CheckHeartBeat(),
                Duration.ofSeconds(checkAgainInSeconds));
        // -1 indicates disabled, which means all updates will be sent immediately
        if (refreshStageAssignementsDurationMs > 0) {
            getTimers().startPeriodicTimer(REFRESH_SEND_STAGE_ASSIGNEMNTS_KEY,
                    new JobProto.SendWorkerAssignementsIfChanged(),
                    Duration.ofMillis(refreshStageAssignementsDurationMs));
        }
        mantisJobMetaData.getJobDefinition().getJobSla().getRuntimeLimitSecs();
        LOGGER.info("Job {} initialized", this.jobId);
    }

    private void setupJobMasterStage(SchedulingInfo schedulingInfo)
            throws io.mantisrx.runtime.command.InvalidJobException {
        LOGGER.info("Job {} is autoscaled setting up Job Master", this.jobId);
        if (schedulingInfo.forStage(0) == null) {
            // create stage 0 schedulingInfo only if not already provided
            final StageSchedulingInfo stageSchedulingInfo =
                    new StageSchedulingInfo(1, getJobMasterMachineDef(),
                            null, null, // for now, there are no hard or soft constraints
                            null, false); // jobMaster stage itself is not scaled
            schedulingInfo.addJobMasterStage(stageSchedulingInfo);

            // Update jobMetadata with the new stage added
            mantisJobMetaData = new MantisJobMetadataImpl.Builder()
                    .from(mantisJobMetaData)
                    .withJobDefinition(new JobDefinition.Builder()
                            .from(mantisJobMetaData.getJobDefinition())
                            .withSchedulingInfo(schedulingInfo)
                            .withNumberOfStages(schedulingInfo.getStages().size())
                            .build())
                    .build();


        }
        hasJobMaster = true;
    }


    private MachineDefinition getJobMasterMachineDef() {
        MasterConfiguration config = ConfigurationProvider.getConfig();

        if (config != null) {
            return new MachineDefinition(
                    config.getJobMasterCores(), config.getJobMasterMemoryMB(), config.getJobMasterNetworkMbps(),
                    config.getJobMasterDiskMB(), 1
            );
        } else {
            return new MachineDefinition(
                    DEFAULT_JOB_MASTER_CORES, DEFAULT_JOB_MASTER_MEM, DEFAULT_JOB_MASTER_NW,
                    DEFAULT_JOB_MASTER_DISK, 1);
        }
    }

    @Override
    public void preStart() throws Exception {
        LOGGER.info("Job Actor {}-{} started", clusterName, jobId);

    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("Job Actor {} stopped invoking cleanup logic", jobId);
        if (jobId != null) {
            MetricsRegistry.getInstance().remove(getMetricGroupId(jobId.getId()));
        }
        //shutdown();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        // custom supervisor strategy to resume the child actors on Exception instead of the default restart
        return MantisActorSupervisorStrategy.getInstance().create();
    }

    @Override
    public Receive createReceive() {
        return getInitializingBehavior();
    }

    private String genUnexpectedMsg(String event, String cluster, String state) {
        return String.format("Unexpected message %s received by Job actor %s in %s State", event, cluster, state);
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /*
        Job Actor behaviors 12 total
     * - Init
     * - GET
     * - LIST workers
     * - GET SCHED INFO
     * - SCALE
     * - KILL
     * - RESUBMIT WORKER
     * - WorkerEvent
     *
     * // SELF SENT
     * - HB enforcement
     * - Runtime enforcement
     * - Self Destruct
     * - Refresh Stage Assignments
    */
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * A Terminating Job allows.
     * - GET
     * - LIST workers
     * - WorkerEvent
     *
     * @return
     */
    private Receive getTerminatingBehavior() {
        String state = "terminating";
        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN//
                // get Job Details
                .match(GetJobDetailsRequest.class, this::onGetJobDetails)

                // list active workers request
                .match(ListWorkersRequest.class, this::onListActiveWorkers)

                // EXPECTED MESSAGES END//

                // UNEXPECTED MESSAGES BEGIN //

                // Worker related events
                .match(WorkerEvent.class, (x) -> LOGGER.warn("Job {} is Terminating, ignoring worker Events {}",
                        this.jobId.getId(), x))

                .match(InitJob.class, (x) -> getSender().tell(new JobInitialized(x.requestId, SUCCESS,
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state), this.jobId, x.requstor), getSelf()))
                // explicit resubmit worker
                .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(new ResubmitWorkerResponse(x.requestId,
                        CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.jobId.getId(), state)), getSelf()))
                // Heart beat accounting timers
                .match(JobProto.CheckHeartBeat.class, (x) -> LOGGER.warn(genUnexpectedMsg(x.toString(),
                        this.jobId.getId(), state)))
                // runtime limit reached
                .match(JobProto.RuntimeLimitReached.class, (x) -> LOGGER.warn(genUnexpectedMsg(x.toString(),
                        this.jobId.getId(), state)))
                // Kill job request
                .match(JobClusterProto.KillJobRequest.class, (x) -> getSender().tell(new KillJobResponse(x.requestId,
                        SUCCESS, JobState.Noop, genUnexpectedMsg(x.toString(), this.jobId.getId(), state),
                        this.jobId, x.user), getSelf()))
                // scale stage request
                .match(ScaleStageRequest.class, (x) -> getSender().tell(new ScaleStageResponse(x.requestId,
                        CLIENT_ERROR, genUnexpectedMsg(x.toString(), this.jobId.getId(), state),
                        0), getSelf()))
                // scheduling Info observable
                .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(
                        new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR,
                                genUnexpectedMsg(x.toString(), this.jobId.getId(), state), empty()), getSelf()))
                .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(
                    new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR,
                                                genUnexpectedMsg(x.toString(), this.jobId.getId(), state), empty()), getSelf()))

                .match(JobProto.SendWorkerAssignementsIfChanged.class,
                        (x) -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                .match(KillJobResponse.class, (x) -> LOGGER.info("Received Kill Job Response in"
                        + "Terminating State Ignoring"))

                .matchAny(x -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))

                // UNEXPECTED MESSAGES END

                .build();

    }

    /**
     * A Terminated Job allows.
     * - GET
     * - LIST workers
     *
     * @return
     */
    private Receive getTerminatedBehavior() {
        String state = "terminated";
        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN//
                // get Job Details
                .match(GetJobDetailsRequest.class, this::onGetJobDetails)

                // list active workers request
                .match(ListWorkersRequest.class, this::onListActiveWorkers)

                // EXPECTED MESSAGES END//

                // UNEXPECTED MESSAGES BEGIN //

                .match(InitJob.class, (x) -> getSender().tell(
                        new JobInitialized(x.requestId, SUCCESS, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), this.jobId, x.requstor), getSelf()))
                // explicit resubmit worker
                .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(
                        new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR,
                                genUnexpectedMsg(x.toString(), this.jobId.getId(), state)), getSelf()))
                // Heart beat accounting timers
                .match(JobProto.CheckHeartBeat.class, (x) -> LOGGER.warn(
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // Migrate worker request
                .match(JobProto.MigrateDisabledVmWorkersRequest.class, (x) -> LOGGER.warn(
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // runtime limit reached
                .match(JobProto.RuntimeLimitReached.class, (x) -> LOGGER.warn(
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // Kill job request
                .match(JobClusterProto.KillJobRequest.class, (x) -> getSender().tell(
                        new KillJobResponse(x.requestId, SUCCESS, JobState.Noop,
                                genUnexpectedMsg(x.toString(), this.jobId.getId(), state), this.jobId, x.user),
                        getSelf()))
                // scale stage request
                .match(ScaleStageRequest.class, (x) -> getSender().tell(
                        new ScaleStageResponse(x.requestId, CLIENT_ERROR,
                                genUnexpectedMsg(x.toString(), this.jobId.getId(), state), 0),
                        getSelf()))
                // scheduling Info observable
                .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(
                        new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), empty()), getSelf()))
                .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(
                    new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state), empty()), getSelf()))

                .match(KillJobResponse.class, (x) -> LOGGER.info("Received Kill Job Response in"
                        + "Terminating State Ignoring"))

                .match(JobProto.SendWorkerAssignementsIfChanged.class, (x) -> LOGGER.warn(genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state)))

                // Worker related events
                .match(WorkerEvent.class, (x) -> LOGGER.info("Received worker event  in Terminated State Ignoring"))

                .matchAny(x -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))

                // UNEXPECTED MESSAGES END

                .build();

    }

    /**
     * An active job allows.
     * - GET
     * - LIST workers
     * - GET SCHED INFO
     * - SCALE
     * - KILL
     * - RESUBMIT WORKER
     * - WorkerEvent
     * - HB enforcement
     * - Runtime enforcement
     * - Refresh Stage Assignments
     *
     * @return
     */
    private Receive getActiveBehavior() {
        String state = "active";
        // get Job Details
        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN//
                .match(GetJobDetailsRequest.class, this::onGetJobDetails)

                // Worker related events
                .match(WorkerEvent.class, r -> processWorkerEvent(r))
                // explicit resubmit worker
                .match(ResubmitWorkerRequest.class, this::onResubmitWorker)
                // Heart beat accounting timers
                .match(JobProto.CheckHeartBeat.class, this::onCheckHeartBeats)
                // Migrate workers from disabled VMs
                .match(JobProto.MigrateDisabledVmWorkersRequest.class, this::onMigrateWorkers)
                // runtime limit reached
                .match(JobProto.RuntimeLimitReached.class, this::onRuntimeLimitReached)
                // Kill job request
                .match(JobClusterProto.KillJobRequest.class, this::onJobKill)
                // scale stage request
                .match(ScaleStageRequest.class, this::onScaleStage)
                // list active workers request
                .match(ListWorkersRequest.class, this::onListActiveWorkers)
                // scheduling Info observable
                .match(GetJobSchedInfoRequest.class, this::onGetJobStatusSubject)
                .match(GetLatestJobDiscoveryInfoRequest.class, this::onGetLatestJobDiscoveryInfo)

                .match(JobProto.SendWorkerAssignementsIfChanged.class, this::onSendWorkerAssignments)

                // EXPECTED MESSAGES END//
                // UNEXPECTED MESSAGES BEGIN //
                .match(InitJob.class, (x) -> getSender().tell(new JobInitialized(x.requestId, SUCCESS,
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state), this.jobId, x.requstor), getSelf()))

                .matchAny(x -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // UNEXPECTED MESSAGES END //

                .build();


    }

    /**
     * INITIALIZED JOB allows.
     * - GET
     * - LIST workers
     * - GET SCHED INFO
     * - KILL
     * - WorkerEvent
     * - HB enforcement
     * - REFRESH STAGE scheduling info
     *
     * @return
     */
    private Receive getInitializedBehavior() {
        String state = "initialized";
        return receiveBuilder()
                // EXPECTED MESSAGES BEGIN//
                // get Job Details
                .match(GetJobDetailsRequest.class, this::onGetJobDetails)
                // Worker related events
                .match(WorkerEvent.class, r -> processWorkerEvent(r))
                // Heart beat accounting timers
                .match(JobProto.CheckHeartBeat.class, this::onCheckHeartBeats)
                // Migrate workers from disabled VMs
                .match(JobProto.MigrateDisabledVmWorkersRequest.class, this::onMigrateWorkers)
                // Kill job request
                .match(JobClusterProto.KillJobRequest.class, this::onJobKill)
                // list active workers request
                .match(ListWorkersRequest.class, this::onListActiveWorkers)

                .match(GetJobSchedInfoRequest.class, this::onGetJobStatusSubject)
                .match(GetLatestJobDiscoveryInfoRequest.class, this::onGetLatestJobDiscoveryInfo)

                .match(JobProto.SendWorkerAssignementsIfChanged.class, this::onSendWorkerAssignments)

                // EXPECTED MESSAGES END//

                // UNEXPECTED MESSAGES BEGIN //
                // explicit resubmit worker
                .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(
                        new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state)), getSelf()))
                // runtime limit reached
                .match(JobProto.RuntimeLimitReached.class, (x) -> LOGGER.warn(genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state)))
                // scale stage request
                .match(ScaleStageRequest.class, (x) -> getSender().tell(
                        new ScaleStageResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), 0), getSelf()))

                .match(InitJob.class, (x) -> getSender().tell(new JobInitialized(x.requestId, SUCCESS,
                        genUnexpectedMsg(x.toString(), this.jobId.getId(), state), this.jobId, x.requstor), getSelf()))
                .matchAny(x -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // UNEXPECTED MESSAGES END //
                .build();
    }

    /**
     * AN INITIALIZING JOB ALLOWS.
     * - Init Job
     *
     * @return
     */
    private Receive getInitializingBehavior() {
        String state = "initializing";
        return receiveBuilder()
                // EXPECTED MESSAGES BEING//

                .match(InitJob.class, this::onJobInitialize)

                // EXPECTED MESSAGES END//

                //UNEXPECTED MESSAGES BEGIN //

                // get Job Details
                .match(GetJobDetailsRequest.class, (x) -> getSender().tell(
                        new GetJobDetailsResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), empty()), getSelf()))

                // Worker related events
                .match(WorkerEvent.class, (x) -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                // explicit resubmit worker
                .match(ResubmitWorkerRequest.class, (x) -> getSender().tell(
                        new ResubmitWorkerResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state)), getSelf()))
                // Heart beat accounting timers
                .match(JobProto.CheckHeartBeat.class, (x) -> LOGGER.warn(genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state)))
                // Migrate workers request
                .match(JobProto.MigrateDisabledVmWorkersRequest.class, (x) -> LOGGER.warn(genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state)))
                // runtime limit reached
                .match(JobProto.RuntimeLimitReached.class, (x) -> LOGGER.warn(genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state)))
                // Kill job request
                .match(JobClusterProto.KillJobRequest.class, (x) -> getSender().tell(
                        new KillJobResponse(x.requestId, CLIENT_ERROR, JobState.Noop, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), this.jobId, x.user), getSelf()))
                // scale stage request
                .match(ScaleStageRequest.class, (x) -> getSender().tell(
                        new ScaleStageResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), 0), getSelf()))
                // list active workers request
                .match(ListWorkersRequest.class, (x) -> getSender().tell(
                        new ListWorkersResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), Lists.newArrayList()), getSelf()))
                // scheduling Info observable
                .match(GetJobSchedInfoRequest.class, (x) -> getSender().tell(
                        new GetJobSchedInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                                x.toString(), this.jobId.getId(), state), empty()), getSelf()))
                // latest scheduling Info
                .match(GetLatestJobDiscoveryInfoRequest.class, (x) -> getSender().tell(
                    new GetLatestJobDiscoveryInfoResponse(x.requestId, CLIENT_ERROR, genUnexpectedMsg(
                        x.toString(), this.jobId.getId(), state), empty()), getSelf()))

                //UNEXPECTED MESSAGES END //
                .matchAny(x -> LOGGER.warn(genUnexpectedMsg(x.toString(), this.jobId.getId(), state)))
                .build();
    }


    //////////////////////////////////////////// Akka Messages sent to the Job Actor Begin/////////////////////

    @Override
    public void onJobInitialize(InitJob i) {
        ActorRef sender = getSender();
        try {

            initialize(i.isSubmit);
            if (JobState.isRunningState(mantisJobMetaData.getState())) {
                getContext().become(activeBehavior);

                setRuntimeLimitTimersIfRequired(Instant.now());
            } else {
                getContext().become(initializedBehavior);
            }
            sender.tell(new JobInitialized(i.requestId, SUCCESS, String.format(
                    "Job %s initialized successfully", jobId), jobId, i.requstor), getSelf());
        } catch (Exception e) {
            LOGGER.error("Exception initializing job ", e);
            sender.tell(new JobInitialized(i.requestId, SERVER_ERROR, "" + e.getMessage(), jobId, i.requstor),
                    getSelf());
        }
    }

    /**
     * Return information related to this job.
     *
     * @param r
     */
    @Override
    public void onGetJobDetails(GetJobDetailsRequest r) {
        ActorRef sender = getSender();

        sender.tell(new GetJobDetailsResponse(r.requestId, SUCCESS, "", of(getJobDetails())), getSelf());

    }

    /**
     * Return a BehaviorSubject that streams worker lifecycle events to the user.
     *
     * @param r
     */

    @Override
    public void onGetJobStatusSubject(GetJobSchedInfoRequest r) {
        LOGGER.trace("Entering onGetJobStatusSubject {}", r);
        ActorRef sender = getSender();
        if (r.getJobId().equals(this.jobId)) {
            sender.tell(new GetJobSchedInfoResponse(r.requestId, SUCCESS, "",
                    of(workerManager.getJobStatusSubject())), getSelf());
        } else {
            String msg = "JobId in the request " + r.getJobId() + " does not match Job Actors job Id " + this.jobId;
            LOGGER.warn(msg);
            sender.tell(new GetJobSchedInfoResponse(r.requestId, CLIENT_ERROR, msg, empty()), getSelf());
        }
    }

    @Override
    public void onGetLatestJobDiscoveryInfo(GetLatestJobDiscoveryInfoRequest r) {
        LOGGER.trace("Entering onGetLatestJobDiscoveryInfo {}", r);
        ActorRef sender = getSender();
        if (r.getJobCluster().equals(this.jobId.getCluster())) {
            JobSchedulingInfo schedulingInfo = workerManager.getJobStatusSubject().getValue();
            if (schedulingInfo != null) {
                sender.tell(new GetLatestJobDiscoveryInfoResponse(r.requestId, SUCCESS, "",
                                                                  ofNullable(schedulingInfo)), getSelf());
            } else {
                LOGGER.info("discoveryInfo from BehaviorSubject is null {}", jobId);
                sender.tell(new GetLatestJobDiscoveryInfoResponse(r.requestId, SERVER_ERROR, "discoveryInfo from BehaviorSubject is null " + jobId,
                                                                  empty()), getSelf());
            }
        } else {
            String msg = "JobCluster in the request " + r.getJobCluster() + " does not match Job Actors job ID " + this.jobId;
            LOGGER.warn(msg);
            sender.tell(new GetLatestJobDiscoveryInfoResponse(r.requestId, SERVER_ERROR, msg, empty()), getSelf());
        }
    }

    /**
     * Worker Events sent by the worker itself of the Scheduling Service.
     */
    @Override
    public void processWorkerEvent(final WorkerEvent e) {
        this.workerManager.processEvent(e, mantisJobMetaData.getState());
    }

    /**
     * Resubmit a specific worker Index.
     */
    @Override
    public void onResubmitWorker(final ResubmitWorkerRequest r) {
        if (LOGGER.isTraceEnabled()) LOGGER.trace("Enter Job {} onResubmitWorker {}", jobId, r);
        ActorRef sender = getSender();
        try {
            eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                    r.getWorkerNum() + " workerNum resubmit requested by " + r.getUser() + " , reason: "
                            + r.getReason(),
                    getJobId(), getJobState()));
            this.workerManager.resubmitWorker(r.getWorkerNum());
            numWorkerResubmissions.increment();
            sender.tell(new ResubmitWorkerResponse(r.requestId, SUCCESS,
                    String.format("Worker %d of job %s resubmitted", r.getWorkerNum(), r.getJobId())), getSelf());
        } catch (Exception e) {
            sender.tell(new ResubmitWorkerResponse(r.requestId, SERVER_ERROR, e.getMessage()), getSelf());
        }
        if (LOGGER.isTraceEnabled()) LOGGER.trace("Exit Job {} onResubmitWorker {}", jobId, r);
    }

    @Override
    public void onMigrateWorkers(final JobProto.MigrateDisabledVmWorkersRequest r) {
        LOGGER.trace("Enter JobActor::onMigrateWorkersRequest {}", jobId);
        workerManager.migrateDisabledVmWorkers(r.time);
    }

    /**
     * Invoked periodically to check heart beat status of the workers.
     *
     * @param r
     */
    @Override
    public void onCheckHeartBeats(final JobProto.CheckHeartBeat r) {
        LOGGER.trace("Enter JobActor::onCheckHearbeats {}", jobId);

        this.workerManager.checkHeartBeats(r.getTime());
    }

    @Override
    public void onRuntimeLimitReached(final JobProto.RuntimeLimitReached r) {
        LOGGER.info("In onRuntimeLimitReached {} for Job {} ", Instant.now(), this.jobId);
        LOGGER.info("Job {} Started at {} and killed at {} due to Runtime limit reached", jobId,
                mantisJobMetaData.getStartedAtInstant().orElse(Instant.now()), Instant.now());
        getContext().getParent().tell(new JobClusterProto.KillJobRequest(jobId,
                "runtime limit reached", JobCompletedReason.Killed,
                MANTIS_MASTER_USER, ActorRef.noSender()), getSelf());
    }

    @Override
    public void onSendWorkerAssignments(final JobProto.SendWorkerAssignementsIfChanged r) {
        LOGGER.trace("Enter JobActor::onSendWorkerAssignments {}", jobId);
        this.workerManager.refreshAndSendWorkerAssignments();
    }

    /**
     * Will update Job state to terminal.
     * Unschedule all workers
     * Update worker state as failed in DB
     * Archive job
     * Self destruct
     * <p>
     * Worker terminated events will get ignored.
     *
     * @param req
     */
    @Override
    public void onJobKill(JobClusterProto.KillJobRequest req) {
        LOGGER.trace("Enter JobActor::onJobKill {}", jobId);
        ActorRef sender = getSender();

        LOGGER.info("Shutting down job {} on request by {}", jobId, sender);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("shutting down job with metadata {}", mantisJobMetaData);
        }
        eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                "Killing job, reason: " + req.reason,
                getJobId(), getJobState()));
        try {
            JobState newState;
            if (req.jobCompletedReason.equals(JobCompletedReason.Error)
                    || req.jobCompletedReason.equals(JobCompletedReason.Lost)) {
                newState = JobState.Failed;
            } else {
                newState = JobState.Completed;
            }
            // update job state
            updateStateAndPersist(newState);
            // inform caller
            sender.tell(new JobClusterProto.KillJobResponse(req.requestId, SUCCESS, getJobState(), getJobId()
                    + " terminated", getJobId(), this.mantisJobMetaData, req.user, req.requestor), getSelf());
            // continue with rest of the shutdown
            getTimers().cancel(CHECK_HB_TIMER_KEY);
            getContext().become(terminatingBehavior);

            // shutdown workers
            shutdown(newState, req.reason);
            // take poison pill
            performFinalShutdown();

        } catch (Exception e) {
            sender.tell(new JobClusterProto.KillJobResponse(req.requestId, SERVER_ERROR, getJobState(),
                    getJobId() + " Could not be terminated due to " + e.getMessage(), getJobId(),
                    this.mantisJobMetaData, req.user, req.requestor), getSelf());
        }

        LOGGER.trace("Exit JobActor::onJobKill {}", jobId);
    }

    @Override
    public void onScaleStage(ScaleStageRequest scaleStage) {
        LOGGER.info("In Scale stage {} for Job {}", scaleStage, this.jobId);
        ActorRef sender = getSender();
        Optional<IMantisStageMetadata> stageMeta = this.mantisJobMetaData.getStageMetadata(scaleStage.getStageNum());

        // Make sure stage is valid
        if (!stageMeta.isPresent()) {
            LOGGER.warn("Stage {} does not exist in Job {}", scaleStage.getStageNum(), this.jobId);
            sender.tell(new ScaleStageResponse(scaleStage.requestId, CLIENT_ERROR, "Non existent stage "
                    + scaleStage.getStageNum(), 0), getSelf());
            return;
        }

        // Make sure stage is scalable
        MantisStageMetadataImpl stageMetaData = (MantisStageMetadataImpl) stageMeta.get();
        if (!stageMetaData.getScalable()) {
            LOGGER.warn("Stage {} is not scalable in Job {}", scaleStage.getStageNum(), this.jobId);
            eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(
                    LifecycleEventsProto.StatusEvent.StatusEventType.WARN,
                    "Can't change #workers to " + scaleStage.getNumWorkers() + ", stage "
                            + scaleStage.getStageNum() + " is not scalable", getJobId(), getJobState()));
            sender.tell(new ScaleStageResponse(scaleStage.requestId, CLIENT_ERROR, "Stage "
                    + scaleStage.getStageNum() + " is not scalable", 0), getSelf());
            return;
        }

        try {
            int actualScaleup = this.workerManager.scaleStage(stageMetaData, scaleStage.getNumWorkers(),
                    scaleStage.getReason());

            LOGGER.info("Scaled stage {} to {} workers for Job {}", scaleStage.getStageNum(), actualScaleup,
                    this.jobId);
            numScaleStage.increment();
            sender.tell(new ScaleStageResponse(scaleStage.requestId, SUCCESS,
                    String.format("Scaled stage %s to %s workers", scaleStage.getStageNum(), actualScaleup),
                    actualScaleup), getSelf());
        } catch (Exception e) {
            String msg = String.format("Stage %d scale failed due to %s", scaleStage.getStageNum(), e.getMessage());
            LOGGER.error(msg, e);
            sender.tell(new ScaleStageResponse(scaleStage.requestId, SERVER_ERROR, msg, 0), getSelf());
        }
    }

    /**
     * Responds with {@link ListWorkersResponse} object containing data about all active workers.
     * @param listWorkersRequest
     */
    public void onListActiveWorkers(ListWorkersRequest listWorkersRequest) {
        ActorRef sender = getSender();
        List<IMantisWorkerMetadata> activeWorkers = this.workerManager.getActiveWorkers(listWorkersRequest.getLimit());

        sender.tell(new ListWorkersResponse(listWorkersRequest.requestId, SUCCESS, "",
                Collections.unmodifiableList(activeWorkers)), getSelf());

    }


    //////////////////////////////////////////// Akka Messages sent to the Job Actor End/////////////////////////

    /////////////////////////////////////////// Internal State change events Begin //////////////////////////////

    private void performFinalShutdown() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Enter performFinalShutdown for Job {}", jobId);
        }
        try {
            LOGGER.info("Archiving Job {}", this.jobId);
            jobStore.archiveJob(mantisJobMetaData);
        } catch (IOException e) {
            LOGGER.warn("Exception archiving job " + mantisJobMetaData.getJobId(), e);
        }
        getContext().become(terminatedBehavior);
        // commit suicide
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Exit performFinalShutdown for Job {}", jobId);
        }
    }

    /**
     * Invoked when all workers are in terminal state.
     * Should get called only during shutdown process
     */
    @Override
    public void onAllWorkersCompleted() {
        LOGGER.info("JobActor: onAllWorkersCompleted with current state {}", mantisJobMetaData.getState());

        if (!JobState.isTerminalState(mantisJobMetaData.getState()) && !allWorkersCompleted) {

            LOGGER.info("All workers completed but job {} in {} state. Request termination", jobId, getJobState());
            allWorkersCompleted = true;
            getContext().parent().tell(
                    new JobClusterProto.KillJobRequest(
                            jobId, "Job Completed", JobCompletedReason.Normal, MANTIS_MASTER_USER,
                            ActorRef.noSender()), getSelf());

            numWorkersCompletedNotTerminal.increment();
        } else {
            // job kill has already been requested, ignore
            LOGGER.debug("Job {} Kill already requested", this.jobId);
        }
    }

    /**
     * Should get called only once after all workers have started.
     */
    @Override
    public boolean onAllWorkersStarted() {
        LOGGER.info("In onAllWorkersStarted for Job {}", jobId);
        boolean isSuccess = true;
        if (mantisJobMetaData.getState() == JobState.Accepted) {
            try {
                // update record in storage
                updateStateAndPersist(JobState.Launched);
                // update behavior to active
                getContext().become(activeBehavior);
                eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                        "all workers started, job transitioning to Active", getJobId(), getJobState()));

                // inform job cluster manager that the job has started
                getContext().getParent().tell(new JobClusterProto.JobStartedEvent(getJobId()), getSelf());
                // kick off max runtime timer if needed

                Instant currentTime = Instant.now();
                // Update start time and persist state
                mantisJobMetaData.setStartedAt(currentTime.toEpochMilli(), jobStore);

                setRuntimeLimitTimersIfRequired(currentTime);

            } catch (Exception e) {
                LOGGER.error("Error processing all worker started event ", e);
                isSuccess = false;
            }
        } else if (mantisJobMetaData.getState() == JobState.Launched) {
            // no op
            LOGGER.info("Job is already in launched state");
            isSuccess = false;

        } else {
            // something is wrong!
            LOGGER.warn("Unexpected all Workers Started Event while job in {} state", mantisJobMetaData.getState());
            isSuccess = false;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Exit AllWorkerStarted event processed successfully ? {}", isSuccess);
        }
        return isSuccess;
    }

    /**
     * Invoked if workers have been relaunched too many times.
     * Request this job to be terminated and marked as failed
     */

    @Override
    public boolean onTooManyWorkerResubmits() {
        LOGGER.warn("Too many worker resubmits detected for Job {}. Requesting job shutdown", jobId);
        boolean isSuccess = true;

        eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(ERROR,
                "Worker Resubmit limit reached, shutting down job", getJobId(), getJobState()));
        numWorkerResubmitLimitReached.increment();
        //updateStateAndPersist(JobState.Terminating_abnormal);
        // ask Parent to shut it down
        getContext().parent().tell(
                new JobClusterProto.KillJobRequest(
                        jobId, "Too many worker resubmits", JobCompletedReason.Error, MANTIS_MASTER_USER,
                        ActorRef.noSender()), getSelf());
        return isSuccess;

    }


    //////////////////////////////////Internal State Change Events END //////////////////////////////////////

    /**
     * Retuns the details of this job.
     */

    @Override
    public IMantisJobMetadata getJobDetails() {
        LOGGER.debug("Returning job Details {}", this.mantisJobMetaData);
        return this.mantisJobMetaData;
    }

    /**
     * Triggered when the JobActor receives the Job Kill message.
     * it will update the state of the job to terminating in the persistence layer and request
     * the workers to be terminated.
     *
     * @param state
     */

    @Override
    public void shutdown(JobState state, String reason) {
        LOGGER.info("Entering JobActor:shutdown {}", jobId);

        workerManager.shutdown();

        eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                "job shutdown, reason: " + reason,
                getJobId(), state));
        eventPublisher.publishAuditEvent(new LifecycleEventsProto.AuditEvent(
                LifecycleEventsProto.AuditEvent.AuditEventType.JOB_TERMINATE,
                jobId.getId(), "job shutdown, reason: " + reason));

    }

    @Override
    public JobId getJobId() {
        return this.jobId;
    }

    private void updateStateAndPersist(JobState newState) throws Exception {

        LOGGER.trace("Enter JobActor::updateStateAndPersist for Job {} Updating job Status {}", jobId, newState);

        mantisJobMetaData.setJobState(newState, jobStore);

        LOGGER.trace("Exit JobActor::updateStateAndPersist for Job {}", jobId);
    }

    /**
     * Always invoked after the job has transitioned to started state.
     * @param currentTime
     */
    private void setRuntimeLimitTimersIfRequired(Instant currentTime) {

        long maxRuntimeSecs = mantisJobMetaData.getJobDefinition().getJobSla().getRuntimeLimitSecs();
        Instant startedAt = mantisJobMetaData.getStartedAtInstant().orElse(currentTime);

        long terminateJobInSecs;
        if (maxRuntimeSecs > 0) {

            terminateJobInSecs = JobHelper.calculateRuntimeDuration(maxRuntimeSecs,startedAt);

            LOGGER.info("Will terminate Job {} at {} ", jobId, (currentTime.plusSeconds(terminateJobInSecs)));

            getTimers().startSingleTimer("RUNTIME_LIMIT", new JobProto.RuntimeLimitReached(),
                    Duration.ofSeconds(terminateJobInSecs));
        } else {
            LOGGER.info("maxRuntime for Job {} is  {} ignore ", jobId, mantisJobMetaData.getJobDefinition()
                    .getJobSla().getRuntimeLimitSecs());
        }
    }

    @Override
    public JobState getJobState() {
        return mantisJobMetaData.getState();
    }

    private boolean isAutoscaled(SchedulingInfo schedulingInfo) {
        LOGGER.trace("In isAutoscaled {}", schedulingInfo);
        for (Map.Entry<Integer, StageSchedulingInfo> entry : schedulingInfo.getStages().entrySet()) {

            final StageScalingPolicy scalingPolicy = entry.getValue().getScalingPolicy();

            if (scalingPolicy != null && scalingPolicy.isEnabled()) {
                LOGGER.info("Job {} is autoscaleable", jobId);
                return true;
            }
        }
        LOGGER.info("Job {} is NOT scaleable", jobId);
        return false;
    }

    /*package protected*/

    /**
     * Returns the calculated subscription timeout in seconds for this job.
     * @param mjmd
     * @return
     */
    static long getSubscriptionTimeoutSecs(final IMantisJobMetadata mjmd) {
        // if perpetual job there is no subscription timeout
        if (mjmd.getJobDefinition().getJobSla().getDurationType() == MantisJobDurationType.Perpetual)
            return 0;
        return mjmd.getSubscriptionTimeoutSecs() == 0
                ? ConfigurationProvider.getConfig().getEphemeralJobUnsubscribedTimeoutSecs()
                : mjmd.getSubscriptionTimeoutSecs();
    }


    /**
     * Keeps track of the last used worker number and mints a new one every time a worker is scheduled.
     */
    static class WorkerNumberGenerator {

        private static final Logger LOGGER = LoggerFactory.getLogger(WorkerNumberGenerator.class);
        private static final int DEFAULT_INCREMENT_STEP = 10;
        private final int incrementStep;
        private int lastUsed;
        private int currLimit;

        private volatile boolean hasErrored = false;

        /**
         * Creates an instance of this class.
         *
         * @param lastUsed
         * @param incrementStep
         */
        WorkerNumberGenerator(int lastUsed, int incrementStep) {
            Preconditions.checkArgument(lastUsed >= 0,
                    "Last Used worker Number cannot be negative {} ", lastUsed);
            Preconditions.checkArgument(incrementStep >= 1,
                    "incrementStepcannot be less than 1 {} ", incrementStep);
            LOGGER.debug("WorkerNumberGenerator ctor2 lastUsed {}", lastUsed);

            this.lastUsed = lastUsed;
            this.currLimit = lastUsed;
            this.incrementStep = incrementStep;

        }

        /**
         * Default constructor sets last used number to 0.
         */
        WorkerNumberGenerator() {
            this(0, DEFAULT_INCREMENT_STEP);
            LOGGER.trace("WorkerNumberGenerator ctor1");

        }

        private void advance(MantisJobMetadataImpl mantisJobMetaData, MantisJobStore jobStore) {
            LOGGER.trace("getNextWorkerNumber in advance");
            try {
                currLimit += incrementStep;

                mantisJobMetaData.setNextWorkerNumberToUse(currLimit, jobStore);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{} nextWorkerNumber set to : {} ", mantisJobMetaData.getJobId(), currLimit);
                }
            } catch (Exception e) {
                hasErrored = true;
                LOGGER.error("Exception setting next Worker number to use ", e);
                throw new RuntimeException("Unexpected: " + e.getMessage());
            }

        }

        /**
         * Get the next unused worker number.
         * <p>
         * For performance reasosns, this object updates state in persistence every N calls made to this method.
         *
         * @return The next worker number to use for new workers
         *
         * @throws IllegalStateException if there was an error saving the next worker number to use to the job store
         */
        int getNextWorkerNumber(MantisJobMetadataImpl mantisJobMetaData, MantisJobStore jobStore) {
            LOGGER.debug("getNextWorkerNumber lastUsed {}", lastUsed);
            if (hasErrored)
                throw new IllegalStateException("Unexpected: Invalid state likely due to getting/setting"
                        + "next worker number");

            if (lastUsed == currLimit)
                advance(mantisJobMetaData, jobStore);
            LOGGER.debug("getNextWorkerNumber returns {}", lastUsed + 1);
            return ++lastUsed;

        }
    }

    /**
     * Responsible for managing worker related state of this job.
     */
    class WorkerManager implements IWorkerManager {

        private static final int WORKER_RESUBMIT_LIMIT = 100;
        private ObjectMapper mapper = new ObjectMapper();

        private final WorkerNumberGenerator workerNumberGenerator;
        private boolean allWorkersStarted = false;
        private final IMantisJobManager jobMgr;
        private ConcurrentSkipListSet<Integer> workersToMigrate = new ConcurrentSkipListSet<>();
        private int sinkStageNum;
        private final MigrationStrategy migrationStrategy;
        private final MantisScheduler scheduler;
        private long lastWorkerMigrationTimestamp = Long.MIN_VALUE;
        private Map<Integer, WorkerAssignments> stageAssignments = new HashMap<>();
        private BehaviorSubject<JobSchedulingInfo> jobSchedulingInfoBehaviorSubject;
        private String currentJobSchedulingInfoStr = null;
        private final WorkerResubmitRateLimiter resubmitRateLimiter = new WorkerResubmitRateLimiter();
        private volatile boolean stageAssignmentPotentiallyChanged;

        /**
         * Creates an instance of this class.
         *
         * @param jobMgr
         * @param migrationConfig
         * @param scheduler
         * @param isSubmit
         *
         * @throws Exception
         */
        WorkerManager(IMantisJobManager jobMgr, WorkerMigrationConfig migrationConfig, MantisScheduler scheduler,
                      boolean isSubmit) throws Exception {
            LOGGER.debug("In WorkerManager ctor");

            workerNumberGenerator = new WorkerNumberGenerator((isSubmit) ? 0
                    : jobMgr.getJobDetails().getNextWorkerNumberToUse(), WorkerNumberGenerator.DEFAULT_INCREMENT_STEP);
            this.scheduler = scheduler;
            this.jobMgr = jobMgr;
            migrationStrategy = MigrationStrategyFactory.getStrategy(jobId.getId(), migrationConfig);
            int noOfStages = mantisJobMetaData.getStageMetadata().size();
            if (noOfStages == 1) {
                sinkStageNum = 1;
            } else {
                sinkStageNum = noOfStages - 1;
            }
            JobSchedulingInfo initialJS = new JobSchedulingInfo(jobMgr.getJobId().getId(), new HashMap<>());
            currentJobSchedulingInfoStr = mapper.writeValueAsString(initialJS);
            jobSchedulingInfoBehaviorSubject = BehaviorSubject.create(initialJS);

            initialize(isSubmit);
            LOGGER.debug("Exit WorkerManager ctor");
        }

        /**
         * Initializes a worker manager.
         *
         * A WorkerManager can get initialized on a job submission or a failover.
         *
         * Init from Job submission: submits initial workers which each go through their startup lifecycle.
         *
         * Init from Master failover: workers are already running; gets state from Mesos and updates its view
         * of the world. If worker information is bad from Mesos, gather up these worker and resubmit them
         * in all together after initialization of running workers.
         *
         * @param isSubmit specifies if this initialization is due to job submission or a master failover.
         *
         * @throws Exception
         */
        void initialize(boolean isSubmit) throws Exception {

            LOGGER.trace("In initialize WorkerManager for Job {} with isSubmit {}", jobId, isSubmit);
            if (isSubmit) {
                submitInitialWorkers();
            } else {
                initializeRunningWorkers();
            }
            LOGGER.trace("Exit initialize WorkerManager for Job {}", jobId);
        }

        private void initializeRunningWorkers() {
            LOGGER.trace("In initializeRunningWorkers for Job {}", jobId);

            // Scan for the list of all corrupted workers to be resubmitted.
            List<JobWorker> workersToResubmit = markCorruptedWorkers();

            // publish a refresh before enqueuing tasks to the Scheduler, as there is a potential race between
            // WorkerRegistryV2 getting updated and isWorkerValid being called from SchedulingService loop
            // If worker is not found in the SchedulingService loop, it is considered invalid and prematurely
            // removed from Fenzo state.
            markStageAssignmentsChanged(true);

            for (IMantisStageMetadata stageMeta : mantisJobMetaData.getStageMetadata().values()) {
                Map<Integer, WorkerHost> workerHosts = new HashMap<>();

                for (JobWorker worker : stageMeta.getAllWorkers()) {
                    IMantisWorkerMetadata wm = worker.getMetadata();

                    if (WorkerState.isRunningState(wm.getState())) {
                        // send fake heartbeat
                        try {
                            WorkerEvent fakeHB = new WorkerHeartbeat(new Status(jobId.getId(), stageMeta.getStageNum(),
                                    wm.getWorkerIndex(), wm.getWorkerNumber(), Status.TYPE.HEARTBEAT, "",
                                    MantisJobState.Started, System.currentTimeMillis()));
                            worker.processEvent(fakeHB, jobStore);
                        } catch (InvalidWorkerStateChangeException | IOException e) {
                            LOGGER.error("problem sending initial heartbeat for Job {} during initialization",
                                    worker.getMetadata().getJobId(), e);
                        }

                        workerHosts.put(wm.getWorkerNumber(),
                                new WorkerHost(
                                        wm.getSlave(),
                                        wm.getWorkerIndex(),
                                        wm.getWorkerPorts().getPorts(),
                                        DataFormatAdapter.convertWorkerStateToMantisJobState(wm.getState()),
                                        wm.getWorkerNumber(),
                                        wm.getMetricsPort(),
                                        wm.getCustomPort()));

                        ScheduleRequest scheduleRequest = createSchedulingRequest(wm, empty());

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("initializing Running task {}-worker-{}-{}", wm.getJobId(),
                                    wm.getWorkerIndex(), wm.getWorkerNumber());
                        }

                        scheduler.initializeRunningWorker(scheduleRequest, wm.getSlave());
                        LOGGER.debug("Initialized running worker {}", wm.getSlave());
                    } else if (wm.getState().equals(WorkerState.Accepted)) {
                        queueTask(wm);
                    }
                }

                if (stageMeta.getStageNum() > 0) {
                    stageAssignments.put(stageMeta.getStageNum(), new WorkerAssignments(stageMeta.getStageNum(),
                            stageMeta.getNumWorkers(), workerHosts));
                }
            }

            // publish another update after queuing tasks to Fenzo (in case some workers were marked Started
            // due to the Fake heartbeat in above loop)
            markStageAssignmentsChanged(true);

            // Resubmit workers with missing ports so they can be reassigned new resources.
            for (JobWorker jobWorker : workersToResubmit) {
                LOGGER.warn("discovered workers with missing ports during initialization: {}", jobWorker);
                try {
                    resubmitWorker(jobWorker);
                } catch (Exception e) {
                    LOGGER.warn("Exception resubmitting worker {} during initializeRunningWorkers due to {}",
                            jobWorker, e.getMessage(), e);
                }
            }

            LOGGER.trace("Initialized running workers for Job {} complete", jobId);
        }

        private List<JobWorker> markCorruptedWorkers() {
            LOGGER.trace("Enter markCorruptedWorkers for Job {} ", jobId);
            List<JobWorker> corruptedWorkers = new ArrayList<>();

            for (IMantisStageMetadata stageMeta : mantisJobMetaData.getStageMetadata().values()) {
                for (JobWorker worker : stageMeta.getAllWorkers()) {
                    IMantisWorkerMetadata wm = worker.getMetadata();

                    Optional<WorkerPorts> workerPortsOptional = wm.getPorts();
                    if (WorkerState.isRunningState(wm.getState()) &&
                            (!workerPortsOptional.isPresent() || !workerPortsOptional.get().isValid())) {

                        LOGGER.info("marking corrupted worker {} for Job ID {} as {}",
                                worker.getMetadata().getWorkerId(), jobId, WorkerState.Failed);
                        numMissingWorkerPorts.increment();
                        // Mark this worker as corrupted.
                        corruptedWorkers.add(worker);

                        // Send initial status event to signal to the worker to mark itself as failed.
                        try {
                            WorkerStatus status = new WorkerStatus(new Status(jobId.getId(), stageMeta.getStageNum(),
                                    wm.getWorkerIndex(), wm.getWorkerNumber(), Status.TYPE.HEARTBEAT, "",
                                    MantisJobState.Failed, System.currentTimeMillis()));
                            worker.processEvent(status, jobStore);
                        } catch (InvalidWorkerStateChangeException | IOException e) {
                            LOGGER.error("problem sending initial heartbeat for Job {} during initialization",
                                    worker.getMetadata().getJobId(), e);
                        }
                    }
                }
            }
            LOGGER.trace("Exit markCorruptedWorkers for Job {} ", jobId);

            return corruptedWorkers;
        }

        private void markStageAssignmentsChanged(boolean forceRefresh) {
            this.stageAssignmentPotentiallyChanged = true;
            long refreshInterval = ConfigurationProvider.getConfig().getStageAssignmentRefreshIntervalMs();
            if (refreshInterval == -1 || forceRefresh) {
                refreshStageAssignmentsAndPush();
            }
        }

        private void refreshStageAssignmentsAndPush() {
            LOGGER.trace("Enter refreshStageAssignmentsAndPush for Job {} ", jobId);

            if (!stageAssignmentPotentiallyChanged) {
                LOGGER.debug("Worker Assignments have not changed since last push skipping.");
                return;
            }

            List<IMantisWorkerMetadata> acceptedAndActiveWorkers = new ArrayList<>();
            List<IMantisWorkerMetadata> activeWorkers = new ArrayList<>();

            for (IMantisStageMetadata stageMeta : mantisJobMetaData.getStageMetadata().values()) {

                Map<Integer, WorkerHost> workerHosts = new HashMap<>();
                for (JobWorker worker : stageMeta.getAllWorkers()) {
                    IMantisWorkerMetadata wm = worker.getMetadata();
                    if (WorkerState.isRunningState(wm.getState())) {

                        workerHosts.put(wm.getWorkerNumber(),
                                new WorkerHost(
                                        wm.getSlave(),
                                        wm.getWorkerIndex(),
                                        wm.getWorkerPorts().getPorts(),
                                        DataFormatAdapter.convertWorkerStateToMantisJobState(wm.getState()),
                                        wm.getWorkerNumber(),
                                        wm.getMetricsPort(),
                                        wm.getCustomPort()));
                        activeWorkers.add(wm);
                        acceptedAndActiveWorkers.add(wm);

                    } else if (wm.getState().equals(WorkerState.Accepted)) {
                        acceptedAndActiveWorkers.add(wm);
                    }

                }
                stageAssignments.put(stageMeta.getStageNum(), new WorkerAssignments(stageMeta.getStageNum(),
                        stageMeta.getNumWorkers(), workerHosts));


            }
            JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(jobId.getId(), stageAssignments);

            LOGGER.debug("publishing scheduling Info for job {}", jobId);
            jobSchedulingInfoBehaviorSubject.onNext(jobSchedulingInfo);

            eventPublisher.publishWorkerListChangedEvent(new LifecycleEventsProto.WorkerListChangedEvent(
                    new WorkerInfoListHolder(this.jobMgr.getJobId(), acceptedAndActiveWorkers)));

            numSchedulingChangesRefreshed.increment();

            stageAssignmentPotentiallyChanged = false;

            LOGGER.trace("Exit refreshStageAssignmentsAndPush for Job {} ", jobId);
        }

        private void submitInitialWorkers() throws Exception {
            List<IMantisWorkerMetadata> workers = getInitialWorkers(mantisJobMetaData.getJobDefinition(),
                    System.currentTimeMillis());

            LOGGER.debug("Got initial workers " + workers);
            int beg = 0;
            while (true) {
                if (beg >= workers.size())
                    break;
                int en = beg + Math.min(workerWritesBatchSize, workers.size() - beg);
                final List<IMantisWorkerMetadata> workerRequests = workers.subList(beg, en);
                try {
                    jobStore.storeNewWorkers(jobMgr.getJobDetails(), workerRequests);
                    LOGGER.info("Stored workers {} for Job {}", workerRequests, jobId);
                    // refresh Worker Registry state before enqueuing task to Scheduler
                    markStageAssignmentsChanged(true);
                    // queue to scheduler
                    workerRequests.forEach(this::queueTask);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error {} storing workers of job {}", e.getMessage(), jobId.getId());
                    throw new RuntimeException(String.format("Exception saving worker %s for Job %s ", e.getMessage(),
                            jobId));
                }
                beg = en;
            }

        }

        private void queueTask(final IMantisWorkerMetadata workerRequest, final Optional<Long> readyAt) {
            final ScheduleRequest schedulingRequest = createSchedulingRequest(workerRequest, readyAt);
            LOGGER.info("Queueing up scheduling request {} ", schedulingRequest);
            try {
                scheduler.scheduleWorker(schedulingRequest);
            } catch (Exception e) {
                LOGGER.error("Exception queueing task", e);
                e.printStackTrace();
            }
        }

        private void queueTask(final IMantisWorkerMetadata workerRequest) {
            queueTask(workerRequest, empty());
        }

        private ScheduleRequest createSchedulingRequest(final IMantisWorkerMetadata workerRequest,
                                                        final Optional<Long> readyAt) {
            try {
                LOGGER.trace("In createSchedulingRequest for worker {}", workerRequest);

                final WorkerId workerId = workerRequest.getWorkerId();

                // setup constraints
                final List<ConstraintEvaluator> hardConstraints = new ArrayList<>();
                final List<VMTaskFitnessCalculator> softConstraints = new ArrayList<>();
                Optional<IMantisStageMetadata> stageMetadataOp =
                        mantisJobMetaData.getStageMetadata(workerRequest.getStageNum());
                LOGGER.debug("Got stageMeta {}", stageMetadataOp);

                if (!stageMetadataOp.isPresent()) {
                    throw new RuntimeException(String.format("No such stage %s", workerRequest.getStageNum()));
                }
                IMantisStageMetadata stageMetadata = stageMetadataOp.get();

                List<JobConstraints> stageHC = stageMetadata.getHardConstraints();

                List<JobConstraints> stageSC = stageMetadata.getSoftConstraints();

                final Set<String> coTasks = new HashSet<>();

                if ((stageHC != null && !stageHC.isEmpty())
                        || (stageSC != null && !stageSC.isEmpty())) {
                    for (JobWorker jobWorker : stageMetadata.getAllWorkers()) {
                        if (jobWorker.getMetadata().getWorkerNumber() != workerId.getWorkerNum())
                            coTasks.add(workerId.getId());
                    }
                }

                if (stageHC != null && !stageHC.isEmpty()) {
                    for (JobConstraints c : stageHC) {
                        hardConstraints.add(ConstraintsEvaluators.hardConstraint(c, coTasks));
                    }
                }

                if (stageSC != null && !stageSC.isEmpty()) {
                    for (JobConstraints c : stageSC) {
                        softConstraints.add(ConstraintsEvaluators.softConstraint(c, coTasks));
                    }
                }

                ScheduleRequest sr = new ScheduleRequest(workerId,
                        workerRequest.getStageNum(),
                        workerRequest.getNumberOfPorts(),
                        new JobMetadata(mantisJobMetaData.getJobId().getId(),
                                mantisJobMetaData.getJobJarUrl(),
                                mantisJobMetaData.getTotalStages(),
                                mantisJobMetaData.getUser(),
                                mantisJobMetaData.getSchedulingInfo(),
                                mantisJobMetaData.getParameters(),
                                getSubscriptionTimeoutSecs(mantisJobMetaData),
                                mantisJobMetaData.getMinRuntimeSecs()
                        ),
                        mantisJobMetaData.getSla().orElse(new JobSla.Builder().build()).getDurationType(),
                        stageMetadata.getMachineDefinition(),
                        hardConstraints,
                        softConstraints,
                        readyAt.orElse(0L),
                        workerRequest.getPreferredClusterOptional());
                LOGGER.trace("created scheduleRequest {}", sr);
                return sr;
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Exception creating scheduleRequest {}", e.getMessage());
                throw e;
            }
        }

        private List<IMantisWorkerMetadata> getInitialWorkers(JobDefinition jobDetails, long submittedAt)
                throws Exception {
            List<IMantisWorkerMetadata> workerRequests = Lists.newLinkedList();
            LOGGER.debug("In getInitial Workers : " + jobDetails);

            SchedulingInfo schedulingInfo = jobDetails.getSchedulingInfo();
            LOGGER.debug("scheduling info " + schedulingInfo);
            int totalStages = schedulingInfo.getStages().size();

            LOGGER.debug("total stages {} ", totalStages);

            Iterator<Integer> it = schedulingInfo.getStages().keySet().iterator();
            while (it.hasNext()) {
                int stageNum = it.next();
                List<IMantisWorkerMetadata> stageWorkers = setupStageWorkers(schedulingInfo, totalStages,
                        stageNum, submittedAt);
                workerRequests.addAll(stageWorkers);
            }

            return workerRequests;
        }


        private List<IMantisWorkerMetadata> setupStageWorkers(SchedulingInfo schedulingInfo, int totalStages,
                                                              int stageNum, long submittedAt) throws Exception {
            LOGGER.trace("In setupStageWorkers for Job {} with sched info", jobId, schedulingInfo);

            List<IMantisWorkerMetadata> workerRequests = new LinkedList<>();
            StageSchedulingInfo stage = schedulingInfo.getStages().get(stageNum);
            if (stage == null) {
                LOGGER.error("StageSchedulingInfo cannot be null for Stage {}", stageNum);
                throw new Exception("StageSchedulingInfo cannot be null for Stage " + stageNum);
                //return workerRequests; // can happen when stageNum=0 and there is no jobMaster defined
            }

            int numInstancesAtStage = stage.getNumberOfInstances();
            // add worker request for each instance required in stage
            int stageIndex = 0;
            for (int i = 0; i < numInstancesAtStage; i++) {
                // during initialization worker number and index are identical
                int workerIndex = stageIndex++;

                if (!mantisJobMetaData.getStageMetadata(stageNum).isPresent()) {
                    IMantisStageMetadata msmd = new MantisStageMetadataImpl.Builder().
                            withJobId(jobId)
                            .withStageNum(stageNum)
                            .withNumStages(totalStages)
                            .withMachineDefinition(stage.getMachineDefinition())
                            .withNumWorkers(numInstancesAtStage)
                            .withHardConstraints(stage.getHardConstraints())
                            .withSoftConstraints(stage.getSoftConstraints())
                            .withScalingPolicy(stage.getScalingPolicy())

                            .isScalable(stage.getScalable())
                            .build();
                    LOGGER.debug("Job Actor adding stage ");
                    mantisJobMetaData.addJobStageIfAbsent(msmd);
                    jobStore.updateStage(msmd);

                    LOGGER.debug("Added " + mantisJobMetaData.getStageMetadata(stageNum));

                    LOGGER.debug("Total stages " + mantisJobMetaData.getTotalStages());
                }
                IMantisWorkerMetadata mwmd = addWorker(schedulingInfo, stageNum, workerIndex);
                workerRequests.add(mwmd);
            }
            LOGGER.trace("Exit setupStageWorkers for Job {} ", jobId);
            return workerRequests;
        }

        private IMantisWorkerMetadata addWorker(SchedulingInfo schedulingInfo, int stageNo, int workerIndex)
                throws InvalidJobException {

            LOGGER.trace("In addWorker for index {} for Job {} with sched info", workerIndex, jobId, schedulingInfo);

            StageSchedulingInfo stageSchedInfo = schedulingInfo.getStages().get(stageNo);
            int workerNumber = workerNumberGenerator.getNextWorkerNumber(mantisJobMetaData, jobStore);
            JobWorker jw = new JobWorker.Builder()
                    .withJobId(jobId)
                    .withWorkerIndex(workerIndex)
                    .withWorkerNumber(workerNumber)
                    .withNumberOfPorts(stageSchedInfo.getMachineDefinition().getNumPorts()
                            + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                    .withStageNum(stageNo)
                    .withLifecycleEventsPublisher(eventPublisher)

                    .build();
            if (!mantisJobMetaData.addWorkerMetadata(stageNo, jw)) {
                Optional<JobWorker> tmp = mantisJobMetaData.getWorkerByIndex(stageNo, workerIndex);
                if (tmp.isPresent()) {
                    throw new InvalidJobException(mantisJobMetaData.getJobId().getId(), stageNo, workerIndex,
                            new Exception("Couldn't add worker " + workerNumber + " as index " + workerIndex
                                    + ", that index already has worker " + tmp.get().getMetadata().getWorkerNumber()));
                } else {
                    throw new InvalidJobException(mantisJobMetaData.getJobId().getId(), stageNo, workerIndex,
                            new Exception("Couldn't add worker " + workerNumber + " as index "
                                    + workerIndex + "doesn't exist "));
                }
            }


            LOGGER.trace("Exit addWorker for index {} for Job {} with sched info", workerIndex, jobId, schedulingInfo);

            return jw.getMetadata();
        }


        @Override
        public void shutdown() {
            LOGGER.trace("Enter shutdown for Job {}", jobId);
            // if workers have not already completed
            if (!allWorkerCompleted()) {
                // kill workers
                terminateAllWorkersAsync();
            }
            //send empty schedulingInfo changes so downstream jobs would explicitly disconnect
            jobSchedulingInfoBehaviorSubject.onNext(new JobSchedulingInfo(this.jobMgr.getJobId().getId(),
                    new HashMap<>()));
            jobSchedulingInfoBehaviorSubject.onCompleted();

            LOGGER.trace("Exit shutdown for Job {}", jobId);
        }

        private void terminateAllWorkersAsync() {
            LOGGER.info("Terminating all workers of job {}", jobId);

            Observable.from(mantisJobMetaData.getStageMetadata().values())
                    .flatMap((st) -> Observable.from(st.getAllWorkers()))
                    .filter((worker) -> !WorkerState.isTerminalState(worker.getMetadata().getState()))
                    .map((worker) -> {
                        LOGGER.info("Terminating " + worker);
                        terminateWorker(worker.getMetadata(), WorkerState.Completed, JobCompletedReason.Killed);
                        return worker;
                    })
                    .doOnCompleted(() -> markStageAssignmentsChanged(true))
                    .subscribeOn(Schedulers.io())

                    .subscribe();


            LOGGER.info("Terminated all workers of job {}", jobId);
        }

        private void terminateWorker(IMantisWorkerMetadata workerMeta, WorkerState finalWorkerState,
                                     JobCompletedReason reason) {
            LOGGER.info("Terminating  worker {} with number {}", workerMeta, workerMeta.getWorkerNumber());
            try {
                WorkerId workerId = workerMeta.getWorkerId();
                // call vmservice terminate
                scheduler.unscheduleAndTerminateWorker(workerMeta.getWorkerId(),
                        Optional.ofNullable(workerMeta.getSlave()));

                LOGGER.debug("WorkerNumber->StageMap {}", mantisJobMetaData.getWorkerNumberToStageMap());

                int stageNum = mantisJobMetaData.getWorkerNumberToStageMap().get(workerMeta.getWorkerNumber());

                Optional<IMantisStageMetadata> stageMetaOp = mantisJobMetaData.getStageMetadata(stageNum);
                if (stageMetaOp.isPresent()) {

                    // Mark work as terminal
                    WorkerTerminate terminateEvent = new WorkerTerminate(workerId, finalWorkerState, reason);
                    MantisStageMetadataImpl stageMetaData = (MantisStageMetadataImpl) stageMetaOp.get();
                    Optional<JobWorker> jobWorkerOp = stageMetaData.processWorkerEvent(terminateEvent, jobStore);

                    // Mark work as terminal
                    if (jobWorkerOp.isPresent()) {
                        jobStore.archiveWorker(jobWorkerOp.get().getMetadata());
                        eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(INFO,
                                "Terminated worker, reason: " + reason.name(),
                                workerMeta.getStageNum(), workerMeta.getWorkerId(), workerMeta.getState()));
                    }
                } else {
                    LOGGER.error("Stage {} not found while terminating worker {}", stageNum, workerId);
                }
            } catch (Exception e) {
                LOGGER.error("Error terminating worker {}", workerMeta.getWorkerId(), e);
            }
        }

        private void terminateAndRemoveWorker(IMantisWorkerMetadata workerMeta, WorkerState finalWorkerState,
                                              JobCompletedReason reason) {
            LOGGER.info("Terminating and removing worker {}", workerMeta.getWorkerId().getId());
            try {
                WorkerId workerId = workerMeta.getWorkerId();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("WorkerNumber->StageMap {}", mantisJobMetaData.getWorkerNumberToStageMap());
                }
                int stageNum = mantisJobMetaData.getWorkerNumberToStageMap().get(workerMeta.getWorkerNumber());

                Optional<IMantisStageMetadata> stageMetaOp = mantisJobMetaData.getStageMetadata(stageNum);
                if (stageMetaOp.isPresent()) {
                    // Mark work as terminal
                    WorkerTerminate terminateEvent = new WorkerTerminate(workerId, finalWorkerState, reason);
                    MantisStageMetadataImpl stageMetaData = (MantisStageMetadataImpl) stageMetaOp.get();
                    Optional<JobWorker> workerOp = stageMetaData.processWorkerEvent(terminateEvent, jobStore);


                    eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(INFO,
                            "Removing worker, reason: " + reason.name(),
                            workerMeta.getStageNum(), workerMeta.getWorkerId(), workerMeta.getState()));
                    // remove this worker index and archives the worker
                    stageMetaData.unsafeRemoveWorker(workerId.getWorkerIndex(), workerId.getWorkerNum(), jobStore);

                    // call vmservice terminate
                    scheduler.unscheduleAndTerminateWorker(workerMeta.getWorkerId(), Optional.ofNullable(
                            workerMeta.getSlave()));

                    //remove from workerNumber to stage map
                    mantisJobMetaData.removeWorkerMetadata(workerMeta.getWorkerNumber());

                    LOGGER.info("Terminated worker {}", workerMeta);
                    markStageAssignmentsChanged(true);
                } else {
                    LOGGER.error("Stage {} not found while terminating worker {}", stageNum, workerId);

                }
            } catch (Exception e) {
                LOGGER.error("Error terminating worker {}", workerMeta.getWorkerId(), e);
            }

        }

        @Override
        public void refreshAndSendWorkerAssignments() {
            LOGGER.trace("In WorkerManager::refreshAndSendWorkerAssignments");
            refreshStageAssignmentsAndPush();
            LOGGER.trace("Exit WorkerManager::refreshAndSendWorkerAssignments");

        }

        @Override
        public void checkHeartBeats(Instant currentTime) {
            LOGGER.trace("In WorkerManager::checkHeartBeats");
            //// heartbeat misses are calculated as 3 * heartbeatInterval, pick 1.5 multiplier for this check interval
            long missedHeartBeatToleranceSecs = (long) (1.5 * ConfigurationProvider.getConfig().getWorkerTimeoutSecs());
            // Allow more time for workers to start
            long stuckInSubmitToleranceSecs = missedHeartBeatToleranceSecs + 120;

            List<JobWorker> workersToResubmit = Lists.newArrayList();
            // expire worker resubmit entries
            resubmitRateLimiter.expireResubmitRecords(currentTime.toEpochMilli());
            // For each stage
            for (Iterator<? extends IMantisStageMetadata> stageIt =
                 mantisJobMetaData.getStageMetadata().values().iterator(); stageIt.hasNext();) {
                IMantisStageMetadata stage = stageIt.next();
                // For each worker in the stage
                for (Iterator<JobWorker> workerIt = stage.getAllWorkers().iterator(); workerIt.hasNext();) {
                    JobWorker worker = workerIt.next();
                    IMantisWorkerMetadata workerMeta = worker.getMetadata();
                    if (!workerMeta.getLastHeartbeatAt().isPresent()) {
                        System.out.println("1");
                        Instant acceptedAt = Instant.ofEpochMilli(workerMeta.getAcceptedAt());
                        if (Duration.between(acceptedAt, currentTime).getSeconds() > stuckInSubmitToleranceSecs) {
                            // worker stuck in accepted
                            workersToResubmit.add(worker);
                            eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(WARN,
                                    "worker stuck in Accepted state, resubmitting worker",
                                    workerMeta.getStageNum(),
                                    workerMeta.getWorkerId(),
                                    workerMeta.getState()));
                        }
                    } else {
                        LOGGER.debug("Duration between last heartbeat and now {} ",
                                Duration.between(workerMeta.getLastHeartbeatAt().get(), currentTime).getSeconds());

                        if (Duration.between(workerMeta.getLastHeartbeatAt().get(), currentTime).getSeconds()
                                > missedHeartBeatToleranceSecs) {
                            // heartbeat too old
                            LOGGER.info("Job {}, Worker {} Duration between last heartbeat and now {} "
                                    + "missed heart beat threshold {} exceeded", this.jobMgr.getJobId(),
                                    workerMeta.getWorkerId(), Duration.between(workerMeta.getLastHeartbeatAt().get(),
                                            currentTime).getSeconds(), missedHeartBeatToleranceSecs);

                            if (ConfigurationProvider.getConfig().isHeartbeatTerminationEnabled()) {
                                eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(WARN,
                                        "heartbeat too old, resubmitting worker", workerMeta.getStageNum(),
                                        workerMeta.getWorkerId(), workerMeta.getState()));

                                workersToResubmit.add(worker);
                            } else {
                                LOGGER.warn("Heart beat based termination is disabled. Skipping termination of "
                                        + "worker {} Please see mantis.worker.heartbeat.termination.enabled",
                                        workerMeta);
                            }
                        }
                    }
                }
            }

            for (JobWorker worker : workersToResubmit) {
                try {
                    resubmitWorker(worker);
                } catch (Exception e) {
                    LOGGER.warn("Exception {} occurred resubmitting Worker {}", e.getMessage(), worker.getMetadata(), e);
                }
            }
            migrateDisabledVmWorkers(currentTime);

        }

        @Override
        public void migrateDisabledVmWorkers(Instant currentTime) {
            LOGGER.trace("Enter migrateDisabledVmWorkers Workers To Migrate {} for Job {}", workersToMigrate, jobId);

            if (!workersToMigrate.isEmpty()) {
                Map<Integer, Integer> workerToStageMap = mantisJobMetaData.getWorkerNumberToStageMap();
                final List<Integer> workers = migrationStrategy.execute(workersToMigrate,
                        getNumberOfWorkersInStartedState(), getTotalWorkerCount(), lastWorkerMigrationTimestamp);
                if (!workers.isEmpty()) {
                    LOGGER.info("Job {} Going to migrate {} workers in this iteration", jobId, workers.size());
                }
                workers.forEach((w) -> {
                    if (workerToStageMap.containsKey(w)) {
                        int stageNo = workerToStageMap.get(w);
                        Optional<IMantisStageMetadata> stageMetaOp = mantisJobMetaData.getStageMetadata(stageNo);
                        if (stageMetaOp.isPresent()) {
                            JobWorker jobWorker = null;
                            try {
                                jobWorker = stageMetaOp.get().getWorkerByWorkerNumber(w);
                                IMantisWorkerMetadata wm = jobWorker.getMetadata();
                                LOGGER.info("Moving worker {} of job {} away from disabled VM", wm.getWorkerId(),
                                        jobId);
                                eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(INFO,
                                        " Moving out of disabled VM " + wm.getSlave(), wm.getStageNum(),
                                        wm.getWorkerId(), wm.getState()));
                                resubmitWorker(jobWorker);
                                lastWorkerMigrationTimestamp = System.currentTimeMillis();
                            } catch (Exception e) {
                                LOGGER.warn("Exception resubmitting worker {} during migration due to {}",
                                        jobWorker, e.getMessage(), e);
                            }

                        } else {
                            LOGGER.warn("Stage {} Not Found. Skip move for worker {} in Job {}", stageNo, w, jobId);
                        }
                    } else {
                        LOGGER.warn("worker {} not found in workerToStageMap {} for Job {}", w, workerToStageMap,
                                jobId);
                    }
                });
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Exit migrateWorkersIfNeeded Workers To Migrate {} for Job {}", workersToMigrate, jobId);
            }
        }

        private Optional<IMantisStageMetadata> getStageForWorker(WorkerEvent event) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Enter getStageForWorker with Num {} in Job {} ", event.getWorkerId().getWorkerNum(),
                        this.jobMgr.getJobId());
            }
            // Make sure we know about this worker. If not terminate it
            Map<Integer, Integer> workerToStageMap = mantisJobMetaData.getWorkerNumberToStageMap();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Worker to Stage Map {} in Job {}", workerToStageMap, this.jobMgr.getJobId());
            }
            if (!workerToStageMap.containsKey(event.getWorkerId().getWorkerNum())) {
                LOGGER.warn("Event {} from Unknown worker {} ", event.getWorkerId(), event);
                LOGGER.trace("Exit getStageForWorker");
                return empty();
            }

            // Find stage associated with this worker
            Integer stageNum = workerToStageMap.get(event.getWorkerId().getWorkerNum());
            Optional<IMantisStageMetadata> stageMetaOp = mantisJobMetaData.getStageMetadata(stageNum);
            if (!stageMetaOp.isPresent()) {
                LOGGER.warn("Stage {} not found in Job {} while processing event {}", stageNum, jobId, event);
            }
            LOGGER.trace("Exit getStageForWorker");
            return stageMetaOp;
        }

        private void terminateUnknownWorkerIfNonTerminal(final WorkerEvent event) {
            if (!JobHelper.isTerminalWorkerEvent(event)) {
                LOGGER.warn("Non terminal event from Unknown worker {} in Job {}. Request Termination",
                        event.getWorkerId(), this.jobMgr.getJobId());
                Optional<String> host = JobHelper.getWorkerHostFromWorkerEvent(event);

                scheduler.unscheduleAndTerminateWorker(event.getWorkerId(), host);
            } else {
                LOGGER.warn("Job {} Terminal event from Unknown worker {}. Ignoring", jobId, event.getWorkerId());
            }
        }

        @Override
        public void processEvent(WorkerEvent event, JobState jobState) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Processing worker event in Worker Manager {} in Job {}", event, this.jobMgr.getJobId());
            }

            try {
                Optional<IMantisStageMetadata> stageMetaOp = getStageForWorker(event);
                if (!stageMetaOp.isPresent()) {
                    terminateUnknownWorkerIfNonTerminal(event);
                    return;
                }
                // If worker cannot be scheduled currently, then put it back on the queue with delay and don't update
                // its state
                if (event instanceof WorkerUnscheduleable) {
                    scheduler.updateWorkerSchedulingReadyTime(event.getWorkerId(),
                            resubmitRateLimiter.getWorkerResubmitTime(event.getWorkerId(),
                                    stageMetaOp.get().getStageNum()));
                    eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(
                            LifecycleEventsProto.StatusEvent.StatusEventType.ERROR,
                            "rate limiting: no resources to fit worker",
                            ((WorkerUnscheduleable) event).getStageNum(), event.getWorkerId(), WorkerState.Accepted));
                    return;
                }

                MantisStageMetadataImpl stageMeta = (MantisStageMetadataImpl) stageMetaOp.get();
                try {
                    // Delegate processing of the event to the stage
                    Optional<JobWorker> workerOp = stageMeta.processWorkerEvent(event, jobStore);
                    if (!workerOp.isPresent()) {
                        terminateUnknownWorkerIfNonTerminal(event);
                        return;
                    }

                    IMantisWorkerMetadata wm = workerOp.get().getMetadata();
                    // If we need to migrate off of disabled VM add it to the queue
                    if (event instanceof WorkerOnDisabledVM) {
                        workersToMigrate.add(wm.getWorkerNumber());
                        return;
                    }

                    // Worker transitioned to terminal state resubmit
                    if (WorkerState.isErrorState(wm.getState()) && !JobState.isTerminalState(jobState)) {
                        eventPublisher.publishStatusEvent(new LifecycleEventsProto.WorkerStatusEvent(WARN,
                                "resubmitting lost worker ", wm.getStageNum(),
                                wm.getWorkerId(), wm.getState()));

                        resubmitWorker(workerOp.get());
                        return;
                    } else if (WorkerState.isTerminalState(wm.getState())) { // worker has explicitly
                        // completed complete job
                        jobStore.archiveWorker(wm);
                        LOGGER.info("Received Worker Complete signal. Wait for all workers to complete before "
                                + "terminating Job {}", jobId);
                    }

                    if (!(event instanceof WorkerHeartbeat)) {
                        markStageAssignmentsChanged(false);
                    }

                } catch (Exception e) {
                    LOGGER.warn("Exception saving worker update", e);
                }

                if (!allWorkersStarted && !JobState.isTerminalState(jobState)) {
                    if (allWorkerStarted()) {
                        allWorkersStarted = true;
                        jobMgr.onAllWorkersStarted();
                        markStageAssignmentsChanged(true);
                    } else if (allWorkerCompleted()) {
                        LOGGER.info("Job {} All workers completed1", jobId);
                        allWorkersStarted = false;
                        jobMgr.onAllWorkersCompleted();
                    }
                } else {
                    if (allWorkerCompleted()) {
                        LOGGER.info("Job {} All workers completed", jobId);
                        allWorkersStarted = false;
                        jobMgr.onAllWorkersCompleted();
                    }
                }


            } catch (Exception e1) {
                e1.printStackTrace();
                LOGGER.error("Job {} Exception occurred in process worker event ", jobId, e1);
            }

        }

        private boolean allWorkerStarted() {

            Iterator<? extends IMantisStageMetadata> iterator =
                    mantisJobMetaData.getStageMetadata().values().iterator();
            while (iterator.hasNext()) {
                MantisStageMetadataImpl stageMeta = (MantisStageMetadataImpl) iterator.next();
                if (!stageMeta.isAllWorkerStarted()) {
                    return false;
                }
            }
            return true;
        }

        private int getNumberOfWorkersInStartedState() {

            return mantisJobMetaData.getStageMetadata().values().stream()
                    .map((stageMeta) -> ((MantisStageMetadataImpl) stageMeta).getNumStartedWorkers())
                    .reduce(0, (acc, num) -> acc + num);

        }

        private int getTotalWorkerCount() {

            return mantisJobMetaData.getStageMetadata().values().stream()
                    .map(IMantisStageMetadata::getNumWorkers)
                    .reduce(0, (acc, num) -> acc + num);
        }

        private boolean allWorkerCompleted() {
            Iterator<? extends IMantisStageMetadata> iterator =
                    mantisJobMetaData.getStageMetadata().values().iterator();
            while (iterator.hasNext()) {
                MantisStageMetadataImpl stageMeta = (MantisStageMetadataImpl) iterator.next();
                // skip job master worker
                if (stageMeta.getStageNum() == 0) {
                    continue;
                }
                if (!stageMeta.isAllWorkerCompleted()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void resubmitWorker(int workerNum) throws Exception {
            Map<Integer, Integer> workerToStageMap = mantisJobMetaData.getWorkerNumberToStageMap();
            if (workerToStageMap.containsKey(workerNum)) {
                int stageNum = workerToStageMap.get(workerNum);
                Optional<IMantisStageMetadata> stageMeta = mantisJobMetaData.getStageMetadata(stageNum);
                if (stageMeta.isPresent()) {
                    JobWorker worker = stageMeta.get().getWorkerByWorkerNumber(workerNum);
                    resubmitWorker(worker);
                } else {
                    throw new Exception(String.format("Invalid stage {} in resubmit Worker request {}", stageNum,
                            workerNum));
                }
            } else {
                LOGGER.warn("No such Worker number {} in Job with ID {}", workerNum, jobId);
                throw new Exception(String.format("No such worker number {} in resubmit Worker request", workerNum));
            }
        }

        @Override
        public List<IMantisWorkerMetadata> getActiveWorkers(int limit) {
            List<IMantisWorkerMetadata> workers = mantisJobMetaData.getStageMetadata().values()
                    .stream()
                    .flatMap((st) -> st.getAllWorkers().stream())
                    .filter((worker) -> !WorkerState.isTerminalState(worker.getMetadata().getState()))
                    .map(JobWorker::getMetadata)
                    .collect(Collectors.toList());

            if (workers.size() > limit) {
                return workers.subList(0, limit);
            } else {
                return workers;
            }
        }

        @Override
        public BehaviorSubject<JobSchedulingInfo> getJobStatusSubject() {
            return this.jobSchedulingInfoBehaviorSubject;
        }


        private void resubmitWorker(JobWorker oldWorker) throws Exception {
            LOGGER.info("Resubmitting worker {}", oldWorker.getMetadata());
            Map<Integer, Integer> workerToStageMap = mantisJobMetaData.getWorkerNumberToStageMap();

            IMantisWorkerMetadata oldWorkerMetadata = oldWorker.getMetadata();
            if (oldWorkerMetadata.getTotalResubmitCount()
                    < ConfigurationProvider.getConfig().getMaximumResubmissionsPerWorker()) {

                Integer stageNo = workerToStageMap.get(oldWorkerMetadata.getWorkerId().getWorkerNum());
                if (stageNo == null) {
                    String errMsg = String.format("Stage {} not found in Job {} while resubmiting worker {}",
                            stageNo, jobId, oldWorker);
                    LOGGER.warn(errMsg);
                    throw new Exception(errMsg);

                }
                Optional<IMantisStageMetadata> stageMetaOp = mantisJobMetaData.getStageMetadata(stageNo);
                if (!stageMetaOp.isPresent()) {
                    String errMsg = String.format("Stage {} not found in Job {} while resubmiting worker {}",
                            stageNo, jobId, oldWorker);
                    LOGGER.warn(errMsg);
                    throw new Exception(errMsg);

                }

                MantisStageMetadataImpl stageMeta = (MantisStageMetadataImpl) stageMetaOp.get();

                JobWorker newWorker = new JobWorker.Builder()
                        .withJobId(jobId)
                        .withWorkerIndex(oldWorkerMetadata.getWorkerIndex())
                        .withWorkerNumber(workerNumberGenerator.getNextWorkerNumber(mantisJobMetaData, jobStore))
                        .withNumberOfPorts(stageMeta.getMachineDefinition().getNumPorts()
                                + MANTIS_SYSTEM_ALLOCATED_NUM_PORTS)
                        .withStageNum(oldWorkerMetadata.getStageNum())
                        .withResubmitCount(oldWorkerMetadata.getTotalResubmitCount() + 1)
                        .withResubmitOf(oldWorkerMetadata.getWorkerNumber())
                        .withLifecycleEventsPublisher(eventPublisher)
                        .build();

                mantisJobMetaData.replaceWorkerMetaData(oldWorkerMetadata.getStageNum(), newWorker, oldWorker,
                        jobStore);

                // kill the task if it is still running
                scheduler.unscheduleAndTerminateWorker(oldWorkerMetadata.getWorkerId(),
                        Optional.ofNullable(oldWorkerMetadata.getSlave()));

                long workerResubmitTime = resubmitRateLimiter.getWorkerResubmitTime(
                        newWorker.getMetadata().getWorkerId(), stageMeta.getStageNum());
                Optional<Long> delayDuration = of(workerResubmitTime);
                // publish a refresh before enqueuing new Task to Scheduler
                markStageAssignmentsChanged(true);
                // queue the new worker for execution
                queueTask(newWorker.getMetadata(), delayDuration);
                LOGGER.info("Worker {} successfully queued for scheduling", newWorker);
                numWorkerResubmissions.increment();
            } else {

                // todo numWorkerResubmitLimitReached.increment();
                LOGGER.error("Resubmit count exceeded");
                jobMgr.onTooManyWorkerResubmits();
            }
        }

        /**
         * Preconditions : Stage is Valid and scalable
         * Determines the actual no of workers for this stage within min and max, updates the expected num workers
         * first and saves to store.
         * (If that fails we abort the operation)
         * then continues adding/terminating worker one by one.
         * If an exception occurs adding/removing any worker we continue forward with others.
         * Heartbeat check should kick in and resubmit any workers that didn't get scheduled
         */
        @Override
        public int scaleStage(MantisStageMetadataImpl stageMetaData, int numWorkers, String reason) {
            LOGGER.info("Scaling stage {} to {} workers", stageMetaData.getStageNum(), numWorkers);
            final int oldNumWorkers = stageMetaData.getNumWorkers();
            int max = ConfigurationProvider.getConfig().getMaxWorkersPerStage();
            int min = 0;
            if (stageMetaData.getScalingPolicy() != null) {
                max = stageMetaData.getScalingPolicy().getMax();
                min = stageMetaData.getScalingPolicy().getMin();
            }
            // sanitize input worker count to be between min and max
            int newNumWorkerCount = Math.max(Math.min(numWorkers, max), min);
            if (newNumWorkerCount != oldNumWorkers) {
                try {
                    stageMetaData.unsafeSetNumWorkers(newNumWorkerCount, jobStore);
                    eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(INFO,
                            "Setting #workers to " + newNumWorkerCount + " for stage "
                                    + stageMetaData.getStageNum() + ", reason=" + reason, getJobId(), getJobState()));
                } catch (Exception e) {
                    String error = String.format("Exception updating stage {} worker count for Job {} due to {}",
                            stageMetaData.getStageNum(), jobId, e.getMessage());
                    LOGGER.warn(error);
                    eventPublisher.publishStatusEvent(new LifecycleEventsProto.JobStatusEvent(WARN,
                            "Scaling stage failed for stage " + stageMetaData.getStageNum() + " reason: "
                                    + e.getMessage(),
                            getJobId(), getJobState()));
                    throw new RuntimeException(error);
                }
                if (newNumWorkerCount > oldNumWorkers) {
                    for (int i = 0; i < newNumWorkerCount - oldNumWorkers; i++) {
                        try {
                            int newWorkerIndex = oldNumWorkers + i;
                            SchedulingInfo schedInfo = mantisJobMetaData.getJobDefinition().getSchedulingInfo();
                            IMantisWorkerMetadata workerRequest = addWorker(schedInfo, stageMetaData.getStageNum(),
                                    newWorkerIndex);
                            jobStore.storeNewWorker(workerRequest);
                            markStageAssignmentsChanged(true);
                            queueTask(workerRequest);

                        } catch (Exception e) {
                            // creating a worker failed but expected no of workers was set successfully,
                            // during heartbeat check we will
                            // retry launching this worker
                            LOGGER.warn("Exception adding new worker for {}", stageMetaData.getJobId().getId(), e);
                        }
                    }
                } else {
                    //  potential bulk removal opportunity?
                    for (int i = 0; i < oldNumWorkers - newNumWorkerCount; i++) {
                        try {
                            final JobWorker w = stageMetaData.getWorkerByIndex(oldNumWorkers - i - 1);
                            terminateAndRemoveWorker(w.getMetadata(), WorkerState.Completed, JobCompletedReason.Killed);
                        } catch (InvalidJobException e) {
                            // deleting a worker failed but expected no of workers was set successfully,
                            // during heartbeat check we will
                            // retry killing this worker
                            LOGGER.warn("Exception terminating worker for {}", stageMetaData.getJobId().getId(), e);
                        }
                    }
                }
            }
            LOGGER.info("{} Scaled stage to {} workers", stageMetaData.getJobId().getId(), newNumWorkerCount);
            return newNumWorkerCount;
        }
    }


}
