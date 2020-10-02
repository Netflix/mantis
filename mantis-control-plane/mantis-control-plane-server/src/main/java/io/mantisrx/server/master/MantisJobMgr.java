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

package io.mantisrx.server.master;

public class MantisJobMgr {

}
//
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.ConcurrentSkipListSet;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.concurrent.atomic.AtomicReference;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//import com.netflix.fenzo.ConstraintEvaluator;
//import com.netflix.fenzo.VMTaskFitnessCalculator;
//import com.netflix.spectator.api.BasicTag;
//import io.mantisrx.common.WorkerPorts;
//import io.mantisrx.common.metrics.Counter;
//import io.mantisrx.common.metrics.Metrics;
//import io.mantisrx.common.metrics.MetricsRegistry;
//import io.mantisrx.common.metrics.spectator.GaugeCallback;
//import io.mantisrx.common.metrics.spectator.MetricGroupId;
////import io.mantisrx.master.jobcluster.job.JobActor;
////import io.mantisrx.master.jobcluster.job.WorkerResubmitRateLimiter;
//import io.mantisrx.runtime.JobConstraints;
//import io.mantisrx.runtime.MachineDefinition;
//import io.mantisrx.runtime.MantisJobDefinition;
//import io.mantisrx.runtime.MantisJobDurationType;
//import io.mantisrx.runtime.MantisJobState;
//import io.mantisrx.runtime.MigrationStrategy;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import io.mantisrx.runtime.descriptor.StageScalingPolicy;
//import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
//import io.mantisrx.server.core.JobCompletedReason;
//import io.mantisrx.server.core.JobSchedulingInfo;
//import io.mantisrx.server.core.Status;
//import io.mantisrx.server.core.StatusPayloads;
//import io.mantisrx.server.core.WorkerAssignments;
//import io.mantisrx.server.core.WorkerHost;
//import io.mantisrx.server.core.domain.JobMetadata;
//import io.mantisrx.server.core.domain.WorkerId;
//import io.mantisrx.server.master.agentdeploy.MigrationStrategyFactory;
//import io.mantisrx.server.master.config.ConfigurationProvider;
//import io.mantisrx.server.master.config.MasterConfiguration;
//import io.mantisrx.server.master.domain.WorkerRequest;
//import io.mantisrx.server.master.heartbeathandlers.HeartbeatPayloadHandler;
//import io.mantisrx.server.master.scheduler.MantisScheduler;
//import io.mantisrx.server.master.scheduler.ScheduleRequest;
//import io.mantisrx.server.master.store.InvalidJobException;
//import io.mantisrx.server.master.store.InvalidJobStateChangeException;
//import io.mantisrx.server.master.store.JobAlreadyExistsException;
//import io.mantisrx.server.master.store.MantisJobMetadata;
//import io.mantisrx.server.master.store.MantisJobMetadataWritable;
//import io.mantisrx.server.master.store.MantisJobStore;
//import io.mantisrx.server.master.store.MantisStageMetadata;
//import io.mantisrx.server.master.store.MantisStageMetadataWritable;
//import io.mantisrx.server.master.store.MantisWorkerMetadata;
//import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
//import io.mantisrx.server.master.store.NamedJob;
//import io.mantisrx.server.master.utils.MantisClock;
//import io.mantisrx.server.master.utils.MantisSystemClock;
//import io.reactivx.mantis.operators.OperatorOnErrorResumeNextViaFunction;
//import org.HdrHistogram.SynchronizedHistogram;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observable;
//import rx.functions.Action0;
//import rx.functions.Func1;
//import rx.observers.SerializedObserver;
//import rx.schedulers.Schedulers;
//import rx.subjects.BehaviorSubject;
//import rx.subjects.ReplaySubject;
//
//
///**
// * Manages operational and informational aspects of a Mantis Job.
// * It has at least the following responsibilities:
// * <UL>
// * <LI>Submit initial stages and workers needed for the job</LI>
// * <LI>React to status reports on workers</LI>
// * <LI>React to health reports on workers (in future)</LI>
// * </UL>
// * Also contained herein are job specific information such as status subject for the job.
// */
//public class MantisJobMgr {
//
//    public static class HeartbeatsStatus {
//
//        private final int totalWorkers;
//        private final int heartbeatsFrom;
//
//        public HeartbeatsStatus(int totalWorkers, int heartbeatsFrom) {
//            this.totalWorkers = totalWorkers;
//            // if job scaled down, total could be < heartbeatsFrom
//            this.heartbeatsFrom = Math.min(heartbeatsFrom, totalWorkers);
//        }
//
//        public int getTotalWorkers() {
//            return totalWorkers;
//        }
//
//        public int getHeartbeatsFrom() {
//            return heartbeatsFrom;
//        }
//    }
//
//    private static final ExecutorService mantisJobMgrExecutorService = Executors.newFixedThreadPool(10,
//            new ThreadFactoryBuilder().setNameFormat("MantisJobMgr-pool-%d").build());
//
//    private static final Logger logger = LoggerFactory.getLogger(MantisJobMgr.class);
//    private static final long DISABLE_FOR_MILIS = 60000;
//    private final String jobId;
//    private final NamedJob namedJob;
//    private final MantisScheduler scheduler;
//    private final MantisJobDefinition jobDefinition;
//    private final ReplaySubject<Status> statusReplaySubject;
//    private final SerializedObserver<Status> statusSerializedObserver;
//    private MantisJobStatus jobStatus;
//    private final AtomicBoolean initialized;
//    private volatile MantisJobStore store = null;
//    private final JobActor.WorkerNumberGenerator workerNumberGenerator;
//    private final VirtualMachineMasterService vmService;
//    private final BehaviorSubject<JobSchedulingInfo> schedulingInfoBehaviorSubject;
//    private Map<Integer, WorkerAssignments> stageAssignments = new HashMap<>();
//    private final ObjectMapper mapper = new ObjectMapper();
//    private final Counter numWorkerResubmissions;
//    private final Counter numWorkerResubmitLimitReached;
//    private final Counter numWorkerTerminated;
//    private final Counter numScaleStage;
//    private final Counter workerLaunchToStartMillis;
//    private final Counter numHeartBeatsReceived;
//    private final WorkerResubmitRateLimiter resubmitRateLimiter = WorkerResubmitRateLimiter.getInstance();
//    private final ConcurrentSkipListSet<Integer> workersOnDisabledVMs = new ConcurrentSkipListSet<>();
//    private final AtomicLong lastNewSubscriberAt = new AtomicLong(System.currentTimeMillis());
//    private long subscriptionTimeoutSecs = 0L;
//    private final AtomicReference<ArrayList<MantisWorkerMetadata>> heartbeatReceipts = new AtomicReference<>(new ArrayList<MantisWorkerMetadata>());
//    private final MetricGroupId metricsName;
//    private final int workerWritesBatchSize;
//    private boolean hasJobMaster = false;
//    private final MigrationStrategy migrationStrategy;
//    private final MantisClock clock = MantisSystemClock.INSTANCE;
//    private volatile long lastWorkerMigrationTimestamp = Long.MIN_VALUE;
//    private final SynchronizedHistogram workerLaunchToStartDistMillis = new SynchronizedHistogram(3_600_000L, 3);
//
//    public MantisJobMgr(final String jobId,
//                        final MantisJobDefinition jobDefinition,
//                        final NamedJob namedJob,
//                        final MantisScheduler scheduler,
//                        final VirtualMachineMasterService vmService) {
//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        mapper.registerModule(new Jdk8Module());
//        this.jobId = jobId;
//        if (namedJob == null)
//            throw new NullPointerException("Job cluster does not exist for job " + jobId);
//        this.namedJob = namedJob;
//        this.scheduler = scheduler;
//        this.vmService = vmService;
//        this.jobDefinition = jobDefinition;
//        final String jobIdValue = Optional.ofNullable(jobId).orElse("none");
//        metricsName = new MetricGroupId(MantisJobMgr.class.getCanonicalName(), new BasicTag("jobId", jobIdValue));
//        Metrics m = new Metrics.Builder()
//                .id(metricsName)
//                .addCounter("numWorkerResubmissions")
//                .addCounter("numWorkerResubmitLimitReached")
//                .addCounter("numWorkerTerminated")
//                .addCounter("numEphemeralJobTerminated")
//                .addCounter("numScaleStage")
//                .addCounter("workerLaunchToStartMillis")
//                .addCounter("numHeartBeatsReceived")
//                .addGauge(new GaugeCallback(metricsName, "workerLaunchToStartMillisP50", () -> (double) workerLaunchToStartDistMillis.getValueAtPercentile(50)))
//                .addGauge(new GaugeCallback(metricsName, "workerLaunchToStartMillisP95", () -> (double) workerLaunchToStartDistMillis.getValueAtPercentile(95)))
//                .addGauge(new GaugeCallback(metricsName, "workerLaunchToStartMillisP99", () -> (double) workerLaunchToStartDistMillis.getValueAtPercentile(99)))
//                .addGauge(new GaugeCallback(metricsName, "workerLaunchToStartMillisMax", () -> (double) workerLaunchToStartDistMillis.getValueAtPercentile(100)))
//                .build();
//        m = MetricsRegistry.getInstance().registerAndGet(m);
//        numWorkerResubmissions = m.getCounter("numWorkerResubmissions");
//        numWorkerResubmitLimitReached = m.getCounter("numWorkerResubmitLimitReached");
//        numWorkerTerminated = m.getCounter("numWorkerTerminated");
//        numScaleStage = m.getCounter("numScaleStage");
//        workerLaunchToStartMillis = m.getCounter("workerLaunchToStartMillis");
//        numHeartBeatsReceived = m.getCounter("numHeartBeatsReceived");
//        statusReplaySubject = ReplaySubject.create();
//        this.statusSerializedObserver = new SerializedObserver<>(statusReplaySubject);
//        jobStatus = new MantisJobStatus(jobId, statusReplaySubject, (jobDefinition == null ? "null job" : jobDefinition.getName()));
//
//        schedulingInfoBehaviorSubject = BehaviorSubject.create(new JobSchedulingInfo(jobId, new HashMap<>()));
//        logger.debug("Added behavior subject of job " + jobId + " to schedulingObserver");
//        statusReplaySubject
//                .lift(new OperatorOnErrorResumeNextViaFunction<>(throwable -> {
//                    logger.warn("Couldn't send status to assignmentChangeObservers: " + throwable.getMessage());
//                    return Observable.empty();
//                }))
//                .subscribe(status -> stageAssignmentsSetter(status));
//        initialized = new AtomicBoolean(false);
//        workerNumberGenerator = new WorkerNumberGenerator(jobId);
//        workerWritesBatchSize = ConfigurationProvider.getConfig().getWorkerWriteBatchSize();
//        migrationStrategy = MigrationStrategyFactory.getStrategy(jobId, namedJob.getMigrationConfig());
//    }
//
//    public BehaviorSubject<JobSchedulingInfo> getSchedulingInfoSubject() {
//        return schedulingInfoBehaviorSubject;
//    }
//
//    public String getJobId() {
//        return jobId;
//    }
//
//    boolean isReady() {
//        if (!initialized.get())
//            logger.info("Job " + jobId + " not ready to be dispatched");
//        return initialized.get();
//    }
//
//    public MantisJobMetadata getJobMetadata() {
//        return store.getActiveJob(jobId);
//    }
//
//    public Optional<MantisJobMetadata> getCompletedJobMetadata() {
//        try {
//            return Optional.ofNullable(store.getCompletedJob(jobId));
//        } catch (Exception e) {
//            return Optional.empty();
//        }
//    }
//
//    public MantisJobDefinition getJobDefinition() {
//        return jobDefinition;
//    }
//
//    public ReplaySubject<Status> getStatusSubject() {
//        return statusReplaySubject;
//    }
//
//    public void setInvalidStatus(String msg) {
//        jobStatus.setFatalError(msg);
//    }
//
//    public MantisJobStatus getJobStatus() {
//        return jobStatus;
//    }
//
//    public List<? extends MantisWorkerMetadata> getArchivedWorkers() {
//        try {
//            return store.getArchivedWorkers(jobId);
//        } catch (IOException e) {
//            logger.error("Can't get archived workers - " + e.getMessage(), e);
//        }
//        return new ArrayList<>();
//    }
//
//    public WorkerAssignments getSink() {
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        return stageAssignments.get(mjmd.getNumStages());
//    }
//
//    private void stageAssignmentsSetter(Status status) {
//        MantisJobMetadataWritable jobMetadata = (MantisJobMetadataWritable) getJobMetadata();
//        if (MantisJobState.isTerminalState(jobMetadata.getState())) {
//            schedulingInfoBehaviorSubject.onCompleted();
//            return;
//        }
//        if (status.getWorkerNumber() == -1)
//            return;
//        MantisWorkerMetadata mwmd = null;
//        MantisStageMetadata msmd = null;
//        try {
//            mwmd = getMantisWorkerMetadataWritable(status.getWorkerNumber());
//            msmd = jobMetadata.getStageMetadata(mwmd.getStageNum());
//        } catch (InvalidJobException e) {
//            try {
//                mwmd = getArchivedWorker(status.getWorkerNumber());
//                if (mwmd == null) {
//                    logger.warn("Unexpected to not find worker " + status.getWorkerNumber() + " for job " + jobId);
//                } else {
//                    logger.warn("Skipping setting assignments for archived worker");
//                }
//            } catch (IOException e1) {
//                logger.warn("Can't get archive worker number " + status.getWorkerNumber() + " for job " + jobId, e1);
//            }
//            return;
//        }
//        if (mwmd.getStageNum() < 1)
//            return; // for now don't send assignments for stage 0
//        status.setStageNum(mwmd.getStageNum());
//        status.setWorkerIndex(mwmd.getWorkerIndex());
//
//        WorkerAssignments assignments = stageAssignments.get(status.getStageNum());
//        if (assignments == null) {
//            assignments = new WorkerAssignments(status.getStageNum(), msmd.getNumWorkers(), new HashMap<Integer, WorkerHost>());
//            stageAssignments.put(status.getStageNum(), assignments);
//        }
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            assignments.setNumWorkers(msmd.getNumWorkers());
//            assignments.setActiveWorkers(((MantisStageMetadataWritable) msmd).getNumActiveWorkers());
//        } catch (Exception e) {
//            logger.warn(jobId + ": Unexpected error locking stage " + msmd.getStageNum() + ": " + e.getMessage());
//        }
//        Map<Integer, WorkerHost> hosts = assignments.getHosts();
//        hosts.put(mwmd.getWorkerNumber(), new WorkerHost(mwmd.getSlave(), mwmd.getWorkerIndex(), mwmd.getPorts(),
//                status.getState(), mwmd.getWorkerNumber(), mwmd.getMetricsPort(), mwmd.getCustomPort()));
//
//        //logger.info("Sending scheduling change to behavior subject due to " + status);
//        if (schedulingInfoBehaviorSubject != null) {
//            schedulingInfoBehaviorSubject.onNext(new JobSchedulingInfo(jobId, new HashMap<>(stageAssignments)));
//        }
//        // remove from WorkerHost any workers that are in terminal state, notification went out for them once.
//        Set<Integer> keysToRem = new HashSet<>();
//        for (Map.Entry<Integer, WorkerHost> entry : assignments.getHosts().entrySet()) {
//            if (MantisJobState.isTerminalState(entry.getValue().getState()))
//                keysToRem.add(entry.getKey());
//        }
//        for (Integer k : keysToRem)
//            assignments.getHosts().remove(k);
//    }
//
//    private List<? extends Object> getAllWorkers(Func1<MantisWorkerMetadata, Boolean> filter,
//                                                 Func1<MantisWorkerMetadata, ? extends Object> mapFunc) {
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        List<Object> result = new ArrayList<>();
//        for (MantisStageMetadata msmd : mjmd.getStageMetadata())
//            for (MantisWorkerMetadata mwmd : msmd.getAllWorkers())
//                if (filter.call(mwmd))
//                    result.add(mapFunc.call(mwmd));
//        return result;
//    }
//
//    @SuppressWarnings("unchecked")
//    public List<MantisWorkerMetadata> getAllRunningWorkers() {
//        List<MantisWorkerMetadata> result = (List<MantisWorkerMetadata>) getAllWorkers(
//                new Func1<MantisWorkerMetadata, Boolean>() {
//                    @Override
//                    public Boolean call(MantisWorkerMetadata mwmd) {
//                        return MantisJobState.isRunningState(mwmd.getState());
//                    }
//                },
//                new Func1<MantisWorkerMetadata, Object>() {
//                    @Override
//                    public Object call(MantisWorkerMetadata workerMetadata) {
//                        return workerMetadata;
//                    }
//                });
//        return result;
//    }
//
//    private boolean allWorkersStarted() {
//        for (MantisStageMetadata msmd : getJobMetadata().getStageMetadata()) {
//            for (MantisWorkerMetadata mwmd : msmd.getWorkerByIndexMetadataSet())
//                if (mwmd.getState() != MantisJobState.Started)
//                    return false;
//        }
//        return true;
//    }
//
//    private boolean heartbeatTooOld(MantisWorkerMetadata mwmd) {
//        if (((MantisWorkerMetadataWritable) mwmd).getLastHeartbeatAt() <
//                (System.currentTimeMillis() - (3 * ConfigurationProvider.getConfig().getWorkerTimeoutSecs() * 1000)))
//            return true;
//        return false;
//    }
//
//    private void scaleStages(MantisJobMetadata mjmd) {
//        if (!initialized.get())
//            return; // not ready yet
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        if (MantisJobState.isTerminalState(jobMetadata.getState()))
//            return;
//        // ensure that each stage has the right number of workers
//        // For now, the number of workers per stage is static (no auto scaling), just make sure there is a worker
//        // for each worker index.
//        boolean killTheJob = false;
//        String killMsg = "";
//        for (MantisStageMetadata msmd : mjmd.getStageMetadata()) {
//            if (!killTheJob) {
//                try (AutoCloseable l = jobMetadata.obtainLock()) {
//                    for (int wIndex = 0; wIndex < msmd.getNumWorkers(); wIndex++) {
//                        boolean hasWorker = true;
//                        MantisWorkerMetadata terminatedWorker = null;
//                        try {
//                            terminatedWorker = mjmd.getWorkerByIndex(msmd.getStageNum(), wIndex);
//                            if (MantisJobState.isErrorState(terminatedWorker.getState()))
//                                hasWorker = false;
//                            if (terminatedWorker.getState() == MantisJobState.Completed) {
//                                // worker complete, tear down job
//                                killMsg = "Terminating job " + jobId + " due to worker # " + terminatedWorker.getWorkerNumber() +
//                                        " index " + terminatedWorker.getWorkerIndex() + " of stage " + terminatedWorker.getStageNum() + " completed";
//                                killTheJob = true;
//                                break;
//                            }
//                        } catch (InvalidJobException e) {
//                            hasWorker = false;
//                        }
//                        if (!hasWorker) {
//                            // There isn't a worker for this index
//                            int numPorts = msmd.getMachineDefinition().getNumPorts();
//                            try {
//                                int workerNumber = workerNumberGenerator.getNextWorkerNumber();
//                                logger.info("creating non-existing worker for index " + wIndex + " workerNumber=" +
//                                        workerNumber + " for job " + jobId);
//                                final WorkerRequest request;
//                                if (terminatedWorker == null) {
//                                    logger.info("storing new worker " + workerNumber + " for index " + wIndex + " of job " + jobId);
//                                    request = createWorkerRequest(msmd, wIndex, mjmd.getUser(), Optional.empty());
//                                    store.storeNewWorker(request);
//                                } else {
//                                    logger.info("replacing worker " + terminatedWorker.getWorkerNumber() + " with new worker " +
//                                            workerNumber + " for index " + wIndex + " of job " + jobId);
//                                    request = createWorkerRequest(msmd, wIndex, mjmd.getUser(), terminatedWorker.getCluster());
//                                    final MantisWorkerMetadata mwmd = store.replaceTerminatedWorker(request, terminatedWorker);
//                                    logger.info("Done replacing worker " + terminatedWorker.getWorkerNumber() + " with new worker " +
//                                            mwmd.getWorkerNumber() + " for index " + wIndex + " of job " + jobId);
//                                }
//                                queueTask(request);
//                            } catch (Exception e1) {
//                                logger.error("Can't store new worker for stage " + msmd.getStageNum() + ", workerIndex=" +
//                                        wIndex + " - " + e1.getMessage(), e1);
//                            }
//                        }
//                    }
//                } catch (Exception e) {
//                    logger.error("caught unexpected error ", e);
//                } // shouldn't happen
//            }
//        }
//        if (killTheJob) {
//            killJob("MantisMaster", killMsg);
//        }
//    }
//
//    private WorkerRequest createWorkerRequest(final MantisStageMetadata msmd,
//                                              final int index,
//                                              final String user,
//                                              final Optional<String> preferredCluster) {
//        int workerNumber = workerNumberGenerator.getNextWorkerNumber();
//        MantisJobMetadata mjmd = getJobMetadata();
//        return new WorkerRequest(msmd.getMachineDefinition(), jobId, index, workerNumber,
//                mjmd.getJarUrl(), msmd.getStageNum(), msmd.getNumStages(), msmd.getNumWorkers(),
//                mjmd.getName(), msmd.getMachineDefinition().getNumPorts(), mjmd.getParameters(), mjmd.getSla(),
//                msmd.getHardConstraints(), msmd.getSoftConstraints(), jobDefinition.getSchedulingInfo(),
//                subscriptionTimeoutSecs, getMinRuntimeSecs(), mjmd.getSubmittedAt(), user, preferredCluster);
//    }
//
//    private WorkerRequest createExistingWorkerRequest(final MantisStageMetadata msmd,
//                                                      final int index,
//                                                      final int workerNumber,
//                                                      final String user,
//                                                      final Optional<String> preferredCluster) {
//        MantisJobMetadata mjmd = getJobMetadata();
//        return new WorkerRequest(msmd.getMachineDefinition(), jobId, index, workerNumber,
//                mjmd.getJarUrl(), msmd.getStageNum(), msmd.getNumStages(), msmd.getNumWorkers(),
//                mjmd.getName(), msmd.getMachineDefinition().getNumPorts(), mjmd.getParameters(), mjmd.getSla(),
//                msmd.getHardConstraints(), msmd.getSoftConstraints(), jobDefinition.getSchedulingInfo(),
//                subscriptionTimeoutSecs, getMinRuntimeSecs(), mjmd.getSubmittedAt(), user, preferredCluster);
//    }
//
//    public int scaleUpStage(int stage, int increment, String reason) throws InvalidJobException {
//        if (!isActive() || increment < 1)
//            return 0;
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        final MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) jobMetadata.getStageMetadata(stage);
//        if (msmd == null)
//            throw new InvalidJobException(jobId, stage, -1);
//        if (!msmd.getScalingPolicy().isEnabled()) {
//            logger.warn("Job " + jobId + " stage " + stage + " is not scalable, can't increment #workers by " + increment);
//            return 0;
//        }
//        int actualIncrement = 0;
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            final int oldNumWorkers = msmd.getNumWorkers();
//            final int max = msmd.getScalingPolicy().getMax();
//            actualIncrement = Math.min(oldNumWorkers + increment, max) - oldNumWorkers;
//            if (actualIncrement > 0) {
//                msmd.unsafeSetNumWorkers(oldNumWorkers + increment);
//                try {
//                    store.updateStage(msmd);
//                } catch (IOException e) {
//                    logger.error("Error setting stage count for job " + jobId);
//                    msmd.unsafeSetNumWorkers(oldNumWorkers);
//                    return 0;
//                }
//                for (int i = 0; i < actualIncrement; i++) {
//                    int newWorkerIndex = oldNumWorkers + i;
//                    final WorkerRequest workerRequest = createWorkerRequest(msmd, newWorkerIndex,
//                            jobMetadata.getUser(), Optional.empty());
//                    MantisWorkerMetadata workerByIndex = null;
//                    try {
//                        workerByIndex = jobMetadata.getWorkerByIndex(msmd.getStageNum(), newWorkerIndex);
//                    } catch (InvalidJobException e) {} // worker for that index didn't exist before
//                    if (workerByIndex == null) {
//                        store.storeNewWorker(workerRequest);
//                    } else {
//                        store.replaceTerminatedWorker(workerRequest, workerByIndex);
//                    }
//                    queueTask(workerRequest);
//                    //                    if(workerByIndex==null)
//                    //                        workerByIndex = mjmd.getWorkerByIndex(msmd.getStageNum(), i);
//                    //                    logger.info("Worker for index " + i + " is " + workerByIndex.getWorkerNumber());
//                }
//            }
//        } catch (IOException | InvalidJobException e) {
//            logger.error("Couldn't create new worker for scaling up: " + e.getMessage(), e);
//        } catch (Exception e) {
//            logger.error("Unexpected error: " + e.getMessage(), e);
//        } // shouldn't happen
//        if (actualIncrement > 0) {
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(
//                    MantisAuditLogEvent.Type.JOB_SCALE_UP, jobId, "Scaled up (auto) stage " + stage + " by " + actualIncrement + " workers"));
//            String mesg = "Scaled up (auto) stage " + stage + " of job " + jobId + " by " + actualIncrement + " workers to " +
//                    msmd.getNumWorkers() + ", reason: " + reason;
//            sendStatus(new Status(jobId, stage, -1, -1, Status.TYPE.INFO, mesg, MantisJobState.Started));
//            logger.info(mesg);
//        }
//        return actualIncrement;
//    }
//
//    public int scaleDownStage(int stage, int decrement, String reason) throws InvalidJobException {
//        if (!isActive() || decrement < 1)
//            return 0;
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        final MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) jobMetadata.getStageMetadata(stage);
//        if (msmd == null)
//            throw new InvalidJobException(jobId, stage, -1);
//        if (!msmd.getScalingPolicy().isEnabled()) {
//            logger.warn("Job " + jobId + " stage " + stage + " is not scalable, can't increment #workers by " + decrement);
//            return 0;
//        }
//        int actualDecrement = 0;
//        StringBuilder sb = new StringBuilder();
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            int oldNumWorkers = msmd.getNumWorkers();
//            int min = msmd.getScalingPolicy().getMin();
//            actualDecrement = oldNumWorkers - Math.max(oldNumWorkers - decrement, min);
//            if (actualDecrement > 0) {
//                msmd.unsafeSetNumWorkers(oldNumWorkers - decrement);
//                try {
//                    store.updateStage(msmd);
//                } catch (IOException e) {
//                    logger.error("Error setting stage count for job " + jobId);
//                    msmd.unsafeSetNumWorkers(oldNumWorkers);
//                    return 0;
//                }
//                for (int w = 0; w < actualDecrement; w++) {
//                    final MantisWorkerMetadata r = msmd.getWorkerByIndex(oldNumWorkers - w - 1);
//                    killWorker(r);
//                    store.archiveWorker((MantisWorkerMetadataWritable) r);
//                    if (!msmd.unsafeRemoveWorker(r.getWorkerIndex(), r.getWorkerNumber())) {
//                        logger.warn(String.format("Job %s stage %d: Unexpected worker state, couldn't remove worker idx=%d, number=%d",
//                                jobId, msmd.getStageNum(), r.getWorkerIndex(), r.getWorkerNumber()));
//                    }
//                    logger.info(String.format("Job %s archived worker index %d, number %d", jobId,
//                            r.getWorkerIndex(), r.getWorkerNumber()));
//                    sb.append(r.getWorkerIndex()).append(",");
//                }
//            }
//        } catch (Exception e) {
//            logger.warn("Unexpected error scaling down stage " + stage + " of job " + jobId + " by " +
//                    decrement + " workers: " + e.getMessage());
//        }
//        if (actualDecrement > 0) {
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(
//                    MantisAuditLogEvent.Type.JOB_SCALE_DOWN, jobId, "Scaled down (auto) stage " + stage + " by " + actualDecrement + " workers"));
//            String mesg = "Scaled down (auto) stage " + stage + " of job " + jobId + " by " + actualDecrement +
//                    " workers (indexes " + sb.toString() + ") to " + msmd.getNumWorkers() + ", reason: " + reason;
//            sendStatus(new Status(jobId, stage, -1, -1, Status.TYPE.INFO, mesg, MantisJobState.Started));
//        }
//        return actualDecrement;
//    }
//
//    public void setScalingPolicy(int stageNum, StageScalingPolicy scalingPolicy) throws InvalidJobException {
//        if (!isActive())
//            return;
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        final MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) jobMetadata.getStageMetadata(stageNum);
//        if (msmd == null)
//            throw new InvalidJobException(jobId, stageNum, -1);
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            msmd.setScalingPolicy(scalingPolicy);
//            boolean stored = false;
//            if (scalingPolicy != null && scalingPolicy.isEnabled()) {
//                msmd.setScalable(true);
//                logger.info(jobId + ": setting #workers from " + msmd.getNumWorkers() + " to " + scalingPolicy.getMin() +
//                        "/" + scalingPolicy.getMax());
//                if (msmd.getNumWorkers() < scalingPolicy.getMin()) {
//                    setStageWorkersCount(stageNum, scalingPolicy.getMin(), "updated scaling policy");
//                    stored = true;
//                }
//                if (msmd.getNumWorkers() > scalingPolicy.getMax()) {
//                    setStageWorkersCount(stageNum, scalingPolicy.getMax(), "updated scaling policy");
//                    stored = true;
//                }
//            }
//            if (!stored) {
//                try {
//                    store.updateStage(msmd);
//                } catch (IOException e) {
//                    logger.error("Error updating stage: " + e.getMessage(), e);
//                    throw new InvalidJobException(jobId, stageNum, -1, e);
//                }
//            }
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(
//                    MantisAuditLogEvent.Type.JOB_SCALE_UPDATE, jobId, "Scaling policy updated for stage " + stageNum + " (" +
//                    (scalingPolicy == null ? "null" : mapper.writeValueAsString(scalingPolicy)) + ")"));
//        } catch (InvalidJobException ije) {
//            throw ije;
//        } catch (Exception e) { // catching for AutoCloseable
//            logger.error("Unexpected error: " + e.getMessage(), e); // shouldn't happen
//        }
//    }
//
//    public void setStageWorkersCount(int stageNum, int numWorkers, String reason) throws InvalidJobException {
//        if (!isActive() || numWorkers < 1)
//            return;
//        final MantisJobMetadata mjmd = getJobMetadata();
//        final MantisStageMetadataWritable msmd = (MantisStageMetadataWritable) mjmd.getStageMetadata(stageNum);
//        if (msmd == null)
//            throw new InvalidJobException(jobId, stageNum, -1);
//        if (!msmd.getScalable()) {
//            sendStatus(new Status(jobId, stageNum, -1, -1, Status.TYPE.WARN,
//                    "Can't change #workers to " + numWorkers + ", stage " + stageNum + " is not scalable", MantisJobState.Started));
//            logger.warn("Job " + jobId + " stage " + stageNum + " is not scalable, can't change #workers to " + numWorkers);
//            return;
//        }
//        if (msmd.getScalingPolicy() != null && numWorkers < msmd.getScalingPolicy().getMin()) {
//            final String msg = "Can't scale down #workers of stage " + stageNum + " below MIN=" +
//                    msmd.getScalingPolicy().getMin() + " to requested " + numWorkers;
//            sendStatus(new Status(jobId, stageNum, -1, -1, Status.TYPE.WARN, msg, MantisJobState.Started));
//            logger.warn("Job " + jobId + ": " + msg);
//            return;
//        }
//        if (msmd.getScalingPolicy() != null && numWorkers > msmd.getScalingPolicy().getMax()) {
//            final String msg = "Can't scale up #workers of stage " + stageNum + " above MAX=" +
//                    msmd.getScalingPolicy().getMax() + " to requested " + numWorkers;
//            sendStatus(new Status(jobId, stageNum, -1, -1, Status.TYPE.WARN, msg, MantisJobState.Started));
//            logger.warn("Job " + jobId + ": " + msg);
//            return;
//        }
//        sendStatus(new Status(jobId, stageNum, -1, -1, Status.TYPE.INFO,
//                "Setting #workers to " + numWorkers + " for stage " + stageNum + ", reason=" + reason,
//                MantisJobState.Started));
//        numScaleStage.increment();
//        try (AutoCloseable l = mjmd.obtainLock()) {
//            int prevNum = msmd.getNumWorkers();
//            msmd.unsafeSetNumWorkers(numWorkers);
//            try {
//                store.updateStage(msmd);
//            } catch (IOException e) {
//                logger.error("Error setting stage count for job " + jobId);
//                throw new InvalidJobException(jobId, msmd.getStageNum(), -1, e);
//            }
//            if (prevNum < numWorkers) {
//                for (int i = prevNum; i < numWorkers; i++) {
//                    //logger.info("Creating new worker for index " + i);
//                    MantisWorkerMetadata workerByIndex = null;
//                    try {
//                        workerByIndex = mjmd.getWorkerByIndex(msmd.getStageNum(), i);
//                    } catch (InvalidJobException e) {} // worker for that index didn't exist before
//                    final WorkerRequest workerRequest;
//                    if (workerByIndex == null) {
//                        workerRequest = createWorkerRequest(msmd, i, mjmd.getUser(), Optional.empty());
//                        store.storeNewWorker(workerRequest);
//                    } else {
//                        workerRequest = createWorkerRequest(msmd, i, mjmd.getUser(), workerByIndex.getCluster());
//                        store.replaceTerminatedWorker(workerRequest, workerByIndex);
//                    }
//                    queueTask(workerRequest);
//                    //                    if(workerByIndex==null)
//                    //                        workerByIndex = mjmd.getWorkerByIndex(msmd.getStageNum(), i);
//                    //                    logger.info("Worker for index " + i + " is " + workerByIndex.getWorkerNumber());
//                }
//                MantisAuditLogWriter.getInstance()
//                        .getObserver().onNext(new MantisAuditLogEvent(
//                        MantisAuditLogEvent.Type.JOB_SCALE_UP, jobId, "Scaled up (manual) stage " + stageNum + " by " +
//                        (numWorkers - prevNum) + " workers" +
//                        (reason != null && !reason.isEmpty() ? ", reason=" + reason : "")));
//            } else {
//                for (int w = numWorkers; w < prevNum; w++) {
//                    MantisWorkerMetadata worker = mjmd.getWorkerByIndex(msmd.getStageNum(), w);
//                    logger.info("Removing worker index " + worker.getWorkerIndex() + " due to scale down of stage " + stageNum + " of job " + jobId);
//                    sendStatus(new Status(jobId, stageNum, worker.getWorkerIndex(), worker.getWorkerNumber(), Status.TYPE.INFO,
//                            "Removing worker index " + worker.getWorkerIndex() + " due to scale down of stage " + stageNum, MantisJobState.Noop));
//                    killWorker(worker);
//                    store.archiveWorker((MantisWorkerMetadataWritable) worker);
//                    if (!msmd.unsafeRemoveWorker(worker.getWorkerIndex(), worker.getWorkerNumber()))
//                        logger.error(String.format("Job %s stage %d: Unexpected worker state, couldn't remove worker idx=%d, number=%d",
//                                jobId, msmd.getStageNum(), worker.getWorkerIndex(), worker.getWorkerNumber()));
//                    logger.info(String.format("Job %s archived worker index %d, number %d", jobId,
//                            worker.getWorkerIndex(), worker.getWorkerNumber()));
//                }
//                MantisAuditLogWriter.getInstance()
//                        .getObserver().onNext(new MantisAuditLogEvent(
//                        MantisAuditLogEvent.Type.JOB_SCALE_DOWN, jobId, "Scaled down (manual) stage " + stageNum + " by " +
//                        (prevNum - numWorkers) + " workers" +
//                        (reason != null && !reason.isEmpty() ? ", reason=" + reason : "")));
//            }
//        } catch (IOException | InvalidJobException e) {
//            logger.error("Error in manual scale: " + e.getMessage(), e);
//        } catch (Exception e) {
//            logger.error("Unexpected error: " + e.getMessage(), e);
//        } // shouldn't happen
//    }
//
//    public HeartbeatsStatus getAndResetHeartbeatsStatus() {
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            if (!MantisJobState.isRunningState(jobMetadata.getState()))
//                return null;
//            final ArrayList<MantisWorkerMetadata> receipts = heartbeatReceipts.getAndSet(new ArrayList<MantisWorkerMetadata>());
//            Set<String> uniqueWorkers = new HashSet<>();
//            if (!receipts.isEmpty()) {
//                for (MantisWorkerMetadata mwmd : receipts)
//                    uniqueWorkers.add(mwmd.getStageNum() + ":" + mwmd.getWorkerIndex());
//            }
//            return new HeartbeatsStatus(getAllRunningWorkers().size(), uniqueWorkers.size());
//        } catch (Exception e) {
//            logger.warn("Unexpected: " + e.getMessage());
//            return null;
//        }
//    }
//
//    public void checkJobWorkersHealth(final boolean checkMissedHeartbeats) {
//        if (initialized.get() && !MantisJobState.isTerminalState(getJobMetadata().getState())) {
//            if (checkMissedHeartbeats) {
//                checkOnHeartbeatStatus();
//            }
//            moveWorkersOnDisabledVMs();
//            scaleStages(getJobMetadata());
//        }
//    }
//
//    private void checkOnHeartbeatStatus() {
//        for (MantisWorkerMetadata mwmd : getAllRunningWorkers()) {
//            if (heartbeatTooOld(mwmd)) {
//                vmService.killTask(mwmd.getWorkerId());
//                handleStatus(new Status(jobId, mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber(),
//                        Status.TYPE.ERROR, getWorkerStringPrefix(mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()) +
//                        " Heartbeat too old", MantisJobState.Failed));
//            }
//        }
//    }
//
//    private int getTotalNumWorkers() {
//        return getAllWorkers(mwm -> true, mwm -> mwm).size();
//    }
//
//    private void moveWorkersOnDisabledVMs() {
//        if (!workersOnDisabledVMs.isEmpty()) {
//            final List<Integer> workersToMigrate = migrationStrategy.execute(
//                    workersOnDisabledVMs, getAllRunningWorkers().size(),
//                    getTotalNumWorkers(), lastWorkerMigrationTimestamp);
//            workersToMigrate.forEach(workerNumber -> {
//                final MantisWorkerMetadata workerMetadata = getWorkerMetadata(workerNumber, false);
//                if (workerMetadata == null) {
//                    return; // worker not active
//                }
//                try (AutoCloseable ac = getJobMetadata().obtainLock()) {
//                    if (MantisJobState.isRunningState(workerMetadata.getState())) {
//                        logger.info("Moving worker number " + workerMetadata.getWorkerNumber() + " index " + workerMetadata.getWorkerIndex() +
//                                " of job " + jobId + " away from disabled VM");
//                        vmService.killTask(workerMetadata.getWorkerId());
//                        handleStatus(new Status(jobId, workerMetadata.getStageNum(), workerMetadata.getWorkerIndex(), workerMetadata.getWorkerNumber(),
//                                Status.TYPE.ERROR, getWorkerStringPrefix(workerMetadata.getStageNum(), workerMetadata.getWorkerIndex(), workerMetadata.getWorkerNumber()) +
//                                " Moving out of disabled VM " +
//                                workerMetadata.getSlave(), MantisJobState.Failed));
//                        lastWorkerMigrationTimestamp = clock.now();
//                    }
//                } catch (Exception e) {
//                    // shouldn't happen
//                    logger.warn("Unexpected exception locking metadata for worker num " + workerMetadata.getWorkerNumber() + " of job " + jobId);
//                }
//            });
//        }
//    }
//
//    public void workerUnScheduleable(int stage, final WorkerId workerId) {
//        logger.info("Rate limiting unschedulable worker {} stage {}", workerId, stage);
//        sendStatus(new Status(jobId, stage, workerId.getWorkerIndex(), workerId.getWorkerNum(), Status.TYPE.INFO,
//                getWorkerStringPrefix(stage, workerId.getWorkerIndex(), workerId.getWorkerNum()) +
//                        " rate limiting: no resources to fit worker", MantisJobState.Accepted));
//        final long resubmitAt = resubmitRateLimiter.getWorkerResubmitTime(jobId, stage, workerId.getWorkerIndex());
//        scheduler.updateWorkerSchedulingReadyTime(workerId, resubmitAt);
//    }
//
//    protected ScheduleRequest createSchedulingRequest(final WorkerRequest workerRequest, final Optional<Long> readyAt) {
//        final WorkerId workerId = new WorkerId(workerRequest.getJobId(), workerRequest.getWorkerIndex(), workerRequest.getWorkerNumber());
//
//        // setup constraints
//        final List<ConstraintEvaluator> hardConstraints = new ArrayList<>();
//        final List<VMTaskFitnessCalculator> softConstraints = new ArrayList<>();
//        MantisStageMetadata stageMetadata = getJobMetadata().getStageMetadata(workerRequest.getWorkerStage());
//        List<JobConstraints> stageHC = stageMetadata.getHardConstraints();
//        List<JobConstraints> stageSC = stageMetadata.getSoftConstraints();
//
//        final Set<String> coTasks = new HashSet<>();
//        if ((stageHC != null && !stageHC.isEmpty()) ||
//                (stageSC != null && !stageSC.isEmpty())) {
//            for (MantisWorkerMetadata mwmd : stageMetadata.getWorkerByIndexMetadataSet()) {
//                if (mwmd.getWorkerNumber() != workerId.getWorkerNum())
//                    coTasks.add(workerId.getId());
//            }
//        }
//        if (stageHC != null && !stageHC.isEmpty()) {
//            for (JobConstraints c : stageHC) {
//                hardConstraints.add(ConstraintsEvaluators.hardConstraint(c, coTasks));
//            }
//        }
//        if (stageSC != null && !stageSC.isEmpty()) {
//            for (JobConstraints c : stageSC) {
//                softConstraints.add(ConstraintsEvaluators.softConstraint(c, coTasks));
//            }
//        }
//
//        final ScheduleRequest scheduleRequest = new ScheduleRequest(workerId,
//                workerRequest.getWorkerStage(),
//                workerRequest.getNumPortsPerInstance(),
//                new JobMetadata(jobId,
//                        jobDefinition.getJobJarFileLocation(),
//                        workerRequest.getTotalStages(),
//                        workerRequest.getUser(),
//                        workerRequest.getSchedulingInfo(),
//                        workerRequest.getParameters(),
//                        workerRequest.getSubscriptionTimeoutSecs(),
//                        workerRequest.getMinRuntimeSecs()),
//                getJobMetadata().getSla().getDurationType(),
//                workerRequest.getDefinition(),
//                hardConstraints,
//                softConstraints,
//                readyAt.orElse(0L),
//                workerRequest.getPreferredCluster());
//        return scheduleRequest;
//    }
//
//    protected void queueTask(final WorkerRequest workerRequest, final Optional<Long> readyAt) {
//        final ScheduleRequest schedulingRequest = createSchedulingRequest(workerRequest, readyAt);
//        scheduler.scheduleWorker(schedulingRequest);
//    }
//
//    protected void queueTask(final WorkerRequest workerRequest) {
//        queueTask(workerRequest, Optional.empty());
//    }
//
//    private void dequeueIfTerminalAndSendStatus(Status status) {
//        try {
//            if (MantisJobState.isTerminalState(status.getState())) {
//                final int workerNumber = status.getWorkerNumber();
//                final MantisJobMetadata jobMetadata = getJobMetadata();
//                if (jobMetadata != null) {
//                    try {
//                        final MantisWorkerMetadata w = jobMetadata.getWorkerByNumber(workerNumber);
//                        if (w != null)
//                            scheduler.unscheduleWorker(
//                                    w.getWorkerId(),
//                                    Optional.ofNullable(w.getSlave()));
//                    } catch (InvalidJobException e) {
//                        logger.info(jobId + ": couldn't remove task number " + workerNumber +
//                                " from scheduler queue upon state=" + status.getState() + " - " + e.getMessage());
//                    }
//                }
//            }
//            statusSerializedObserver.onNext(status);
//        } catch (Exception e) {
//            logger.warn("Problem sending status out: " + e.getMessage(), e);
//        }
//    }
//
//    private void storeWorkerStateAndSendStatus(int workerNumber, MantisJobState state, JobCompletedReason reason, Status status)
//            throws IOException, InvalidJobException, InvalidJobStateChangeException {
//        dequeueIfTerminalAndSendStatus(status);
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        if (jobMetadata == null || jobMetadata.getSla() == null || store == null) {
//            logger.error("would fail to store worker state {} jobID {} workerNum {} (jobMetadataNull? {} slaNull? {} storeNull? {})",
//                    state, jobId, workerNumber, (jobMetadata == null), (jobMetadata == null || jobMetadata.getSla() == null), (store == null));
//        }
//        store.storeWorkerState(jobId, workerNumber, state, reason,
//                jobMetadata.getSla().getDurationType() == MantisJobDurationType.Perpetual);
//    }
//
//    private void sendStatus(Status status) {
//        try {
//            statusSerializedObserver.onNext(status);
//        } catch (Exception e) {
//            logger.warn("Problem sending status out: " + e.getMessage(), e);
//        }
//    }
//
//    public boolean isActive() {
//        return !MantisJobState.isTerminalState(getJobMetadata().getState());
//    }
//
//    // caller must lock job object
//    private void killWorker(MantisWorkerMetadata mwmd) {
//        try {
//            storeWorkerStateAndSendStatus(mwmd.getWorkerNumber(), MantisJobState.Completed, JobCompletedReason.Killed,
//                    new Status(jobId, mwmd.getStageNum(), mwmd.getWorkerIndex(),
//                            mwmd.getWorkerNumber(), Status.TYPE.INFO,
//                            getWorkerStringPrefix(mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()) +
//                                    " Killed", MantisJobState.Completed));
//            vmService.killTask(mwmd.getWorkerId());
//            numWorkerTerminated.increment();
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.WORKER_TERMINATE,
//                    jobId + "-workerNum-" + mwmd.getWorkerNumber(), "stage=" + mwmd.getStageNum() + ", index=" + mwmd.getWorkerIndex()));
//        } catch (IOException | InvalidJobException | InvalidJobStateChangeException e) {
//            logger.error("Can't kill worker " + mwmd.getWorkerNumber() + " of job " + jobId + ": " + e.getMessage());
//        }
//    }
//
//    private void killJob(String user) {
//        killJob(user, null);
//    }
//
//    public void killJob(String user, String reason) {
//        MantisJobMetadata jobMetadata = getJobMetadata();
//        logger.info("attempt to acquire lock to kill job {}", jobId);
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            if (MantisJobState.isTerminalState(jobMetadata.getState()))
//                return;
//            for (MantisWorkerMetadata mwmd : getAllRunningWorkers()) {
//                killWorker(mwmd);
//            }
//            storeAndMarkJobTerminated(MantisJobState.Completed);
//            if (reason != null && !reason.isEmpty()) {
//                dequeueIfTerminalAndSendStatus(new Status(jobId, -1, -1, -1, Status.TYPE.INFO, "Killing job, reason: " + reason, MantisJobState.Completed));
//                logger.info("Killing job {} user {} reason {}", jobId, user, reason);
//            }
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.JOB_TERMINATE, jobId, "user=" + user));
//        } catch (Exception e) {
//            logger.error("Unexpected error while killing job: " + e.getMessage(), e);
//        }
//    }
//
//    void enforceSla() {
//        if (MantisJobState.isTerminalState(getJobMetadata().getState()))
//            return;
//        // this is the only sla we have for now
//        if (getJobMetadata().getSla().getRuntimeLimitSecs() > 0 && hasReachedRuntimeLimit()) {
//            killJob("MantisMaster", "reached runtime limit of " + getJobMetadata().getSla().getRuntimeLimitSecs() + " secs");
//        }
//    }
//
//    private boolean hasReachedRuntimeLimit() {
//        return (System.currentTimeMillis() - getJobMetadata().getSubmittedAt()) > (getJobMetadata().getSla().getRuntimeLimitSecs() * 1000);
//    }
//
//    private void registerHeartbeat(final Status status) {
//        MantisJobState jobState = getJobMetadata().getState();
//        numHeartBeatsReceived.increment();
//        final Optional<WorkerId> workerIdO = status.getWorkerId();
//        if (!workerIdO.isPresent()) {
//            logger.warn("received heartbeat from worker with invalid ID {} {}-{}", status.getJobId(), status.getWorkerIndex(), status.getWorkerNumber());
//            return;
//        }
//        final WorkerId workerId = workerIdO.get();
//        if (MantisJobState.isTerminalState(jobState)) {
//            logger.warn("Got heartbeat from worker {} in state={}, killing it", workerId, jobState);
//            try {
//                int stageNum = getJobMetadata().getWorkerByNumber(workerId.getWorkerNum()).getStageNum();
//                storeWorkerStateAndSendStatus(workerId.getWorkerNum(), MantisJobState.Failed, JobCompletedReason.Killed,
//                        new Status(jobId, stageNum, workerId.getWorkerIndex(), workerId.getWorkerNum(), Status.TYPE.INFO,
//                                getWorkerStringPrefix(stageNum, workerId.getWorkerIndex(), workerId.getWorkerNum()) + " killed, shouldn't be running",
//                                MantisJobState.Failed));
//            } catch (Exception e) {
//                logger.warn("Unexpected error storing state=Failed for worker {}, ignoring", workerId);
//            }
//            vmService.killTask(workerId);
//            return;
//        }
//        try {
//            logger.debug("Got heartbeat from {} with {} payloads", workerId, status.getPayloads().size());
//            MantisWorkerMetadataWritable mwmd = getMantisWorkerMetadataWritable(workerId.getWorkerNum());
//            if (mwmd != null) {
//                try (AutoCloseable l = getJobMetadata().obtainLock()) {
//                    if (!MantisJobState.isOnSlaveState(mwmd.getState())) {
//                        // this worker shouldn't be running, kill it
//                        vmService.killTask(mwmd.getWorkerId());
//                    } else {
//                        heartbeatReceipts.get().add(mwmd);
//                        mwmd.setLastHeartbeatAt(System.currentTimeMillis());
//                        if (mwmd.getState() != MantisJobState.Started) {
//                            storeWorkerStateAndSendStatus(workerId.getWorkerNum(), MantisJobState.Started, JobCompletedReason.Normal, status);
//                        }
//                        if ((jobState != MantisJobState.Launched) && allWorkersStarted()) {
//                            markJobLaunched();
//                        }
//                        handlePayloads(mwmd, status.getPayloads());
//                    }
//                } catch (Exception e) {
//                    logger.warn("Can't obtain lock on worker " + mwmd.getWorkerNumber() + " of job " + mwmd.getJobId()
//                            + " - " + e.getMessage());
//                }
//            } else {
//                // heartbeat from a worker that isn't supposed to be there, kill it
//                vmService.killTask(workerId);
//            }
//        } catch (InvalidJobException e) {
//            logger.warn("Got worker heartbeat for invalid worker {}, killing", workerId, e);
//            vmService.killTask(workerId);
//        }
//    }
//
//    public void handleSubscriberTimeout() {
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            if (!MantisJobState.isTerminalState(jobMetadata.getState())) {
//                if ((System.currentTimeMillis() - lastNewSubscriberAt.get()) > (evalSubscriberTimeoutSecs() * 1000))
//                    killJob("MantisMaster", "ephemeral job with no subscribers for " + evalSubscriberTimeoutSecs() + " secs");
//            }
//        } catch (Exception e) {
//            logger.error("Unexpected to not get lock on job metadata: " + e.getMessage(), e);
//        }
//    }
//
//    private long getMinRuntimeSecs() {
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        return (jobMetadata.getSubmittedAt() / 1000L + jobMetadata.getSla().getMinRuntimeSecs()) - System.currentTimeMillis() / 1000L;
//    }
//
//    public long evalSubscriberTimeoutSecs() {
//        long minRuntimeSecs = getMinRuntimeSecs();
//        return Math.max(
//                minRuntimeSecs,
//                subscriptionTimeoutSecs
//        );
//    }
//
//    public boolean markNewSubscriber() throws InvalidJobException {
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            if (MantisJobState.isTerminalState(jobMetadata.getState()))
//                return false;
//            lastNewSubscriberAt.set(System.currentTimeMillis());
//            return true;
//        } catch (Exception e) {
//            logger.error("Unexpected to not get lock on job metadata: " + e.getMessage(), e);
//            return false;
//        }
//    }
//
//    private void handlePayloads(MantisWorkerMetadata mwmd, List<Status.Payload> payloads) {
//        if (payloads == null)
//            return;
//        for (Status.Payload payload : payloads) {
//            if (!hasJobMaster ||
//                    StatusPayloads.Type.valueOf(payload.getType()) == StatusPayloads.Type.IncomingDataDrop
//            ) {
//                // handle only if there's no jobMaster, or if it exists, handle only data drop for outlier workers
//                HeartbeatPayloadHandler.getInstance().handle(
//                        new HeartbeatPayloadHandler.Data(jobId, this, mwmd.getStageNum(), mwmd.getWorkerIndex(),
//                                mwmd.getWorkerNumber(), payload));
//            }
//        }
//    }
//
//    /*
//    Return empty Optional if no resubmit, else return the delayed resubmit time for the new worker
//     */
//    private Optional<Long> shouldResubmit(int stageNumber, int workerIndex,
//                                          MantisJobState oldState, MantisJobState newState, JobCompletedReason reason) {
//        if (MantisJobState.isTerminalState(oldState))
//            return Optional.empty();
//        switch (newState) {
//        case Failed:
//            return Optional.ofNullable(resubmitRateLimiter.getWorkerResubmitTime(jobId, stageNumber, workerIndex));
//        case Completed:
//            return Optional.empty(); // for now don't resubmit on this case
//        //            if(reason == JobCompletedReason.Error) {
//        //                resubmitHandler.delayResubmit(stageNumber, workerIndex);
//        //                return true;
//        //            }
//        }
//        return Optional.empty();
//    }
//
//    @SuppressWarnings("unchecked")
//    private List<MantisWorkerMetadata> getAllActiveWorkers() {
//        return (List<MantisWorkerMetadata>) getAllWorkers(
//                new Func1<MantisWorkerMetadata, Boolean>() {
//                    @Override
//                    public Boolean call(MantisWorkerMetadata w) {
//                        return !MantisJobState.isTerminalState(w.getState());
//                    }
//                },
//                new Func1<MantisWorkerMetadata, MantisWorkerMetadata>() {
//                    @Override
//                    public MantisWorkerMetadata call(MantisWorkerMetadata mantisWorkerMetadata) {
//                        return mantisWorkerMetadata;
//                    }
//                });
//    }
//
//    public void handleStatus(final Status status) {
//        Action0 postAction = null;
//        boolean doPostActionIfRequired = false;
//        if (status.getType() == Status.TYPE.HEARTBEAT) {
//            registerHeartbeat(status);
//            return;
//        }
//        try {
//            MantisWorkerMetadataWritable mwmd = getMantisWorkerMetadataWritable(status.getWorkerNumber());
//            final Optional<Long> shdResubmit = shouldResubmit(mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getState(), status.getState(), status.getReason());
//            if (MantisJobState.isOnSlaveState(status.getState()) || MantisJobState.isTerminalState(status.getState()))
//                status.setHostname(mwmd.getSlave());
//            if (MantisJobState.isTerminalState(status.getState())) {
//                if (!shdResubmit.isPresent() && isValidWorkerIndex(status)) {
//                    boolean killJob = getJobMetadata().getSla().getDurationType() == MantisJobDurationType.Perpetual;
//                    if (!killJob) {
//                        List<MantisWorkerMetadata> allActiveWorkers = getAllActiveWorkers();
//                        if (allActiveWorkers != null && allActiveWorkers.size() == 1) {
//                            if (allActiveWorkers.get(0).getWorkerNumber() == status.getWorkerNumber()) {
//                                // got complete on the last running worker, terminate job
//                                killJob = true;
//                            }
//                        }
//                    }
//                    final boolean klJob = killJob;
//                    postAction = new Action0() {
//                        @Override
//                        public void call() {
//                            status.getWorkerId().ifPresent(wId -> {
//                                vmService.killTask(wId); // explicitly kill the worker reporting complete
//                            });
//                            if (klJob) {
//                                logger.warn("Worker " + status.getWorkerNumber() + " of job " + jobId +
//                                        " reached terminal state of " + status.getState() + ", terminating entire job");
//                                killJob("MantisMaster");
//                            }
//                        }
//                    };
//                }
//            }
//            try (AutoCloseable l = getJobMetadata().obtainLock()) {
//                if (shdResubmit.isPresent()) {
//                    resubmitWorker(status, shdResubmit, mwmd.getCluster());
//                    MantisAuditLogWriter.getInstance()
//                            .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.WORKER_TERMINATE,
//                            jobId + "-workerNum-" + status.getWorkerNumber(),
//                            "stage=" + status.getStageNum() + ", index=" + status.getWorkerIndex()));
//                } else {
//                    storeWorkerStateAndSendStatus(status.getWorkerNumber(), status.getState(), status.getReason(), status);
//                    if (status.getState() == MantisJobState.Started) {
//                        if (getJobMetadata().getState() == MantisJobState.Accepted) {
//                            if (allWorkersStarted()) {
//                                postAction = new Action0() {
//                                    @Override
//                                    public void call() {
//                                        markJobLaunched();
//                                    }
//                                };
//                                doPostActionIfRequired = true;
//                            }
//                        }
//                        MantisAuditLogWriter.getInstance()
//                                .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.WORKER_START,
//                                jobId + "-workerNum-" + status.getWorkerNumber(),
//                                "stage=" + status.getStageNum() + ", index=" + status.getWorkerIndex()));
//                        final long startLatency = mwmd.getStartedAt() - mwmd.getLaunchedAt();
//                        workerLaunchToStartMillis.increment(startLatency);
//                        workerLaunchToStartDistMillis.recordValue(startLatency);
//                    } else if (MantisJobState.isTerminalState(status.getState()))
//                        MantisAuditLogWriter.getInstance()
//                                .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.WORKER_TERMINATE,
//                                jobId + "-workerNum-" + status.getWorkerNumber(),
//                                "stage=" + status.getStageNum() + ", index=" + status.getWorkerIndex() + ", reason=" + status.getReason()));
//                }
//                doPostActionIfRequired = true;
//            } catch (InvalidJobStateChangeException jse) {
//                if (MantisJobState.isOnSlaveState(status.getState())) {
//                    // slave is trying to start job when it shouldn't, kill it
//                    vmService.killTask(mwmd.getWorkerId());
//                    logger.warn("Invalid to move worker " + status.getWorkerNumber() +
//                            " to " + status.getState() + " state, sent kill request - " + jse.getMessage());
//                } else
//                    logger.warn("Ignoring worker state change error - " + jse.getMessage());
//            } catch (InvalidJobException ije) {
//                logger.warn("Ignoring worker status error - " + ije.getMessage());
//            }
//        } catch (InvalidJobException ije) {
//            if (MantisJobState.isOnSlaveState(status.getState())) {
//                status.getWorkerId().ifPresent(wId -> {
//                    logger.warn("Unknown worker status '{}' received, sent request to kill {}-{}-{}", status.getState(), status.getJobId(),
//                            status.getWorkerIndex(), status.getWorkerNumber());
//                    vmService.killTask(wId);
//                });
//            } else {
//                logger.warn("Ignoring worker status '{}' on unknown worker {}-{}-{}", status.getState(),
//                        status.getJobId(), status.getWorkerIndex(), status.getWorkerNumber());
//            }
//        } catch (Exception e) {
//            logger.error("Unexpected status (" + status + ") - " + e.getMessage(), e);
//            //            sendStatus(new Status(jobId, status.getStageNum(), status.getWorkerIndex(), status.getWorkerNumber(), Status.TYPE.ERROR,
//            //                    status.getMessage()+": Error setting state - " + e.getMessage(), status.getState()));
//        } finally {
//            if (doPostActionIfRequired && postAction != null)
//                postAction.call();
//        }
//    }
//
//    private void markJobLaunched() {
//        final MantisJobMetadata jobMetadata = getJobMetadata();
//        boolean notifyNmdJb = false;
//        try (AutoCloseable l = jobMetadata.obtainLock()) {
//            if (jobMetadata.getState() == MantisJobState.Accepted) {
//                store.storeJobState(jobId, MantisJobState.Launched);
//                notifyNmdJb = true;
//            }
//        } catch (InvalidJobException | IOException | InvalidJobStateChangeException e) {
//            logger.warn("Couldn't mark state=Started on job " + jobId + ": " + e.getMessage());
//        } catch (Exception e) {
//            logger.warn("Unexpected exception: " + e.getMessage()); // shouldn't happen
//        }
//        if (notifyNmdJb)
//            // call NamedJob mutating method asynchronous with current JobMgr mutating method
//            Schedulers.from(mantisJobMgrExecutorService).createWorker().schedule(() -> {
//                try {
//                    namedJob.addJobMgr(this);
//                } catch (Exception e) {
//                    logger.error("caught exception adding Job Manager for {}", jobId);
//                }
//            });
//    }
//
//    private boolean isValidWorkerIndex(Status status) {
//        return !(status.getWorkerIndex() < 0 || status.getStageNum() < 0) &&
//                status.getWorkerIndex() < getJobMetadata().getStageMetadata(status.getStageNum()).getNumWorkers();
//    }
//
//    private String getWorkerStringPrefix(int stageNum, int index, int number) {
//        return "stage " + stageNum + " worker index=" + index + " number=" + number;
//    }
//
//    public void setWorkerLaunched(final int workerNumber, final String hostname, final String slaveID,
//                                  final Optional<String> clusterName, final WorkerPorts ports)
//            throws InvalidJobStateChangeException, InvalidJobException {
//        MantisJobMetadata mjmd = getJobMetadata();
//        MantisWorkerMetadataWritable mwmd = getMantisWorkerMetadataWritable(workerNumber);
//        try (AutoCloseable l = mjmd.obtainLock()) {
//            mwmd.setSlave(hostname);
//            mwmd.setSlaveID(slaveID);
//            mwmd.setCluster(clusterName);
//            mwmd.setMetricsPort(ports.getMetricsPort());
//            mwmd.setDebugPort(ports.getDebugPort());
//            mwmd.setConsolePort(ports.getConsolePort());
//            mwmd.setCustomPort(ports.getCustomPort());
//            mwmd.addPorts(ports.getPorts());
//            mwmd.setLastHeartbeatAt(System.currentTimeMillis()); // start counting heartbeats from now
//            Status status = new Status(jobId,
//                    mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber(), Status.TYPE.INFO,
//                    getWorkerStringPrefix(mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()) +
//                            " scheduled on " + hostname + " with ports=" + mapper.writeValueAsString(ports)
//                    , MantisJobState.Launched);
//            status.setHostname(hostname);
//            storeWorkerStateAndSendStatus(workerNumber, MantisJobState.Launched, JobCompletedReason.Normal, status);
//            logger.info("Launched worker " + mwmd.getWorkerNumber() + " index " + mwmd.getWorkerIndex() +
//                    " of job " + jobId + " on host " + hostname + " using ports " + mapper.writeValueAsString(ports));
//        } catch (InvalidJobException | InvalidJobStateChangeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new InvalidJobStateChangeException(jobId, mwmd.getState(), MantisJobState.Launched, e);
//        }
//    }
//
//    public void handleVMStatusForWorker(Status status) {
//        if (MantisJobState.isTerminalState(status.getState()))
//            handleStatus(status);
//        else if (MantisJobState.isRunningState(status.getState())) {
//            boolean killTask = false;
//            MantisWorkerMetadataWritable w = null;
//            try {
//                w = getMantisWorkerMetadataWritable(status.getWorkerNumber());
//                if (MantisJobState.isTerminalState(w.getState())) {
//                    killTask = true;
//                }
//            } catch (InvalidJobException e) {
//                killTask = true;
//            }
//            if (killTask) {
//                status.getWorkerId().ifPresent(wId -> {
//                    vmService.killTask(wId);
//                    logger.info("Killing worker {} supposed to be in terminal state, got active state notification from VMService", wId);
//                });
//
//            }
//        }
//    }
//
//    public void resubmitWorker(int workerNumber) throws InvalidJobException, InvalidJobStateChangeException {
//        resubmitWorker(workerNumber, null);
//    }
//
//    public void resubmitWorker(int workerNumber, String reason) throws InvalidJobException, InvalidJobStateChangeException {
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        MantisWorkerMetadata mwmd = mjmd.getWorkerByNumber(workerNumber);
//        try (AutoCloseable l = mjmd.obtainLock()) {
//            if (!MantisJobState.isRunningState(mwmd.getState()))
//                throw new InvalidJobStateChangeException(jobId, mwmd.getState());
//            final Status status = new Status(jobId, mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber(),
//                    Status.TYPE.WARN, getWorkerStringPrefix(mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()) +
//                    " User requested resubmit", MantisJobState.Failed);
//            status.setReason(JobCompletedReason.Relaunched);
//            resubmitWorker(status, Optional.empty(), mwmd.getCluster());
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.WORKER_TERMINATE,
//                    jobId + "-workerNum-" + status.getWorkerNumber(),
//                    "stage=" + status.getStageNum() + ", index=" + status.getWorkerIndex() +
//                            (reason == null ? ", resubmit requested" : ", reason=" + reason)));
//        } catch (InvalidJobException | InvalidJobStateChangeException e) {
//            throw e;
//        } catch (Exception e) {
//            logger.error("Unexpected error resubmitting worker " + workerNumber + " of job " + jobId +
//                    " for state change; " + e.getMessage());
//            throw new InvalidJobException(jobId, mwmd.getStageNum(), mwmd.getWorkerNumber(), e);
//        }
//    }
//
//    // Caller must lock worker associated with status
//    private void resubmitWorker(final Status status,
//                                final Optional<Long> when,
//                                final Optional<String> cluster) throws InvalidJobException, InvalidJobStateChangeException {
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        MantisWorkerMetadata mwmd = mjmd.getWorkerByNumber(status.getWorkerNumber());
//        MantisStageMetadata msmd = mjmd.getStageMetadata(mwmd.getStageNum());
//        if (mwmd.getTotalResubmitCount() < ConfigurationProvider.getConfig().getMaximumResubmissionsPerWorker()) {
//            WorkerRequest request = new WorkerRequest(msmd.getMachineDefinition(),
//                    jobId, mwmd.getWorkerIndex(), workerNumberGenerator.getNextWorkerNumber(), mjmd.getJarUrl(),
//                    mwmd.getStageNum(), mjmd.getNumStages(), msmd.getNumWorkers(),
//                    mjmd.getName(), msmd.getMachineDefinition().getNumPorts(),
//                    mjmd.getParameters(), mjmd.getSla(), msmd.getHardConstraints(), msmd.getSoftConstraints(),
//                    jobDefinition.getSchedulingInfo(), subscriptionTimeoutSecs, getMinRuntimeSecs(),
//                    mjmd.getSubmittedAt(), mjmd.getUser(), cluster);
//            try {
//                final Status s = new Status(jobId, mwmd.getStageNum(), status.getWorkerIndex(), status.getWorkerNumber(), Status.TYPE.INFO,
//                        "resubmitting lost worker - " + status.getMessage(), status.getState());
//                s.setHostname(status.getHostname());
//                dequeueIfTerminalAndSendStatus(s);
//                ((MantisWorkerMetadataWritable) mwmd).setState(status.getState(), System.currentTimeMillis(), status.getReason());
//                vmService.killTask(mwmd.getWorkerId()); // in case it is still there
//                final MantisWorkerMetadata mwmdr = store.replaceTerminatedWorker(request, mwmd);
//                queueTask(request, when);
//                logger.info("Resubmitted worker " + status.getWorkerNumber() + " with " + mwmdr.getWorkerNumber() +
//                        " for index " + mwmd.getWorkerIndex() + " of job " + jobId);
//                numWorkerResubmissions.increment();
//            } catch (IOException | InvalidJobException e) {
//                logger.error("Couldn't submit replacement worker " + mwmd.getWorkerNumber() + " for job " +
//                        jobId + "-worker-" + status.getWorkerIndex() + ": " + e.getMessage(), e);
//            }
//        } else {
//            numWorkerResubmitLimitReached.increment();
//            try {
//                storeWorkerStateAndSendStatus(status.getWorkerNumber(), MantisJobState.Failed, JobCompletedReason.Error,
//                        new Status(jobId, mwmd.getStageNum(), status.getWorkerIndex(), status.getWorkerNumber(), Status.TYPE.ERROR,
//                                getWorkerStringPrefix(status.getStageNum(), status.getWorkerIndex(), status.getWorkerNumber()) +
//                                        " Max resubmissions reached: lost worker ", MantisJobState.Failed));
//                logger.info("Setting job state to failed as well");
//                storeAndMarkJobTerminated(MantisJobState.Failed);
//
//            } catch (IOException e) {
//                logger.error("Can't store state for " + status);
//            }
//        }
//    }
//
//    private void storeAndMarkJobTerminated(final MantisJobState jobState)
//            throws IOException, InvalidJobException, InvalidJobStateChangeException {
//        final long submittedAt = getJobMetadata().getSubmittedAt();
//        final String user = getJobMetadata().getUser();
//        store.storeJobState(jobId, jobState);
//        statusSerializedObserver.onCompleted();
//        resubmitRateLimiter.endJob(jobId);
//        HeartbeatPayloadHandler.getInstance().completeJob(this);
//        schedulingInfoBehaviorSubject.onCompleted();
//        MetricsRegistry.getInstance().remove(metricsName);
//        // perform the following call asynchronously since it is expected that the current method we are in is
//        // holding a job lock due to job state mutation. Where as, the following method is a NamedJob mutation which may
//        // grab a NamedJob lock.
//        Schedulers.from(mantisJobMgrExecutorService).createWorker().schedule(() -> {
//            try {
//                namedJob.jobComplete(MantisJobMgr.this, jobState, submittedAt, user);
//            } catch (Exception e) {
//                logger.error("Error marking jobComplete in NamedJob for job " + jobId);
//            }
//        });
//    }
//
//    private MantisWorkerMetadataWritable getMantisWorkerMetadataWritable(int workerNumber) throws InvalidJobException {
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        return (MantisWorkerMetadataWritable) mjmd.getWorkerByNumber(workerNumber);
//    }
//
//    public MantisWorkerMetadata getWorkerMetadata(int workerNumber, boolean evenIfArchived) {
//        try {
//            return getMantisWorkerMetadataWritable(workerNumber);
//        } catch (InvalidJobException e) {
//            if (evenIfArchived) {
//                try {
//                    return store.getArchivedWorker(jobId, workerNumber);
//                } catch (IOException e1) {
//                    logger.warn("Error getting archived worker " + workerNumber + " for job " + jobId, e1);
//                    return null;
//                }
//            } else
//                return null;
//        }
//    }
//
//    private MantisWorkerMetadata getArchivedWorker(int workerNumber) throws IOException {
//        for (MantisWorkerMetadata mwmd : store.getArchivedWorkers(jobId)) {
//            if (mwmd.getWorkerNumber() == workerNumber)
//                return mwmd;
//        }
//        return null;
//    }
//
//    void handlePersistentWorkerState(int workerIndex, int workerNumber, MantisJobState state) {
//        if (state == MantisJobState.Noop)
//            return;
//        try {
//            MantisWorkerMetadata mwmd = getMantisWorkerMetadataWritable(workerNumber);
//            try (AutoCloseable wLock = getJobMetadata().obtainLock()) {
//                if (mwmd.getState() != state)
//                    logger.info("Skipping monitor check on state for job " + jobId +
//                            " worker " + workerIndex + "-" + workerNumber + " since state (" + state +
//                            ") already changed to " + mwmd.getState());
//                else {
//                    logger.info("Will check on job " + jobId + " worker " + workerIndex + "-" + workerNumber +
//                            " for state " + state);
//                    if (state == MantisJobState.Launched) {
//                        // resubmit worker
//                        resubmitWorker(new Status(jobId, mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber(),
//                                        Status.TYPE.ERROR, getWorkerStringPrefix(mwmd.getStageNum(), workerIndex, workerNumber) +
//                                        " Resubmitting worker stuck in " + state + " state", MantisJobState.Failed),
//                                Optional.empty(),
//                                mwmd.getCluster());
//                    }
//                    // Not handling worker stuck in other states for now. E.g., if stuck in Accepted state for too long
//                }
//            } catch (Exception e) {
//                logger.warn("Unable to obtain lock on job " + jobId + " worker " + mwmd.getWorkerNumber() +
//                        " - " + e.getMessage());
//            }
//        } catch (InvalidJobException e) {
//            try {
//                boolean foundArchived = getArchivedWorker(workerNumber) != null;
//                if (!foundArchived)
//                    logger.error("Error handling persistent worker state of " + state + " for job " + jobId + " worker "
//                            + workerNumber + " - " + e.getMessage());
//            } catch (IOException e1) {
//                logger.warn("Error getting archived workers for job " + jobId);
//            }
//        }
//    }
//
//    public void workerOnDisabledVM(int workerNumber) {
//        logger.info(getJobId() + ": adding worker " + workerNumber + " on disabled vm for job " + jobId);
//        workersOnDisabledVMs.add(workerNumber);
//    }
//
//    void initialize(MantisJobStore store) {
//        this.store = store;
//        MantisJobMetadata mjmd = store.getActiveJob(jobId);
//        if (namedJob.getIsReadyForJobMaster() && isAutoscaled(mjmd))
//            hasJobMaster = true;
//        if (mjmd != null) {
//            workerNumberGenerator.init(store, false);
//            if (!MantisJobState.isTerminalState(mjmd.getState())) {
//                for (MantisStageMetadata msmd : mjmd.getStageMetadata()) {
//                    Map<Integer, WorkerHost> workerHosts = new HashMap<>();
//                    for (MantisWorkerMetadata mwmd : msmd.getWorkerByIndexMetadataSet())
//                        if (MantisJobState.isRunningState(mwmd.getState())) {
//                            ((MantisWorkerMetadataWritable) mwmd).setLastHeartbeatAt(System.currentTimeMillis()); // initialize Heartbeat start
//                            workerHosts.put(mwmd.getWorkerNumber(), new WorkerHost(mwmd.getSlave(), mwmd.getWorkerIndex(), mwmd.getPorts(),
//                                    mwmd.getState(), mwmd.getWorkerNumber(), mwmd.getMetricsPort(), mwmd.getCustomPort()));
//
//                            final WorkerRequest workerRequest = createExistingWorkerRequest(msmd, mwmd.getWorkerIndex(), mwmd.getWorkerNumber(), mjmd.getUser(), mwmd.getCluster());
//                            logger.debug("initializing Running task {}-worker-{}-{}", workerRequest.getJobId(), workerRequest.getWorkerIndex(), workerRequest.getWorkerNumber());
//                            scheduler.initializeRunningWorker(
//                                    createSchedulingRequest(workerRequest, Optional.empty()),
//                                    mwmd.getSlave());
//                        } else if (mwmd.getState() == MantisJobState.Accepted) {
//                            final WorkerRequest workerRequest = createExistingWorkerRequest(msmd, mwmd.getWorkerIndex(), mwmd.getWorkerNumber(), mjmd.getUser(), mwmd.getCluster());
//                            logger.debug("queuing Accepted task {}-worker-{}-{}", workerRequest.getJobId(), workerRequest.getWorkerIndex(), workerRequest.getWorkerNumber());
//                            queueTask(workerRequest);
//                        }
//                    if (msmd.getStageNum() > 0) // for now don't send stage 0 assignments
//                        stageAssignments.put(msmd.getStageNum(), new WorkerAssignments(msmd.getStageNum(), msmd.getNumWorkers(), workerHosts));
//                }
//                if (schedulingInfoBehaviorSubject != null)
//                    schedulingInfoBehaviorSubject.onNext(new JobSchedulingInfo(jobId, stageAssignments));
//            } else {
//                statusSerializedObserver.onCompleted();
//                if (schedulingInfoBehaviorSubject != null)
//                    schedulingInfoBehaviorSubject.onCompleted();
//                MetricsRegistry.getInstance().remove(metricsName);
//            }
//            subscriptionTimeoutSecs = initSubscriptionTimeoutSecs(mjmd);
//        } else
//            logger.error("No job metadata from store for job " + jobId);
//        if (!initialized.compareAndSet(false, true))
//            throw new IllegalStateException("Job " + jobId + " already initialized");
//    }
//
//    private long initSubscriptionTimeoutSecs(MantisJobMetadata mjmd) {
//        if (mjmd.getSla().getDurationType() == MantisJobDurationType.Perpetual)
//            return 0L;
//        return mjmd.getSubscriptionTimeoutSecs() == 0 ?
//                ConfigurationProvider.getConfig().getEphemeralJobUnsubscribedTimeoutSecs() :
//                mjmd.getSubscriptionTimeoutSecs();
//    }
//
//    void initialize(WorkerJobDetails jobDetails, final MantisJobStore store)
//            throws InvalidJobDetailsException {
//        this.store = store;
//        MantisJobMetadata mjmd;
//        try {
//            final SchedulingInfo schedulingInfo = jobDetails.getRequest().getJobDefinition().getSchedulingInfo();
//            if (namedJob.getIsReadyForJobMaster() && isAutoscaled(schedulingInfo)) {
//                setupJobMasterStage(schedulingInfo);
//            }
//            mjmd = store.storeNewJob(jobDetails);
//            logger.info("Stored job " + jobId);
//            MantisAuditLogWriter.getInstance()
//                    .getObserver().onNext(new MantisAuditLogEvent(MantisAuditLogEvent.Type.JOB_SUBMIT, jobDetails.getJobId(), "user=" + jobDetails.getUser()));
//            workerNumberGenerator.init(store, true);
//            subscriptionTimeoutSecs = initSubscriptionTimeoutSecs(mjmd);
//        } catch (IOException | JobAlreadyExistsException e) {
//            logger.error("Error storing new job " + jobDetails.getJobId() + " - " + e.getMessage(), e);
//            throw new InvalidJobDetailsException(e.getMessage(), e);
//        }
//        List<WorkerRequest> workers = getInitialWorkers(jobDetails, mjmd.getSubmittedAt());
//        int beg = 0;
//        while (true) {
//            if (beg >= workers.size())
//                break;
//            int en = beg + Math.min(workerWritesBatchSize, workers.size() - beg);
//            final List<WorkerRequest> workerRequests = workers.subList(beg, en);
//            try {
//                store.storeNewWorkers(workerRequests);
//                workerRequests.forEach(this::queueTask);
//            } catch (InvalidJobException e) {
//                // TODO confirm if this is possible, likely not
//                throw new InvalidJobDetailsException("Unexpected error: " + e.getMessage(), e);
//            } catch (IOException e) {
//                logger.error("Error storing workers of job " + jobId + " - " + e.getMessage(), e);
//            }
//            beg = en;
//        }
//        if (!initialized.compareAndSet(false, true))
//            throw new IllegalStateException("Job " + jobId + " already initialized");
//    }
//
//    private void setupJobMasterStage(SchedulingInfo schedulingInfo) {
//        if (schedulingInfo.forStage(0) == null) {
//            // create stage 0 schedulingInfo only if not already provided
//            final StageSchedulingInfo stageSchedulingInfo = new StageSchedulingInfo(1, getJobMasterMachineDef(),
//                    null, null, // for now, there are no hard or soft constraints
//                    null, false); // jobMaster stage itself is not scaled
//            schedulingInfo.addJobMasterStage(
//                    stageSchedulingInfo
//            );
//        }
//        hasJobMaster = true;
//    }
//
//    public boolean hasJobMaster() {
//        return hasJobMaster;
//    }
//
//    private boolean isAutoscaled(SchedulingInfo schedulingInfo) {
//        for (Map.Entry<Integer, StageSchedulingInfo> entry : schedulingInfo.getStages().entrySet()) {
//            final StageScalingPolicy scalingPolicy = entry.getValue().getScalingPolicy();
//            if (scalingPolicy != null && scalingPolicy.isEnabled()) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    private boolean isAutoscaled(MantisJobMetadata job) {
//        for (MantisStageMetadata s : job.getStageMetadata())
//            if (s.getScalable() && s.getScalingPolicy() != null && s.getScalingPolicy().isEnabled())
//                return true;
//        return false;
//    }
//
//    private MachineDefinition getJobMasterMachineDef() {
//        MasterConfiguration config = ConfigurationProvider.getConfig();
//        return new MachineDefinition(
//                config.getJobMasterCores(), config.getJobMasterMemoryMB(), config.getJobMasterNetworkMbps(),
//                config.getJobMasterDiskMB(), 1
//        );
//    }
//
//    @SuppressWarnings( {"rawtypes", "unchecked"})
//    private List<WorkerRequest> getInitialWorkers(WorkerJobDetails jobDetails, long submittedAt) throws InvalidJobDetailsException {
//        List<WorkerRequest> workerRequests = new LinkedList<>();
//        SchedulingInfo schedulingInfo = jobDetails.getRequest().getJobDefinition().getSchedulingInfo();
//        int totalStages = schedulingInfo.getStages().size();
//        // NOTE: stages are numbered from 1, except when a job master exists, it will be 0.
//        for (int s = 0; s <= totalStages; s++)
//            setupStageWorkers(jobDetails, workerRequests, schedulingInfo, totalStages, s, submittedAt);
//        return workerRequests;
//    }
//
//    private void setupStageWorkers(WorkerJobDetails jobDetails, List<WorkerRequest> workerRequests,
//                                   SchedulingInfo schedulingInfo, int totalStages, int stageNum, long submittedAt) {
//        StageSchedulingInfo stage = schedulingInfo.getStages().get(stageNum);
//        if (stage == null)
//            return; // can happen when stageNum=0 and there is no jobMaster defined
//        int numPorts = stage.getMachineDefinition().getNumPorts();
//        int numInstancesAtStage = stage.getNumberOfInstances();
//        // add worker request for each instance required in stage
//        int stageIndex = 0;
//        for (int i = 0; i < numInstancesAtStage; i++) {
//            int workerNumber = workerNumberGenerator.getNextWorkerNumber();
//            // during initialization worker number and index are identical
//            WorkerRequest workerRequest = new WorkerRequest(stage.getMachineDefinition(),
//                    jobDetails.getJobId(), stageIndex++, workerNumber, jobDetails.getJobJarUrl(),
//                    stageNum, totalStages, numInstancesAtStage,
//                    jobDetails.getJobName(), numPorts,
//                    jobDetails.getRequest().getJobDefinition().getParameters(),
//                    jobDetails.getRequest().getJobDefinition().getJobSla(), stage.getHardConstraints(), stage.getSoftConstraints(),
//                    schedulingInfo, subscriptionTimeoutSecs, getMinRuntimeSecs(), submittedAt, jobDetails.getUser(), Optional.empty());
//            workerRequests.add(workerRequest);
//        }
//    }
//}
