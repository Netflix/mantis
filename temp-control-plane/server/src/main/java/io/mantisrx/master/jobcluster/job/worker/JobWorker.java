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

package io.mantisrx.master.jobcluster.job.worker;

import static io.mantisrx.master.events.LifecycleEventsProto.StatusEvent;
import static io.mantisrx.master.events.LifecycleEventsProto.WorkerStatusEvent;
import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.jobcluster.job.IMantisWorkerEventProcessor;
import io.mantisrx.master.jobcluster.job.JobActor;
import io.mantisrx.master.scheduler.WorkerStateAdapter;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.StatusPayloads;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.domain.WorkerRequest;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.persistence.exceptions.InvalidWorkerStateChangeException;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunchFailed;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;
import io.mantisrx.server.master.scheduler.WorkerUnscheduleable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class encapsulates information about a worker of a job.
 */
public class JobWorker implements IMantisWorkerEventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobWorker.class);
    private final IMantisWorkerMetadata metadata;
    private final LifecycleEventPublisher eventPublisher;

    private final Metrics metrics;
    private final MetricGroupId metricsGroupId;

    private final Counter numWorkerLaunched;
    private final Counter numWorkerTerminated;
    private final Counter numWorkerLaunchFailed;
    private final Counter numWorkerUnschedulable;
    private final Counter numWorkersDisabledVM;
    private final Counter numHeartBeatsReceived;
    private final Gauge lastWorkerLaunchToStartMillis;

    /**
     * Creates an instance of JobWorker.
     * @param metadata The {@link IMantisWorkerMetadata} for this worker.
     * @param eventPublisher A {@link LifecycleEventPublisher} where lifecycle events are to be sent.
     */
    public JobWorker(final IMantisWorkerMetadata metadata,
                     final LifecycleEventPublisher eventPublisher) {
        Preconditions.checkNotNull(metadata, "metadata");
        this.metadata = metadata;
        this.eventPublisher = eventPublisher;
        this.metricsGroupId = new MetricGroupId("JobWorker", new BasicTag("jobId", this.metadata.getJobId()));

        Metrics m = new Metrics.Builder()
                .id(metricsGroupId)
                .addCounter("numWorkerLaunched")
                .addCounter("numWorkerTerminated")
                .addCounter("numWorkerLaunchFailed")
                .addCounter("numWorkerUnschedulable")
                .addCounter("numWorkersDisabledVM")
                .addCounter("numHeartBeatsReceived")
                .addGauge("lastWorkerLaunchToStartMillis")
                .build();

        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.numWorkerLaunched = metrics.getCounter("numWorkerLaunched");
        this.numWorkerTerminated = metrics.getCounter("numWorkerTerminated");
        this.numWorkerLaunchFailed = metrics.getCounter("numWorkerLaunchFailed");
        this.numWorkerUnschedulable = metrics.getCounter("numWorkerUnschedulable");
        this.numWorkersDisabledVM = metrics.getCounter("numWorkersDisabledVM");
        this.numHeartBeatsReceived = metrics.getCounter("numHeartBeatsReceived");
        this.lastWorkerLaunchToStartMillis = metrics.getGauge("lastWorkerLaunchToStartMillis");
    }

    public IMantisWorkerMetadata getMetadata() {
        return metadata;
    }

    // Setters on mutable metadata
    private MantisWorkerMetadataImpl mutableMetadata() {
        if (metadata instanceof MantisWorkerMetadataImpl) {
            return (MantisWorkerMetadataImpl) metadata;
        } else {
            throw new IllegalStateException();
        }
    }

    private void setState(WorkerState newState, long when, JobCompletedReason reason)
            throws InvalidWorkerStateChangeException {
        mutableMetadata().setState(newState, when, reason);
    }

    private void setLastHeartbeatAt(long lastHeartbeatAt) {
        mutableMetadata().setLastHeartbeatAt(lastHeartbeatAt);
    }

    private void setSlave(String slave) {
        mutableMetadata().setSlave(slave);
    }

    private void setSlaveID(String slaveID) {
        mutableMetadata().setSlaveID(slaveID);
    }

    private void setCluster(Optional<String> cluster) {
        mutableMetadata().setCluster(cluster);
    }

    /**
     * Marks the worker as being subscribed.
     * @param isSub
     */
    void setIsSubscribed(boolean isSub) {
        mutableMetadata().setIsSubscribed(isSub);
    }

    /**
     * Adds the associated ports data.
     * @param ports
     */
    void addPorts(final WorkerPorts ports) {
        mutableMetadata().addPorts(ports);
    }

    // Worker Event handlers

    /**
     * All events associated to this worker are processed in this method.
     *
     * @param workerEvent The {@link WorkerEvent} associated with this worker.
     *
     * @return boolean indicating whether to a change worth persisting occurred.
     *
     * @throws InvalidWorkerStateChangeException thrown if the worker event lead to an invalid state transition.
     */
    public boolean processEvent(WorkerEvent workerEvent) throws InvalidWorkerStateChangeException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing event {} for worker {}", workerEvent, metadata.getWorkerId());
        }

        boolean persistStateRequired = false;
        if (workerEvent instanceof WorkerLaunched) {
            persistStateRequired = onWorkerLaunched((WorkerLaunched) workerEvent);
        } else if (workerEvent instanceof WorkerLaunchFailed) {
            persistStateRequired = onWorkerLaunchFailed((WorkerLaunchFailed) workerEvent);
        } else if (workerEvent instanceof WorkerUnscheduleable) {
            persistStateRequired = onWorkerUnscheduleable((WorkerUnscheduleable) workerEvent);
        } else if (workerEvent instanceof WorkerResourceStatus) {
            persistStateRequired = onWorkerResourceStatus((WorkerResourceStatus) workerEvent);
        } else if (workerEvent instanceof WorkerHeartbeat) {
            persistStateRequired = onHeartBeat((WorkerHeartbeat) workerEvent);
        } else if (workerEvent instanceof WorkerTerminate) {
            persistStateRequired = onTerminate((WorkerTerminate) workerEvent);
        } else if (workerEvent instanceof WorkerOnDisabledVM) {
            persistStateRequired = onDisabledVM((WorkerOnDisabledVM) workerEvent);
        } else if (workerEvent instanceof WorkerStatus) {
            persistStateRequired = onWorkerStatus((WorkerStatus) workerEvent);
        }

        return persistStateRequired;
    }

    private boolean onWorkerStatus(WorkerStatus workerEvent) throws InvalidWorkerStateChangeException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on WorkerStatus for {}", workerEvent);
        }
        switch (workerEvent.getState()) {
            case StartInitiated:
            case Started:
            case Completed:
            case Failed:
                setState(workerEvent.getState(), workerEvent.getEventTimeMs(), workerEvent.getStatus().getReason());
                eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                        StatusEvent.StatusEventType.INFO,
                    "worker status update", metadata.getStageNum(), workerEvent.getWorkerId(),
                        workerEvent.getState()));
                return true;
            case Launched:
            case Accepted:
            case Noop:
            case Unknown:
            default:
                LOGGER.warn("unexpected worker state {} in WorkerStatus update", workerEvent.getState().name());
                break;
        }
        return false;
    }

    private boolean onDisabledVM(WorkerOnDisabledVM workerEvent) {
        numWorkersDisabledVM.increment();
        LOGGER.info("on WorkerDisabledVM for {}", workerEvent);
        return false;
    }

    private boolean onTerminate(WorkerTerminate workerEvent) throws InvalidWorkerStateChangeException {
        numWorkerTerminated.increment();
        setState(workerEvent.getFinalState(), workerEvent.getEventTimeMs(), workerEvent.getReason());
        eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                StatusEvent.StatusEventType.INFO,
            "worker terminated", -1, workerEvent.getWorkerId(), WorkerState.Failed,
                ofNullable(metadata.getSlave())));
        return true;
    }

    /**
     * Updates this {@link JobWorker}'s metadata from a {@link WorkerLaunched} event received by Mesos.
     * This method will update metadata followed by updating the worker's state via
     * {@link JobWorker#setState(WorkerState, long, JobCompletedReason)}. If any of the metadata
     * fails to save, an {@link InvalidWorkerStateChangeException} is thrown and eventually bubbled up to the
     * corresponding {@link JobActor} to handle.
     *
     * @param workerEvent an event received by Mesos with worker metadata after it was launched.
     *
     * @return {@code true} if all saving state succeeds
     *         {@code false} otherwise (and don't durably persist; expect worker to be relaunched because
     *         our state doesn't match Mesos)
     */
    private boolean onWorkerLaunched(WorkerLaunched workerEvent) throws InvalidWorkerStateChangeException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing for worker {} with id {}", workerEvent, metadata.getWorkerId());
        }

        setSlave(workerEvent.getHostname());
        addPorts(workerEvent.getPorts());
        setSlaveID(workerEvent.getVmId());
        setCluster(workerEvent.getClusterName());
        setState(WorkerState.Launched, workerEvent.getEventTimeMs(), JobCompletedReason.Normal);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Worker {} state changed to Launched", workerEvent.getWorkerId());
        }
        numWorkerLaunched.increment();

        try {
            eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                    StatusEvent.StatusEventType.INFO,
                "scheduled on " + workerEvent.getHostname() + " with ports "
                        + Jackson.toJson(workerEvent.getPorts()), workerEvent.getStageNum(),
                workerEvent.getWorkerId(), WorkerState.Launched));
        } catch (IOException e) {
            LOGGER.warn("Error publishing status event for worker {} launch", workerEvent.getWorkerId(), e);
        }

        return true;
    }

    // handle worker status update from Mesos
    private boolean onWorkerResourceStatus(final WorkerResourceStatus workerEvent)
            throws InvalidWorkerStateChangeException {

        WorkerState workerStateFromEvent = WorkerStateAdapter.from(workerEvent.getState());
        // if worker current state is terminated, but we get a resource update from Mesos
        // saying worker is still running, terminate the task
        if (WorkerState.isRunningState(workerStateFromEvent)) {
            if (WorkerState.isTerminalState(metadata.getState())) {
                numWorkerTerminated.increment();
                // kill worker
            }
        }
        // Resource status is terminal but our metadata shows worker as running => update our worker state
        // based on event and
        if (WorkerState.isTerminalState(workerStateFromEvent)) {
            if (!WorkerState.isTerminalState(metadata.getState())) {
                LOGGER.info("Worker {} state changed to {}", this, workerEvent.getState());
                setState(workerStateFromEvent, workerEvent.getEventTimeMs(), JobCompletedReason.Normal);
                eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                        StatusEvent.StatusEventType.INFO,
                    "worker resource state " + workerEvent.getMessage(), -1,
                        workerEvent.getWorkerId(), workerStateFromEvent, ofNullable(metadata.getSlave())));
                return true;
            }
        }
        return false;
    }


    /**
     * Handles a {@link WorkerHeartbeat} event.
     *
     * Assumptions:
     *
     * 1. Heartbeats from workers of terminated jobs are ignored at a higher level.
     *
     * @param workerEvent a {@link WorkerHeartbeat} event.
     *
     * @throws InvalidWorkerStateChangeException if it fails to persist worker state
     *         via {@link JobWorker#setState(WorkerState, long, JobCompletedReason)}.
     */
    private boolean onHeartBeat(WorkerHeartbeat workerEvent) throws InvalidWorkerStateChangeException {
        numHeartBeatsReceived.increment();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Job {} Processing onHeartBeat for {}", this.metadata.getJobId(),
                metadata.getWorkerId());
        }

        WorkerState workerState = metadata.getState();
        setLastHeartbeatAt(workerEvent.getEventTimeMs());
        boolean persistStateRequired = false;
        if (workerState != WorkerState.Started) {
            setState(WorkerState.Started, workerEvent.getEventTimeMs(), JobCompletedReason.Normal);
            persistStateRequired = true;
            final long startLatency = workerEvent.getEventTimeMs() - metadata.getLaunchedAt();
            if (startLatency > 0) {
                lastWorkerLaunchToStartMillis.set(startLatency);
            } else {
                LOGGER.info("Unexpected error when computing startlatency for {} start time {} launch time {}",
                        workerEvent.getWorkerId().getId(), workerEvent.getEventTimeMs(), metadata.getLaunchedAt());
            }
            LOGGER.info("Job {} Worker {} started ", metadata.getJobId(), metadata.getWorkerId());
            eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                    StatusEvent.StatusEventType.INFO,
                "setting worker Started on heartbeat", workerEvent.getStatus().getStageNum(),
                workerEvent.getWorkerId(), WorkerState.Started, ofNullable(metadata.getSlave())));
        }

        List<Status.Payload> payloads = workerEvent.getStatus().getPayloads();
        for (Status.Payload payload : payloads) {
            if (payload.getType().equals(StatusPayloads.Type.SubscriptionState.toString())) {
                String data = payload.getData();
                try {
                    boolean subscriptionStatus = Boolean.parseBoolean(data);
                    if (getMetadata().getIsSubscribed() != subscriptionStatus) {
                        setIsSubscribed(subscriptionStatus);
                        persistStateRequired = true;
                    }
                } catch (Exception e) {
                    // could not parse subscriptionstatus
                    LOGGER.warn("Exception parsing subscription payload", e);

                }
            }
        }
        return persistStateRequired;
    }

    private boolean onWorkerLaunchFailed(WorkerLaunchFailed workerEvent) throws InvalidWorkerStateChangeException {
        numWorkerLaunchFailed.increment();
        setState(WorkerState.Failed, workerEvent.getEventTimeMs(), JobCompletedReason.Error);
        eventPublisher.publishStatusEvent(new WorkerStatusEvent(
                StatusEvent.StatusEventType.ERROR,
            "worker launch failed, reason: " + workerEvent.getErrorMessage(), workerEvent.getStageNum(),
                workerEvent.getWorkerId(), WorkerState.Failed));
        return true;
    }

    private boolean onWorkerUnscheduleable(WorkerUnscheduleable workerEvent) {
        // we shouldn't reach here for Worker Unscheduleable events, as Job Actor would update the readyAt time
        // in the JobActor on receiving this event
        numWorkerUnschedulable.increment();
        return true;
    }

    /**
     * Processes a {@link WorkerEvent} and if successful, saves/update state in the {@link MantisJobStore}.
     *
     * @param event a worker event which can be one of many event types such as launched, heartbeat, etc.
     * @param jobStore a place to persist metadata.
     *
     * @throws InvalidWorkerStateChangeException if a worker failed to persist its state.
     * @throws IOException if the job store failed to update the worker metadata.
     */
    @Override
    public void processEvent(final WorkerEvent event, final MantisJobStore jobStore)
            throws InvalidWorkerStateChangeException, IOException {

        if (event.getWorkerId().equals(this.metadata.getWorkerId())) {
            boolean persistStateRequired = processEvent(event);
            if (persistStateRequired) {
                jobStore.updateWorker(this.metadata);
            }
        } else {

            LOGGER.warn("Current workerId is " + this.metadata.getWorkerId()
                            + " event received from workerId " + event.getWorkerId() + " ignoring");

            // pbly event from an old worker number
        }
    }

    /**
     * Builder to enable fluid creation of a {@link JobWorker}.
     */
    public static class Builder {
        private static final int INVALID_VALUE = -1;
        private int workerIndex = INVALID_VALUE;
        private int workerNumber = INVALID_VALUE;
        private String jobId = null;
        private int stageNum = INVALID_VALUE;
        private int numberOfPorts = INVALID_VALUE;
        private WorkerPorts workerPorts = null;
        private WorkerState state = WorkerState.Accepted;
        private String slave = null;
        private String slaveID = null;
        private long acceptedAt = System.currentTimeMillis();
        private long launchedAt = -1;
        private long startingAt = -1;
        private long startedAt = -1;
        private long completedAt = -1;

        private JobCompletedReason reason = JobCompletedReason.Normal;
        private int resubmitOf = 0;
        private int totalResubmitCount = 0;
        private Optional<String> preferredCluster = Optional.empty();
        private IMantisWorkerMetadata metadata;
        private LifecycleEventPublisher eventPublisher;

        /**
         * Default constructor.
         */
        public Builder() {

        }

        /**
         * Required. WorkerIndex of this worker.
         * @param ind
         * @return
         */
        public JobWorker.Builder withWorkerIndex(int ind) {
            this.workerIndex = ind;
            return this;
        }

        /**
         * Required. Worker number associated with this worker.
         * @param num
         * @return
         */
        public JobWorker.Builder withWorkerNumber(int num) {
            this.workerNumber = num;
            return this;
        }

        /**
         * Optional. Resubmit count associated with this workerIndex.
         * @param c
         * @return
         */
        public JobWorker.Builder withResubmitCount(int c) {
            this.totalResubmitCount = c;
            return this;
        }

        /**
         * Optional. If this is a resubmit of an old worker then the Worker Number of the old worker.
         * @param r
         * @return
         */
        public JobWorker.Builder withResubmitOf(int r) {
            this.resubmitOf = r;
            return this;
        }

        /**
         * Required. Job id for this worker.
         * @param jid
         * @return
         */

        public JobWorker.Builder withJobId(String jid) {
            this.jobId = jid;
            return this;
        }

        /**
         * Required (if String version not used). {@link JobId} of the job of this worker.
         * @param jid
         * @return
         */
        public JobWorker.Builder withJobId(JobId jid) {
            this.jobId = jid.getId();
            return this;
        }

        /**
         * Required. Stage number for this worker.
         * @param num
         * @return
         */
        public JobWorker.Builder withStageNum(int num) {
            this.stageNum = num;
            return this;
        }

        /**
         * Required. Number of ports to be assigned to this worker.
         * @param portNums
         * @return
         */
        public JobWorker.Builder withNumberOfPorts(int portNums) {
            this.numberOfPorts = portNums;
            return this;
        }

        /**
         * Required. Details of the ports assigned to this worker.
         * @param workerP
         * @return
         */
        public JobWorker.Builder withWorkerPorts(WorkerPorts workerP) {
            this.workerPorts = workerP;
            return this;
        }

        /**
         * Optional. The {@link WorkerState} associated with this worker.
         * @param state
         * @return
         */
        public JobWorker.Builder withState(WorkerState state) {
            this.state = state;
            return this;
        }

        /**
         * (Optional) Mesos Slave on which this worker is executing.
         * @param slave
         * @return
         */
        public JobWorker.Builder withSlave(String slave) {
            this.slave = slave;
            return this;
        }

        /**
         * (Optional) Mesos slave Id on which this worker is executing.
         * @param slaveid
         * @return
         */
        public JobWorker.Builder withSlaveID(String slaveid) {
            this.slaveID = slaveid;
            return this;
        }

        /**
         * (Optional) The timestamp this worker went into accepted state.
         * @param acc
         * @return
         */
        public JobWorker.Builder withAcceptedAt(long acc) {
            this.acceptedAt = acc;
            return this;
        }

        /**
         * (Optional) The timestamp this worker went into launched state.
         * @param la
         * @return
         */
        public JobWorker.Builder withLaunchedAt(long la) {
            this.launchedAt = la;
            return this;
        }

        /**
         * (Optional) The timestamp this worker went into starting state.
         * @param sa
         * @return
         */
        public JobWorker.Builder withStartingAt(long sa) {
            this.startingAt = sa;
            return this;
        }

        /**
         * (Optional) The timestamp this worker went into started state.
         * @param sa
         * @return
         */
        public JobWorker.Builder withStartedAt(long sa) {
            this.startedAt = sa;
            return this;
        }

        /**
         * (Optional) The timestamp this worker went into terminal state.
         * @param ca
         * @return
         */
        public JobWorker.Builder withCompletedAt(long ca) {
            this.completedAt = ca;
            return this;
        }


        /**
         * (Optional) The preferred cluster where this worker should be scheduled.
         * @param preferredCluster
         * @return
         */
        public JobWorker.Builder withPreferredCluster(Optional<String> preferredCluster) {
            this.preferredCluster = preferredCluster;
            return this;
        }

        /**
         * (Optional) The reason for worker termination.
         * @param reason
         * @return
         */
        public JobWorker.Builder withJobCompletedReason(JobCompletedReason reason) {
            this.reason = reason;
            return this;
        }

        /**
         * (Required) The listener where worker lifecycle events are published.
         * @param publisher
         * @return
         */
        public JobWorker.Builder withLifecycleEventsPublisher(LifecycleEventPublisher publisher) {
            this.eventPublisher = publisher;
            return this;
        }

        /**
         * Helper builder which clones from an instance of {@link IMantisWorkerMetadata} object.
         * @param cloneFrom
         * @return
         */
        public JobWorker.Builder from(IMantisWorkerMetadata cloneFrom) {
            workerIndex = cloneFrom.getWorkerIndex();
            workerNumber = cloneFrom.getWorkerNumber();
            jobId = cloneFrom.getJobId();
            stageNum = cloneFrom.getStageNum();
            numberOfPorts = cloneFrom.getNumberOfPorts();
            if (cloneFrom.getPorts().isPresent()) {
                workerPorts = cloneFrom.getPorts().get();
            }
            state = cloneFrom.getState();
            slave = cloneFrom.getSlave();
            slaveID = cloneFrom.getSlaveID();
            acceptedAt = cloneFrom.getAcceptedAt();
            launchedAt = cloneFrom.getLaunchedAt();
            startingAt = cloneFrom.getStartingAt();
            startedAt = cloneFrom.getStartedAt();
            completedAt = cloneFrom.getCompletedAt();

            reason = cloneFrom.getReason();
            resubmitOf = cloneFrom.getResubmitOf();
            totalResubmitCount = cloneFrom.getTotalResubmitCount();
            preferredCluster = cloneFrom.getPreferredClusterOptional();

            return this;
        }

        /**
         * Helper builder that clones from given {@link WorkerRequest}.
         * @param workerRequest
         * @return
         */
        public JobWorker.Builder from(WorkerRequest workerRequest) {
            this.workerIndex = workerRequest.getWorkerIndex();
            this.workerNumber = workerRequest.getWorkerNumber();
            this.jobId =  workerRequest.getJobId();
            this.stageNum = workerRequest.getWorkerStage();
            this.numberOfPorts =  workerRequest.getNumPortsPerInstance();

            this.preferredCluster = workerRequest.getPreferredCluster();

            return this;
        }

        /**
         * Creates and returns an instance of {@link JobWorker}.
         * @return
         */
        public JobWorker build() {
            Objects.requireNonNull(jobId, "Job Id cannot be null");

            if (workerIndex <= INVALID_VALUE) {
                IllegalArgumentException ex = new IllegalArgumentException(
                        String.format("Invalid workerIndex {} specified", workerIndex));
                LOGGER.error("Invalid worker index specified {}", workerIndex, ex);
                throw ex;
            }
            if (workerNumber <= INVALID_VALUE) {
                LOGGER.error("Invalid worker number specified {}", workerNumber);
                throw new IllegalArgumentException(String.format("Invalid workerNumber {} specified", workerNumber));
            }
            if (stageNum <= INVALID_VALUE) {
                LOGGER.error("Invalid stage num specified {}", stageNum);
                throw new IllegalArgumentException(String.format("Invalid stageNum {} specified", stageNum));
            }
            if (numberOfPorts <= INVALID_VALUE) {
                LOGGER.error("Invalid num ports specified {}", numberOfPorts);
                throw new IllegalArgumentException(String.format("Invalid no of Ports {} specified", numberOfPorts));
            }
            if (totalResubmitCount < 0) {
                LOGGER.error("Invalid resubmit count specified {}", totalResubmitCount);
                throw new IllegalArgumentException(
                        String.format("Invalid resubmit Count {} specified", totalResubmitCount));
            }

            if (eventPublisher == null) {
                IllegalArgumentException ex = new IllegalArgumentException(
                        String.format("lifecycle event publisher cannot be null"));
                LOGGER.error("lifecycle event publisher is null", ex);
                throw ex;
            }
            this.metadata = new MantisWorkerMetadataImpl(workerIndex,
                workerNumber,
                jobId,
                stageNum,
                numberOfPorts,
                workerPorts,
                state,
                slave,
                slaveID,
                acceptedAt,
                launchedAt,
                startingAt,
                startedAt,
                completedAt,
                reason,
                resubmitOf,
                totalResubmitCount,
                preferredCluster
            );
            return new JobWorker(this.metadata, this.eventPublisher);
        }

    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final JobWorker jobWorker = (JobWorker) o;
        return Objects.equals(metadata, jobWorker.metadata)
                && Objects.equals(eventPublisher, jobWorker.eventPublisher);
    }

    @Override
    public int hashCode() {

        return Objects.hash(metadata, eventPublisher);
    }

    @Override
    public String toString() {
        return "JobWorker{" + "metadata=" + metadata + '}';
    }
}
