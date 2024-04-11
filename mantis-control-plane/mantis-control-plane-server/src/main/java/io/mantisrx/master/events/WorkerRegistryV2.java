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

package io.mantisrx.master.events;

import static java.util.stream.Collectors.toMap;

import akka.actor.Props;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.master.events.LifecycleEventsProto.WorkerStatusEvent;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Actor holds a registry of all running workers for all jobs in the system.
 * The Job Actor sends a message with a complete snapshot of running workers to the LifeCycleEventPublisher
 * The LifeCycleEventPublisher then forwards them to this Actor.
 */
public class WorkerRegistryV2 implements WorkerRegistry, WorkerEventSubscriber {
    private final Logger logger = LoggerFactory.getLogger(WorkerRegistryV2.class);
    private final ConcurrentMap<JobId, List<IMantisWorkerMetadata>> jobToWorkerInfoMap = new ConcurrentHashMap<>();

    public static final WorkerRegistryV2 INSTANCE = new WorkerRegistryV2();
    private final Metrics metrics;
    private final Counter numStatusEvents;
    public static Props props() {
        return Props.create(WorkerRegistryV2.class);
    }

     WorkerRegistryV2() {
        logger.info("WorkerRegistryV2 created");
         Metrics m = new Metrics.Builder()
             .id("WorkerRegistryMetrics")
             .addCounter("numStatusEvents")
             .build();
         this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
         this.numStatusEvents = metrics.getCounter("numStatusEvents");
    }


    /**
     * Iterate through all jobs and addup the worker list size for each
     * @return
     */
    @Override
    public int getNumRunningWorkers(@Nullable ClusterID resourceCluster) {
        if(logger.isDebugEnabled()) { logger.debug("In getNumRunningWorkers"); }
        int cnt = jobToWorkerInfoMap.values().stream()
                    .map(workerList -> workerList.stream()
                            .filter(wm -> Optional.ofNullable(resourceCluster).equals(wm.getResourceCluster()))
                            .filter(wm -> WorkerState.isRunningState(wm.getState()))
                            .collect(Collectors.toList())
                            .size()
                    )
                    .reduce(0,(a, b) -> a + b);
        if(logger.isDebugEnabled()) { logger.debug("Returning {} from getNumRunningWorkers", cnt); }
        return cnt;
    }

    /**
     * Return a Set of all running workers in the system
     * @return
     */

    @Override
    public Set<WorkerId> getAllRunningWorkers(@Nullable ClusterID resourceCluster) {

        return jobToWorkerInfoMap.values().stream()
            .flatMap(workerList -> workerList.stream()
                    .filter(wm -> Optional.ofNullable(resourceCluster).equals(wm.getResourceCluster()))
                    .filter(wm -> WorkerState.isRunningState(wm.getState()))
                    .map(workerMeta -> workerMeta.getWorkerId()))
            .collect(Collectors.toSet());

    }

    /**
     * Return a mapping of workerId to slaveID for all running workers in the system
     * @return
     */
    @Override
    public Map<WorkerId, String> getAllRunningWorkerSlaveIdMappings(@Nullable ClusterID resourceCluster) {
        return
            jobToWorkerInfoMap.values().stream()
                .flatMap(workerList ->
                    workerList.stream()
                        .filter(wm -> Optional.ofNullable(resourceCluster).equals(wm.getResourceCluster()))
                        .filter(wm -> WorkerState.isRunningState(wm.getState())))
                .collect(toMap(
                    IMantisWorkerMetadata::getWorkerId,
                    IMantisWorkerMetadata::getSlaveID,
                    (s1, s2) -> (s1 != null) ? s1 : s2));
    }

    /**
     * Check whether a workerId is valid
     * @param workerId
     * @return
     */
    @Override
    public boolean isWorkerValid(WorkerId workerId) {
        if(logger.isDebugEnabled()) {  logger.debug("In isWorkerValid event {}", workerId); }
        Optional<JobId> jIdOp = JobId.fromId(workerId.getJobId());
        if(!jIdOp.isPresent()) {
            logger.warn("Invalid job Id {}", workerId.getJobId());
            return false;
        }
        List<IMantisWorkerMetadata> mantisWorkerMetadataList = jobToWorkerInfoMap.get(jIdOp.get());
        boolean isValid = false;
        if(mantisWorkerMetadataList != null) {

            isValid = mantisWorkerMetadataList.stream().anyMatch((mData) -> mData.getWorkerId().equals(workerId));
        } else {
            logger.warn("No such job {} found in job To worker map ", jIdOp.get());
        }
        return isValid;
    }

    /**
     * Return the accepted At time for the given worker
     * @param workerId
     * @return
     */
    @Override
    public Optional<Long> getAcceptedAt(WorkerId workerId) {
        if(logger.isDebugEnabled()) {  logger.debug("In getAcceptedAt for worker {}", workerId); }
        Optional<JobId> jId = JobId.fromId(workerId.getJobId());
        if(!jId.isPresent()) {
            return Optional.empty();
        }
        List<IMantisWorkerMetadata> mantisWorkerMetadataList = jobToWorkerInfoMap.get(jId.get());
        if(mantisWorkerMetadataList != null) {

            Optional<IMantisWorkerMetadata> mantisWorkerMetadata = mantisWorkerMetadataList.stream().filter(mData -> mData.getWorkerId().equals(workerId)).findAny();
            if (mantisWorkerMetadata.isPresent()) {
                logger.info("Found worker {} return acceptedAt {}", workerId, mantisWorkerMetadata.get().getAcceptedAt());
                return Optional.of(mantisWorkerMetadata.get().getAcceptedAt());
            }
        }
        return Optional.empty();
    }


    /**
     * When the worker info subject completes this method is invoked to clean up state.
     * @param jobId
     * @return
     */

    private boolean deregisterJob(JobId jobId) {
        logger.info("De-registering {}", jobId);
        return jobToWorkerInfoMap.remove(jobId) != null;
    }

    @Override
    public void process(LifecycleEventsProto.WorkerListChangedEvent event) {
        if(logger.isDebugEnabled()) { logger.debug("on WorkerListChangedEvent for job {} with workers {}", event.getWorkerInfoListHolder().getJobId(), event.getWorkerInfoListHolder().getWorkerMetadataList().size()); }
        JobId jId = event.getWorkerInfoListHolder().getJobId();
        jobToWorkerInfoMap.put(jId, event.getWorkerInfoListHolder().getWorkerMetadataList());

    }

    @Override
    public void process(LifecycleEventsProto.JobStatusEvent statusEvent) {
        if(logger.isDebugEnabled()) {  logger.debug("In JobStatusEvent {}", statusEvent); }
        this.numStatusEvents.increment();
        JobState jobState = statusEvent.getJobState();
        if(JobState.isTerminalState(jobState)) {
            final JobId jobId = statusEvent.getJobId();
            deregisterJob(jobId);
        }
    }

    @Override
    public void process(WorkerStatusEvent workerStatusEvent) {
        this.numStatusEvents.increment();
    }
}
