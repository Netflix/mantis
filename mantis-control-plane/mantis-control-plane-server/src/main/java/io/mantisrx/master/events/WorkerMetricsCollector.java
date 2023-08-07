/*
 * Copyright 2022 Netflix, Inc.
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Gauge;
import io.mantisrx.master.events.LifecycleEventsProto.JobStatusEvent;
import io.mantisrx.master.events.LifecycleEventsProto.WorkerListChangedEvent;
import io.mantisrx.master.events.LifecycleEventsProto.WorkerStatusEvent;
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractScheduledService;
import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import io.netty.util.internal.ConcurrentSet;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * The goal of this service is to keep emitting metrics around how long it took for the worker to be
 * scheduled, to be prepared and how long it was running for.
 */
@RequiredArgsConstructor
@Slf4j
public class WorkerMetricsCollector extends AbstractScheduledService implements
    WorkerEventSubscriber {

    private final ConcurrentMap<JobId, Map<WorkerId, IMantisWorkerMetadata>> jobWorkers =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WorkerMetrics> clusterWorkersMetrics =
        new ConcurrentHashMap<>();
    private final ConcurrentSet<CleanupJobEvent> jobsToBeCleaned = new ConcurrentSet<>();
    private final Duration cleanupInterval;

    private final Duration epochDuration;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    private final WorkerMetricsCollectorMetrics workerMetricsCollectorMetrics = new WorkerMetricsCollectorMetrics(meterRegistry);

    @Override
    protected void runOneIteration() {
        Instant current = clock.instant();
        Iterator<CleanupJobEvent> iterator = jobsToBeCleaned.iterator();
        while (iterator.hasNext()) {
            CleanupJobEvent event = iterator.next();
            if (current.isAfter(event.getExpiry())) {
                jobWorkers.remove(event.getJobId());
                iterator.remove();
            }
        }
        workerMetricsCollectorMetrics.reportJobWorkersSize(jobWorkers.size());
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(
            epochDuration.toMillis(),
            epochDuration.toMillis(),
            TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(WorkerListChangedEvent event) {
        final JobId jobId = event.getWorkerInfoListHolder().getJobId();
        final List<IMantisWorkerMetadata> workers =
            event.getWorkerInfoListHolder().getWorkerMetadataList();
        jobWorkers.put(jobId,
            workers.stream().collect(Collectors.toMap(IMantisWorkerMetadata::getWorkerId, m -> m)));
        workerMetricsCollectorMetrics.reportJobWorkersSize(jobWorkers.size());
    }

    @Override
    public void process(JobStatusEvent statusEvent) {
        if (statusEvent.getJobState().isTerminal()) {
            cleanUp(statusEvent.getJobId());
        }
    }

    private WorkerMetrics getWorkerMetrics(String clusterName) {
        return clusterWorkersMetrics.computeIfAbsent(
            clusterName, dontCare -> new WorkerMetrics(clusterName, meterRegistry));
    }

    @Override
    public void process(WorkerStatusEvent workerStatusEvent) {
        try {
            final WorkerState workerState =
                workerStatusEvent.getWorkerState();
            final JobId jobId = JobId.fromId(workerStatusEvent.getWorkerId().getJobId()).get();
            final WorkerId workerId = workerStatusEvent.getWorkerId();
            Preconditions.checkNotNull(jobWorkers.get(jobId));
            final IMantisWorkerMetadata metadata = jobWorkers.get(jobId).get(workerId);
            if (metadata == null) {
                log.warn("Unknown workerId: {} for metrics collector in job: {}", workerId, jobId);
                return;
            }

            final WorkerMetrics workerMetrics = getWorkerMetrics(
                metadata.getResourceCluster().map(ClusterID::getResourceID).orElse("mesos"));

            switch (workerState) {
                case Accepted:
                    // do nothing; This is the initial state
                    break;
                case Launched:
                    // this represents the scheduling time
                    workerMetrics.reportSchedulingDuration(
                        Math.max(0L, workerStatusEvent.getTimestamp() - metadata.getAcceptedAt()));
                    break;
                case StartInitiated:
                    // do nothing; this event gets sent when the worker has received the request; it's too granular and expected to be really low -
                    // so there's no point in measuring it.
                    break;
                case Started:
                    workerMetrics.reportWorkerPreparationDuration(
                        Math.max(0L, workerStatusEvent.getTimestamp() - metadata.getLaunchedAt()));
                    break;
                case Failed:
                case Completed:
                    workerMetrics.reportRunningDuration(
                        Math.max(0L, workerStatusEvent.getTimestamp() - metadata.getStartedAt()));
                    break;
                case Noop:
                    break;
                case Unknown:
                    log.error("Unknown WorkerStatusEvent {}", workerStatusEvent);
                    break;
            }
        } catch (Exception e) {
            log.error("Failed to process worker status event {}", workerStatusEvent, e);
        }
    }

    private void cleanUp(JobId jobId) {
        jobsToBeCleaned.add(new CleanupJobEvent(jobId, clock.instant().plus(cleanupInterval)));
    }

    private static class WorkerMetrics {

        public static final String SCHEDULING_DURATION = "schedulingDuration";
        public static final String PREPARATION_DURATION = "preparationDuration";
        public static final String RUNNING_DURATION = "runningDuration";
        private final Timer schedulingDuration;
        private final Timer preparationDuration;
        private final Timer runningDuration;
        private final MeterRegistry meterRegistry;

        public WorkerMetrics(final String clusterName, MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            Tags tags = Tags.of("cluster", clusterName, "resourceCluster", clusterName);
            schedulingDuration = meterRegistry.timer("WorkerMetricsCollector_" + SCHEDULING_DURATION, tags);
            preparationDuration = meterRegistry.timer("WorkerMetricsCollector_" + PREPARATION_DURATION, tags);
            runningDuration = meterRegistry.timer("WorkerMetricsCollector_" + RUNNING_DURATION, tags);

        }


        private void reportSchedulingDuration(long durationInMillis) {
            this.schedulingDuration.record(durationInMillis, TimeUnit.MILLISECONDS);
        }

        private void reportWorkerPreparationDuration(long durationInMillis) {
            this.preparationDuration.record(durationInMillis, TimeUnit.MILLISECONDS);
        }

        private void reportRunningDuration(long durationInMillis) {
            this.runningDuration.record(durationInMillis, TimeUnit.MILLISECONDS);
        }
    }

    private static class WorkerMetricsCollectorMetrics {

        public static final String JOB_WORKERS_MAP_SIZE = "jobWorkersMapSize";
        private final Gauge jobWorkersMapSize;
        private final AtomicLong jobWorkersMapSizeValue = new AtomicLong(0);
        private final MeterRegistry registry;


        public WorkerMetricsCollectorMetrics(MeterRegistry meterRegistry) {
            this.registry = meterRegistry;
            jobWorkersMapSize = Gauge.builder("WorkerMetricsCollector_" + JOB_WORKERS_MAP_SIZE, jobWorkersMapSizeValue::get)
                .register(meterRegistry);
        }

        private void reportJobWorkersSize(int size) {
            jobWorkersMapSizeValue.set(size);
        }
    }

    @Value
    private static class CleanupJobEvent {

        JobId jobId;
        Instant expiry;
    }
}
