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

import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.Timer;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
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

    private final WorkerMetricsCollectorMetrics workerMetricsCollectorMetrics = new WorkerMetricsCollectorMetrics();

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
            clusterName, dontCare -> new WorkerMetrics(clusterName));
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
                    log.debug("Worker {} launched with scheduling time: {}",
                        workerId, Math.max(0L, workerStatusEvent.getTimestamp() - metadata.getAcceptedAt()));
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

        public WorkerMetrics(final String clusterName) {
            MetricGroupId metricGroupId =
                new MetricGroupId(
                    "WorkerMetricsCollector",
                    Tag.of("cluster", clusterName),
                    Tag.of("resourceCluster", clusterName));
            Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addTimer(SCHEDULING_DURATION)
                .addTimer(PREPARATION_DURATION)
                .addTimer(RUNNING_DURATION)
                .build();

            m = MetricsRegistry.getInstance().registerAndGet(m);
            this.schedulingDuration = m.getTimer(SCHEDULING_DURATION);
            this.preparationDuration = m.getTimer(PREPARATION_DURATION);
            this.runningDuration = m.getTimer(RUNNING_DURATION);
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

        public WorkerMetricsCollectorMetrics() {
            MetricGroupId metricGroupId = new MetricGroupId("WorkerMetricsCollector");

            Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addGauge(JOB_WORKERS_MAP_SIZE)
                .build();

            this.jobWorkersMapSize = m.getGauge(JOB_WORKERS_MAP_SIZE);
        }

        private void reportJobWorkersSize(int size) {
            jobWorkersMapSize.set(size);
        }
    }

    @Value
    private static class CleanupJobEvent {

        JobId jobId;
        Instant expiry;
    }
}
