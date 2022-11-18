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
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractScheduledService;
import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public static final String SCHEDULING_DURATION = "schedulingDuration";
    public static final String PREPARATION_DURATION = "preparationDuration";
    public static final String RUNNING_DURATION = "runningDuration";
    private final ConcurrentMap<JobId, Map<WorkerId, IMantisWorkerMetadata>> jobWorkers =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WorkerMetrics> clusterWorkersMetrics =
        new ConcurrentHashMap<>();
    private final Set<CleanupJobEvent> jobsToBeCleaned = new HashSet<>();
    private final Duration cleanupInterval;

    private final Duration epochDuration;
    private final Clock clock;

    @Override
    protected void runOneIteration() throws Exception {
        Instant expiry = clock.instant().minus(cleanupInterval);
        Set<CleanupJobEvent> toBeRemoved = new HashSet<>();

        synchronized (jobsToBeCleaned) {
            jobsToBeCleaned.forEach(event -> {
                if (event.isAfter(expiry)) {
                    jobWorkers.remove(event.getJobId());
                    toBeRemoved.add(event);
                }
            });
            jobsToBeCleaned.removeAll(toBeRemoved);
        }
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
            Preconditions.checkNotNull(metadata);
            final WorkerMetrics workerMetrics = getWorkerMetrics(metadata.getCluster().orElse("unknown"));

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
            log.error("Failed to process worker status event {}", workerStatusEvent);
        }
    }

    private void cleanUp(JobId jobId) {
        synchronized (jobsToBeCleaned) {
            jobsToBeCleaned.add(new CleanupJobEvent(jobId, clock.instant()));
        }
    }

    private class WorkerMetrics {

        private final String clusterName;
        private final Timer schedulingDuration;
        private final Timer preparationDuration;
        private final Timer runningDuration;

        public WorkerMetrics(final String clusterName) {
            this.clusterName = clusterName;
            MetricGroupId metricGroupId =
                new MetricGroupId("WorkerMetricsCollector", Tag.of("cluster", clusterName));
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

    @Value
    private static class CleanupJobEvent {

        JobId jobId;
        Instant eventTimestamp;

        boolean isAfter(Instant expiry) {
            return eventTimestamp.isAfter(expiry);
        }
    }
}
