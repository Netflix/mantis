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

package io.mantisrx.master.scheduler;

import akka.actor.ActorRef;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.BatchScheduleRequest;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;
import java.util.Optional;

public class FakeMantisScheduler implements MantisScheduler {

    private final ActorRef jobClusterManagerActor;

    public FakeMantisScheduler(final ActorRef jobClusterManagerActor) {
        this.jobClusterManagerActor = jobClusterManagerActor;
    }

    @Override
    public void scheduleWorkers(BatchScheduleRequest scheduleRequest) {
        // TODO:
    }

    @Override
    public void unscheduleJob(String jobId) {
        // TODO:
    }

    @Override
    public void unscheduleWorker(final WorkerId workerId, final Optional<String> hostname) {
        final WorkerEvent workerCompleted = new WorkerResourceStatus(workerId,
            "fake unschedule worker", WorkerResourceStatus.VMResourceState.COMPLETED);
        jobClusterManagerActor.tell(workerCompleted, ActorRef.noSender());
    }

    @Override
    public void unscheduleAndTerminateWorker(final WorkerId workerId, final Optional<String> hostname) {
        unscheduleWorker(workerId, hostname);
    }

    @Override
    public void updateWorkerSchedulingReadyTime(final WorkerId workerId, final long when) {
        // no-op
    }

    @Override
    public void initializeRunningWorker(final ScheduleRequest scheduleRequest, final String hostname, final String hostID) {
        // no-op
    }

    @Override
    public boolean schedulerHandlesAllocationRetries() {
        return false;
    }
}
