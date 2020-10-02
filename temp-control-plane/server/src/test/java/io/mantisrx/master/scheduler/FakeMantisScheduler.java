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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import akka.actor.ActorRef;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.master.jobcluster.job.worker.WorkerHeartbeat;
import io.mantisrx.master.jobcluster.job.worker.WorkerStatus;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.scheduler.WorkerEvent;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;

public class FakeMantisScheduler implements MantisScheduler {

    private final ActorRef jobClusterManagerActor;

    public FakeMantisScheduler(final ActorRef jobClusterManagerActor) {
        this.jobClusterManagerActor = jobClusterManagerActor;
    }

    @Override
    public void scheduleWorker(final ScheduleRequest scheduleRequest) {
        // Worker Launched
        final WorkerEvent workerLaunched = new WorkerLaunched(scheduleRequest.getWorkerId(),
            scheduleRequest.getStageNum(),
            "host1",
            "vm1",
            scheduleRequest.getPreferredCluster(), new WorkerPorts(Lists.newArrayList(8000, 9000, 9010, 9020, 9030)));

        jobClusterManagerActor.tell(workerLaunched, ActorRef.noSender());

        // fake Worker Start initiated event
        final WorkerEvent workerStartInit = new WorkerStatus(new Status(
            scheduleRequest.getWorkerId().getJobId(),
            scheduleRequest.getStageNum(),
            scheduleRequest.getWorkerId().getWorkerIndex(),
            scheduleRequest.getWorkerId().getWorkerNum(),
            Status.TYPE.INFO,
            "fake Start Initiated",
            MantisJobState.StartInitiated));

        jobClusterManagerActor.tell(workerStartInit, ActorRef.noSender());

        // fake Worker Heartbeat event
        final WorkerEvent workerHeartbeat = new WorkerHeartbeat(new Status(
            scheduleRequest.getWorkerId().getJobId(),
            scheduleRequest.getStageNum(),
            scheduleRequest.getWorkerId().getWorkerIndex(),
            scheduleRequest.getWorkerId().getWorkerNum(),
            Status.TYPE.HEARTBEAT,
            "fake heartbeat event",
            MantisJobState.Started));
        jobClusterManagerActor.tell(workerHeartbeat, ActorRef.noSender());
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
    public void initializeRunningWorker(final ScheduleRequest scheduleRequest, final String hostname) {
        // no-op
    }

    @Override
    public void rescindOffer(final String offerId) {
        // TBD
    }

    @Override
    public void rescindOffers(final String hostname) {
        // TBD
    }

    @Override
    public void addOffers(final List<VirtualMachineLease> offers) {
        // TBD
    }

    @Override
    public void disableVM(final String hostname, final long durationMillis) throws IllegalStateException {
        // TBD
    }

    @Override
    public void enableVM(final String hostname) {
        // TBD
    }

    @Override
    public List<VirtualMachineCurrentState> getCurrentVMState() {
        // TBD
        return Collections.emptyList();
    }

    @Override
    public void setActiveVmGroups(final List<String> activeVmGroups) {
        // TBD
    }
}
