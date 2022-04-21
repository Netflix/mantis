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

package io.mantisrx.server.master.scheduler;

import akka.actor.ActorRef;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.scheduler.ResourceClusterAwareSchedulerActor.CancelRequestEvent;
import io.mantisrx.server.master.scheduler.ResourceClusterAwareSchedulerActor.InitializeRunningWorkerRequestEvent;
import io.mantisrx.server.master.scheduler.ResourceClusterAwareSchedulerActor.ScheduleRequestEvent;
import io.mantisrx.shaded.com.google.common.base.Throwables;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ResourceClusterAwareScheduler implements MantisScheduler {

    private final ActorRef schedulerActor;

    @Override
    public void scheduleWorker(ScheduleRequest scheduleRequest) {
        schedulerActor.tell(ScheduleRequestEvent.of(scheduleRequest), null);
    }

    @Override
    public void unscheduleWorker(WorkerId workerId, Optional<String> hostname) {
        throw new UnsupportedOperationException(
            "This seems to be used only within the SchedulingService which is a MantisScheduler implementation itself; so it's not clear if this is needed or not");
    }

    @Override
    public void unscheduleAndTerminateWorker(WorkerId workerId,
                                             Optional<String> hostname) {
        schedulerActor.tell(CancelRequestEvent.of(workerId, hostname.orElse(null)), null);

        if (!hostname.isPresent()) {
            log.error("Request for cancelling worker {} without hostname", workerId, new Exception());
        }
    }

    @Override
    public void updateWorkerSchedulingReadyTime(WorkerId workerId, long when) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializeRunningWorker(ScheduleRequest scheduleRequest, String hostname, String hostID) {
        log.info("initializeRunningWorker called for {} and {}", scheduleRequest, hostname);
        schedulerActor.tell(
            new InitializeRunningWorkerRequestEvent(scheduleRequest, TaskExecutorID.of(hostID)),
            null);
    }

    @Override
    public void rescindOffer(String offerId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rescindOffers(String hostname) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOffers(List<VirtualMachineLease> offers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableVM(String hostname, long durationMillis)
        throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableVM(String hostname) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<VirtualMachineCurrentState> getCurrentVMState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setActiveVmGroups(List<String> activeVmGroups) {
        log.info("Active VM Groups is {} as per this stack-trace {}", activeVmGroups,
            Throwables.getStackTraceAsString(new Throwable()));
    }
}
