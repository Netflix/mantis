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

package io.mantisrx.server.master.scheduler;

import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.core.domain.WorkerId;
import java.util.List;
import java.util.Optional;


public interface MantisScheduler {

    /**
     * Add a worker to the Scheduler queue
     *
     * @param scheduleRequest worker to schedule
     */
    void scheduleWorkers(final BatchScheduleRequest scheduleRequest);

    void unscheduleJob(final String jobId);

    /**
     * Mark the worker to be removed from the Scheduling queue. This is expected to be called for all tasks that were added to the Scheduler, whether or
     * not the worker is already running. If the worker is running, the <code>hostname</code> parameter must be set, otherwise,
     * it can be <code>Optional.empty()</code>. The actual remove operation is performed before the start of the next scheduling
     * iteration.
     *
     * @param workerId The Id of the worker to be removed.
     * @param hostname The name of the VM where the worker was assigned resources from, or, <code>Optional.empty()</code> if it was
     *                 not assigned any resources.
     */
    void unscheduleWorker(final WorkerId workerId, final Optional<String> hostname);

    /**
     * Mark the worker to be removed from the Scheduling queue and terminate the running container. This is expected to be called for all tasks that were added to the Scheduler, whether or
     * not the worker is already running. If the worker is running, the <code>hostname</code> parameter must be set, otherwise,
     * it can be <code>Optional.empty()</code>. The actual remove operation is performed before the start of the next scheduling
     * iteration.
     *
     * @param workerId The Id of the worker to be removed.
     * @param hostname The name of the VM where the worker was assigned resources from, or, <code>Optional.empty()</code> if it was
     *                 not assigned any resources.
     */
    void unscheduleAndTerminateWorker(final WorkerId workerId, final Optional<String> hostname);

    /**
     * Set the wall clock time when this worker is ready for consideration for resource allocation.
     *
     * @param workerId The Id of the task.
     * @param when     The wall clock time in millis when the task is ready for consideration for assignment.
     */
    void updateWorkerSchedulingReadyTime(final WorkerId workerId, final long when);

    /**
     * Mark the given workers as running. This is expected to be called for all workers that were already running from before
     * {@link com.netflix.fenzo.TaskSchedulingService} started running. For example, when the scheduling service
     * is being started after a restart of the system and there were some workers launched in the previous run of
     * the system. Any workers assigned resources during scheduling invoked by this service will be automatically marked
     * as running.
     * <p>
     *
     * @param scheduleRequest The scheduleRequest(worker) to mark as running
     * @param hostname        The name of the VM that the task is running on.
     */
    void initializeRunningWorker(final ScheduleRequest scheduleRequest, final String hostname, final String hostID);

    /**
     * Informs the scheduler that the offer has been revoked. Typically called by the Resource Manager
     *
     * @param offerId ID of the offer being revoked
     */
    void rescindOffer(final String offerId);

    /**
     * Informs the scheduler to reject all offers for this hostname.
     *
     * @param hostname host
     */
    void rescindOffers(final String hostname);

    /**
     * Informs the scheduler of new offers received from the Resource Manager
     *
     * @param offers new offers from Resource Manager
     */
    void addOffers(final List<VirtualMachineLease> offers);

    /**
     * Reject offers from this host for durationMillis
     *
     * @param hostname       host to disable
     * @param durationMillis duration in milliseconds
     *
     * @throws IllegalStateException
     */
    void disableVM(final String hostname, final long durationMillis) throws IllegalStateException;

    /**
     * Enable a host to allow using its resource offers for task assignment, only required if the host was explicitly disabled
     *
     * @param hostname host to enable
     */
    void enableVM(final String hostname);

    /**
     * Get the current states of all known VMs.
     */
    List<VirtualMachineCurrentState> getCurrentVMState();

    /**
     * Set the list of VM group names that are active. VMs (hosts) that belong to groups that you do not include
     * in this list are considered disabled. The scheduler does not use the resources of disabled hosts when it
     * allocates tasks. If you pass in a null list, this indicates that the scheduler should consider all groups
     * to be enabled.
     *
     * @param activeVmGroups a list of VM group names that the scheduler is to consider to be enabled, or {@code null}
     *                       if the scheduler is to consider every group to be enabled
     */
    void setActiveVmGroups(final List<String> activeVmGroups);
}
