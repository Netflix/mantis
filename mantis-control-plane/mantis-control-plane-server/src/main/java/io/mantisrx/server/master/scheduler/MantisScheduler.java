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

import io.mantisrx.server.core.domain.WorkerId;
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
     * scheduler started running. For example, when the scheduling service
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
     * This should return true if the underlying scheduler handles retrying worker allocations.
     *
     * @return If there are not enough resources to schedule the worker and the scheduler automatically retries until
     * the worker is assigned, then return true; otherwise, return false.
     */
    boolean schedulerHandlesAllocationRetries();
}
