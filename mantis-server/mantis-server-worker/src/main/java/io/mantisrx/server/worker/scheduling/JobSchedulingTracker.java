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

package io.mantisrx.server.worker.scheduling;

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.GroupedObservable;


public class JobSchedulingTracker {

    private static final Logger logger = LoggerFactory.getLogger(JobSchedulingTracker.class);
    private Observable<JobSchedulingInfo> schedulingChangesForJobId;

    public JobSchedulingTracker(Observable<JobSchedulingInfo> schedulingChangesForJobId) {
        this.schedulingChangesForJobId = schedulingChangesForJobId;
    }

    public Observable<WorkerIndexChange> startedWorkersPerIndex(int stageNumber) {
        Observable<WorkerIndexChange> workerIndexChanges = workerIndexChanges(stageNumber);
        return workerIndexChanges
                .filter(new Func1<WorkerIndexChange, Boolean>() {
                    @Override
                    public Boolean call(WorkerIndexChange newWorkerChange) {
                        return (newWorkerChange.getNewState().getState()
                                == MantisJobState.Started);
                    }
                });
    }

    public Observable<WorkerIndexChange> workerIndexChanges(int stageNumber) {
        return
                workerChangesForStage(stageNumber, schedulingChangesForJobId)
                        // flatmap over all numbered workers
                        .flatMap(new Func1<WorkerAssignments, Observable<WorkerHost>>() {
                            @Override
                            public Observable<WorkerHost> call(WorkerAssignments assignments) {
                                logger.info("Received scheduling update from master: " + assignments);
                                return Observable.from(assignments.getHosts().values());
                            }
                        })
                        // group by index
                        .groupBy(new Func1<WorkerHost, Integer>() {
                            @Override
                            public Integer call(WorkerHost workerHost) {
                                return workerHost.getWorkerIndex();
                            }
                        })
                        //
                        .flatMap(new Func1<GroupedObservable<Integer, WorkerHost>, Observable<WorkerIndexChange>>() {
                            @Override
                            public Observable<WorkerIndexChange> call(
                                    final GroupedObservable<Integer, WorkerHost> workerIndexGroup) {
                                // seed sequence, to support buffer by 2
                                return
                                        workerIndexGroup.startWith(new WorkerHost(null, -1, null, null, -1, -1, -1))
                                                .buffer(2, 1) // create pair to compare prev and curr
                                                .filter(new Func1<List<WorkerHost>, Boolean>() {
                                                    @Override
                                                    public Boolean call(List<WorkerHost> currentAndPrevious) {
                                                        if (currentAndPrevious.size() < 2) {
                                                            return false; // not a pair, last element
                                                            // has already been evaluated on last iteration
                                                            // for example: 1,2,3,4,5 = (1,2),(2,3),(3,4),(4,5),(5)
                                                        }
                                                        WorkerHost previous = currentAndPrevious.get(0);
                                                        WorkerHost current = currentAndPrevious.get(1);
                                                        return (previous.getWorkerNumber() != current.getWorkerNumber());
                                                    }
                                                })
                                                .map(new Func1<List<WorkerHost>, WorkerIndexChange>() {
                                                    @Override
                                                    public WorkerIndexChange call(List<WorkerHost> list) {
                                                        return new WorkerIndexChange(workerIndexGroup.getKey(),
                                                                list.get(1), list.get(0));
                                                    }
                                                });
                            }
                        });
    }

    private Observable<WorkerAssignments> workerChangesForStage(final int stageNumber,
                                                                Observable<JobSchedulingInfo> schedulingUpdates) {
        return schedulingUpdates
                // pull out worker assignments from jobSchedulingInfo
                .flatMap(new Func1<JobSchedulingInfo, Observable<WorkerAssignments>>() {
                    @Override
                    public Observable<WorkerAssignments> call(JobSchedulingInfo schedulingChange) {
                        Map<Integer, WorkerAssignments> assignments = schedulingChange.getWorkerAssignments();
                        if (assignments != null && !assignments.isEmpty()) {
                            return Observable.from(assignments.values());
                        } else {
                            return Observable.empty();
                        }
                    }
                })
                // return only changes from previous stage
                .filter(new Func1<WorkerAssignments, Boolean>() {
                    @Override
                    public Boolean call(WorkerAssignments assignments) {
                        return (assignments.getStage() == stageNumber);
                    }
                });
    }
}
