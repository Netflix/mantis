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

package io.mantisrx.server.worker;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.WorkerMap;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;


public class WorkerExecutionOperationsNetworkStageTest {

    @Test
    public void convertJobSchedulingInfoToWorkerMapTest() {

        String jobName = "convertJobSchedulingInfoToWorkerMapTest";
        String jobId = jobName + "-1";

        MantisJobDurationType durationType = MantisJobDurationType.Perpetual;


        WorkerAssignments workerAssignmentsStage1 = createWorkerAssignments(1, 2);
        WorkerAssignments workerAssignmentsStage2 = createWorkerAssignments(2, 4);
        Map<Integer, WorkerAssignments> workerAssignmentsMap = new HashMap<>();
        workerAssignmentsMap.put(1, workerAssignmentsStage1);
        workerAssignmentsMap.put(2, workerAssignmentsStage2);

        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(jobId, workerAssignmentsMap);

        WorkerMap workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(jobName, jobId, durationType, jobSchedulingInfo);

        List<WorkerInfo> workersForStage1 = workerMap.getWorkersForStage(1);

        assertTrue(workersForStage1 != null);
        assertEquals(2, workersForStage1.size());

        for (int i = 0; i < workersForStage1.size(); i++) {
            WorkerInfo workerInfo = workersForStage1.get(i);
            assertEquals(i, workerInfo.getWorkerIndex());
            assertEquals(i + 1, workerInfo.getWorkerNumber());
            assertEquals(durationType, workerInfo.getDurationType());
            assertEquals(i + 2, workerInfo.getWorkerPorts().getMetricsPort());
            assertEquals(i + 3, workerInfo.getWorkerPorts().getCustomPort());
        }

        List<WorkerInfo> workersForStage2 = workerMap.getWorkersForStage(2);

        assertTrue(workersForStage2 != null);
        assertEquals(4, workersForStage2.size());

        for (int i = 0; i < workersForStage2.size(); i++) {
            WorkerInfo workerInfo = workersForStage2.get(i);
            assertEquals(i, workerInfo.getWorkerIndex());
            assertEquals(i + 1, workerInfo.getWorkerNumber());
            assertEquals(durationType, workerInfo.getDurationType());
            assertEquals(i + 2, workerInfo.getWorkerPorts().getMetricsPort());
            assertEquals(i + 3, workerInfo.getWorkerPorts().getCustomPort());
        }


    }


    @Test
    public void convertJobSchedulingInfoToWorkerMapInvalidInputTest() {

        String jobName = "convertJobSchedulingInfoToWorkerMapInvalidInputTest";
        String jobId = jobName + "-1";

        MantisJobDurationType durationType = MantisJobDurationType.Perpetual;


        WorkerAssignments workerAssignmentsStage1 = createWorkerAssignments(1, 2);
        WorkerAssignments workerAssignmentsStage2 = createWorkerAssignments(2, 4);
        Map<Integer, WorkerAssignments> workerAssignmentsMap = new HashMap<>();
        workerAssignmentsMap.put(1, workerAssignmentsStage1);
        workerAssignmentsMap.put(2, workerAssignmentsStage2);

        JobSchedulingInfo jobSchedulingInfo = new JobSchedulingInfo(jobId, workerAssignmentsMap);

        WorkerMap workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(null, jobId, durationType, jobSchedulingInfo);

        assertTrue(workerMap.isEmpty());

        workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(jobName, null, durationType, jobSchedulingInfo);

        assertTrue(workerMap.isEmpty());

        workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(jobName, jobId, durationType, null);

        assertTrue(workerMap.isEmpty());

        jobSchedulingInfo = new JobSchedulingInfo(jobId, null);

        workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(jobName, jobId, durationType, jobSchedulingInfo);

        assertTrue(workerMap.isEmpty());


        workerAssignmentsMap = new HashMap<>();
        workerAssignmentsMap.put(1, null);
        workerAssignmentsMap.put(2, workerAssignmentsStage2);

        jobSchedulingInfo = new JobSchedulingInfo(jobId, workerAssignmentsMap);

        workerMap = WorkerExecutionOperationsNetworkStage.convertJobSchedulingInfoToWorkerMap(jobName, jobId, durationType, jobSchedulingInfo);

        assertTrue(workerMap.isEmpty());


    }


    WorkerAssignments createWorkerAssignments(int stageNo, int noWorkers) {

        Map<Integer, WorkerHost> workerHostMap = new HashMap<>();
        for (int i = 0; i < noWorkers; i++) {
            List<Integer> ports =
                    ImmutableList.of(i + 1);
            workerHostMap.put(i, new WorkerHost("host" + i, i, ports, MantisJobState.Launched, i + 1, i + 2, i + 3));
        }

        return new WorkerAssignments(stageNo, noWorkers, workerHostMap);
    }

    @Test
    public void deferTest() throws InterruptedException {

        Subscription subscribe1 = getObs4().subscribeOn(Schedulers.io()).subscribe((t) -> {
            System.out.println("In 1 -> " + t);
        });

        Thread.sleep(5000);

        Subscription subscribe2 = getObs4().subscribeOn(Schedulers.io()).subscribe((t) -> {
            System.out.println("In 2 -> " + t);
        });


        Thread.sleep(5000);
        subscribe1.unsubscribe();

        Thread.sleep(5000);
        subscribe2.unsubscribe();

        Thread.sleep(5000);
        Subscription subscribe3 = getObs4().subscribeOn(Schedulers.io()).subscribe((t) -> {
            System.out.println("In 3 -> " + t);
        });
        Thread.sleep(5000);
        subscribe3.unsubscribe();
        Thread.sleep(10000);
    }

    Observable<Long> getObs() {
        Observable<Long> oLong = Observable.defer(() -> {
            return Observable.interval(1, TimeUnit.SECONDS).doOnNext((e) -> {
                System.out.println("Minted " + e);
            }).share();
        }).doOnSubscribe(() -> {
            System.out.println("Subscribed111" + System.currentTimeMillis());
        }).doOnUnsubscribe(() -> {
            System.out.println("UnSubscribed111" + System.currentTimeMillis());
        });
        return oLong;
    }

    Observable<Long> getObs2() {

        return Observable.interval(1, TimeUnit.SECONDS)
                .doOnNext((e) -> {
                    System.out.println("Minted " + e);
                })
                .share()
                .doOnSubscribe(() -> {
                    System.out.println("Subscribed111" + System.currentTimeMillis());
                }).doOnUnsubscribe(() -> {
                    System.out.println("UnSubscribed111" + System.currentTimeMillis());
                })

                ;

    }

    Observable<Long> getObs3() {

        return Observable.range(1, 100).doOnNext((e) -> {
            System.out.println("Minted " + e);
        }).map((i) -> {
            return new Long(i);
        }).share()
                .doOnSubscribe(() -> {
                    System.out.println("Subscribed111" + System.currentTimeMillis());
                }).doOnUnsubscribe(() -> {
                    System.out.println("UnSubscribed111" + System.currentTimeMillis());
                });

    }

    Observable<Long> getObs4() {
        BehaviorSubject<Long> o = BehaviorSubject.create();
        Observable.interval(1, TimeUnit.SECONDS).doOnNext((e) -> {
            System.out.println("Minted " + e);
        }).doOnSubscribe(() -> {
            System.out.println("Subscribed111" + System.currentTimeMillis());
        }).doOnUnsubscribe(() -> {
            System.out.println("UnSubscribed111" + System.currentTimeMillis());
        })
                .subscribe(o);

        return o;

    }


    //    Observable<Long> getObs5() {
    //
    //        Observable.create(new SyncOnSubscribe<BehaviorSubject<Long>, Long>() {
    //
    //            @Override
    //            protected BehaviorSubject<Long> generateState() {
    //                BehaviorSubject<Long> subj = BehaviorSubject.create();
    //                Observable.interval(1, TimeUnit.SECONDS).subscribe(subj);
    //                return subj;
    //            }
    //
    //            @Override
    //            protected BehaviorSubject<Long> next(BehaviorSubject<Long> state, Observer<? super Long> observer) {
    //                 state.subscribe((t) -> {
    //                    observer.onNext(t);
    //                });
    //            }
    //
    //
    //        });
    //
    //    }
}
