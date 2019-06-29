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

package io.mantisrx.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;


public class StageWorkersCount {

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";
    @Argument(alias = "j", description = "Specify job Id", required = true)
    private static String jobIdString = "";
    private final String jobId;
    private final MantisClient mantisClient;

    public StageWorkersCount(String jobId, MantisClient mantisClient) {
        this.jobId = jobId;
        this.mantisClient = mantisClient;
    }

    public static void main(String[] args) {
        try {
            Args.parse(StageWorkersCount.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(StageWorkersCount.class);
            System.exit(1);
        }
        Properties properties = new Properties();
        System.out.println("propfile=" + propFile);
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        StageWorkersCount workersCount = new StageWorkersCount(jobIdString, new MantisClient(properties));
        workersCount.getWorkerCounts(1)
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onCompleted() {
                        System.out.println("Completed");
                        System.exit(0);
                    }

                    @Override
                    public void onError(Throwable e) {
                        System.err.println("Unexpected error: " + e.getMessage());
                        e.printStackTrace();
                    }

                    @Override
                    public void onNext(Integer integer) {
                        System.out.println("#Workers changed to " + integer);
                    }
                });
        try {Thread.sleep(10000000);} catch (InterruptedException ie) {}
    }

    Observable<Integer> getWorkerCounts(final int stageNumber) {
        final AtomicInteger workerCount = new AtomicInteger(0);
        final AtomicBoolean gotCompletion = new AtomicBoolean(false);
        return mantisClient
                .getClientWrapper()
                .getMasterClientApi()
                .flatMap(new Func1<MantisMasterClientApi, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(MantisMasterClientApi mantisMasterClientApi) {
                        return mantisMasterClientApi
                                .schedulingChanges(jobId)
                                .map(new Func1<JobSchedulingInfo, Integer>() {
                                    @Override
                                    public Integer call(JobSchedulingInfo jobSchedulingInfo) {
                                        final WorkerAssignments assignments = jobSchedulingInfo.getWorkerAssignments().get(stageNumber);
                                        if (assignments == null)
                                            return -1;
                                        else
                                            return assignments.getNumWorkers();
                                    }
                                })
                                .filter(new Func1<Integer, Boolean>() {
                                    @Override
                                    public Boolean call(Integer newCount) {
                                        if (newCount == workerCount.get())
                                            return false;
                                        workerCount.set(newCount);
                                        return true;
                                    }
                                })
                                .doOnCompleted(new Action0() {
                                    @Override
                                    public void call() {
                                        gotCompletion.set(true);
                                    }
                                });
                    }
                })
                .takeWhile(new Func1<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) {
                        return !gotCompletion.get();
                    }
                });
    }
}
