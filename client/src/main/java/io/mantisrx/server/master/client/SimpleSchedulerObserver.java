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

package io.mantisrx.server.master.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.server.core.JobAssignmentResult;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;


public class SimpleSchedulerObserver {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";
    @Argument(alias = "j", description = "Specify a jobId", required = false)
    private static String jobId = "";
    private final MasterClientWrapper clientWrapper;

    SimpleSchedulerObserver(Properties properties) {
        clientWrapper = new MasterClientWrapper(properties);
    }

    public static void main(String[] args) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new Jdk8Module());
        try {
            Args.parse(SimpleSchedulerObserver.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(SimpleSchedulerObserver.class);
            System.exit(1);
        }
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Listening to scheduling assignments with jobId=" + jobId);
        final CountDownLatch latch = new CountDownLatch(1);
        SimpleSchedulerObserver schedulerObserver = new SimpleSchedulerObserver(properties);
        final AtomicReference<JobAssignmentResult> ref = new AtomicReference<>(null);
        schedulerObserver.getObservable(jobId)
                .filter(new Func1<JobAssignmentResult, Boolean>() {
                    @Override
                    public Boolean call(JobAssignmentResult jobAssignmentResult) {
                        if (jobAssignmentResult == null)
                            return false;
                        if (jobAssignmentResult.isIdentical(ref.get()))
                            return false;
                        ref.set(jobAssignmentResult);
                        return true;
                    }
                })
                .doOnNext(new Action1<JobAssignmentResult>() {
                    @Override
                    public void call(JobAssignmentResult jobAssignmentResult) {
                        System.out.println("Failures for job " + jobAssignmentResult.getJobId() + ":");
                        for (JobAssignmentResult.Failure failure : jobAssignmentResult.getFailures())
                            try {
                                System.out.println("  " + objectMapper.writeValueAsString(failure));
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        latch.countDown();
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        throwable.printStackTrace();
                        latch.countDown();
                    }
                })
                .subscribe();
        System.out.println("Subscribed.");
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    Observable<JobAssignmentResult> getObservable(final String jobId) {
        return clientWrapper
                .getMasterClientApi()
                .flatMap(new Func1<MantisMasterClientApi, Observable<? extends JobAssignmentResult>>() {
                    @Override
                    public Observable<? extends JobAssignmentResult> call(MantisMasterClientApi mantisMasterClientApi) {
                        return mantisMasterClientApi.assignmentResults(jobId);
                    }
                });
    }
}
