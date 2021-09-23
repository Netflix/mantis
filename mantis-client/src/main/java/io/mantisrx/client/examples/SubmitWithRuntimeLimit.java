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

package io.mantisrx.client.examples;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobDurationType;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;


public class SubmitWithRuntimeLimit {

    private static final Logger logger = LoggerFactory.getLogger(SubmitWithRuntimeLimit.class);

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    @Argument(alias = "n", description = "Job name for submission", required = false)
    private static String jobName;

    public static void main(String[] args) {
        try {
            Args.parse(SubmitWithRuntimeLimit.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(SubmitEphemeralJob.class);
            System.exit(1);
        }
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final long runtimeLimitSecs = 30;
        MantisSSEJob job = new MantisSSEJob.Builder(properties)
                .name(jobName)
                .jobSla(new JobSla(runtimeLimitSecs, 0L, JobSla.StreamSLAType.Lossy, MantisJobDurationType.Perpetual, ""))
                .onConnectionReset(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        System.err.println("Reconnecting due to error: " + throwable.getMessage());
                    }
                })
                .buildJobSubmitter();
        final Observable<Observable<MantisServerSentEvent>> observable = job.submitAndGet();
        final Subscription subscription = observable
                .flatMap(new Func1<Observable<MantisServerSentEvent>, Observable<?>>() {
                    @Override
                    public Observable<?> call(Observable<MantisServerSentEvent> eventObservable) {
                        return eventObservable
                                .doOnNext(new Action1<MantisServerSentEvent>() {
                                    @Override
                                    public void call(MantisServerSentEvent event) {
                                        if (completed.get())
                                            System.out.println("FAILURE");
                                        System.out.println("Got: " + event.getEventAsString());
                                    }
                                });
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        latch.countDown();
                    }
                })
                .subscribe();
        try {
            Thread.sleep((runtimeLimitSecs + 10) * 1000); // add a buffer for job launch time
            completed.set(true); // set expectation of complete
            Thread.sleep(20000); // give some time to see if we still get data, which would be a failure

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        subscription.unsubscribe();
        System.exit(0);
    }
}
