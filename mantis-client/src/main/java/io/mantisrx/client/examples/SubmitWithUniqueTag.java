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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.JobSla;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;


public class SubmitWithUniqueTag {

    private static final Logger logger = LoggerFactory.getLogger(SubmitWithUniqueTag.class);

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    @Argument(alias = "n", description = "Job name for submission", required = false)
    private static String jobName;

    public static void main(String[] args) {
        try {
            Args.parse(SubmitWithUniqueTag.class, args);
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
        final JobSla jobSla = new JobSla.Builder()
                .withUniqueJobTagValue("foobar")
                .build();
        MantisSSEJob job = new MantisSSEJob.Builder(properties)
                .jobSla(jobSla)
                .name(jobName)
                .buildJobSubmitter();
        final Observable<Observable<MantisServerSentEvent>> o = job.submitAndGet();
        final CountDownLatch latch = new CountDownLatch(5);
        final Subscription subscribe = o
                .doOnNext(new Action1<Observable<MantisServerSentEvent>>() {
                    @Override
                    public void call(Observable<MantisServerSentEvent> eventObservable) {
                        eventObservable
                                .doOnNext(new Action1<MantisServerSentEvent>() {
                                    @Override
                                    public void call(MantisServerSentEvent event) {
                                        System.out.println("event: " + event.getEventAsString());
                                        latch.countDown();
                                    }
                                })
                                .subscribe();
                    }
                })
                .subscribe();
        try {
            if (latch.await(50, TimeUnit.SECONDS))
                System.out.println("SUCCESS");
            else
                System.out.println("FAILURE");
            subscribe.unsubscribe();
            System.exit(0);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
