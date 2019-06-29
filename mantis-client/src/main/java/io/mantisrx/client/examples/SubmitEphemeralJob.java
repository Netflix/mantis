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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


// Example to show how a short ephemeral job can be submitted and auto killed when done.
public class SubmitEphemeralJob {

    private static final Logger logger = LoggerFactory.getLogger(SubmitEphemeralJob.class);

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    @Argument(alias = "n", description = "Job name for submission", required = false)
    private static String jobName;

    public static void main(String[] args) {
        try {
            Args.parse(SubmitEphemeralJob.class, args);
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
        // AutoCloseable job will terminate job if onCloseKillJob() called when building it
        Subscription subscription1 = null;
        //        Subscription s2=null;
        MantisSSEJob job1 = new MantisSSEJob.Builder(properties)
                .name(jobName)
                //                .parameters(new Parameter("param1", "val1"), new Parameter("param2", "val2"))
                //                .jarVersion("1.2")
                .onCloseKillJob()
                .onConnectionReset(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        System.err.println("Reconnecting due to error: " + throwable.getMessage());
                    }
                })
                .buildJobSubmitter();
        //        MantisSSEJob job2 = new MantisSSEJob.Builder(properties)
        //                .name(jobName)
        ////                .parameters(new Parameter("param1", "val1"), new Parameter("param2", "val2"))
        ////                .jarVersion("1.2")
        //                .onCloseKillJob()
        //                .onConnectionReset(new Action1<Throwable>() {
        //                    @Override
        //                    public void call(Throwable throwable) {
        //                        System.err.println("Reconnecting due to error: " + throwable.getMessage());
        //                    }
        //                })
        //                .buildJobSubmitter();
        try {
            Observable<Observable<MantisServerSentEvent>> observable = job1.submitAndGet();
            subscription1 = observable
                    .doOnNext(new Action1<Observable<MantisServerSentEvent>>() {
                        @Override
                        public void call(Observable<MantisServerSentEvent> o) {
                            o.doOnNext(new Action1<MantisServerSentEvent>() {
                                @Override
                                public void call(MantisServerSentEvent data) {
                                    logger.info("Got event: " + data);
                                    latch.countDown();
                                }
                            }).subscribe();
                        }
                    })
                    .doOnCompleted(new Action0() {
                        @Override
                        public void call() {
                            System.out.println("Observable completed!!!");
                        }
                    })
                    .subscribe();
            if (latch.await(50, TimeUnit.SECONDS))
                System.out.println("SUCCESS");
            else
                System.out.println("FAILURE");
            //            final Observable<Observable<MantisServerSentEvent>> o2 = job2.submitAndGet();
            //            s2 = o2
            //                    .doOnNext(new Action1<Observable<MantisServerSentEvent>>() {
            //                        @Override
            //                        public void call(Observable<MantisServerSentEvent> o) {
            //                            o.doOnNext(new Action1<MantisServerSentEvent>() {
            //                                @Override
            //                                public void call(MantisServerSentEvent event) {
            //                                    logger.info("        Got event:  " + event);
            //                                }
            //                            }).subscribe();
            //                        }
            //                    })
            //                    .doOnCompleted(new Action0() {
            //                        @Override
            //                        public void call() {
            //                            System.out.println("Observable completed!!!");
            //                        }
            //                    })
            //                    .subscribe();
            if (latch.await(50, TimeUnit.SECONDS))
                System.out.println("SUCCESS");
            else
                System.out.println("FAILURE");
            Thread.sleep(30000000);
            subscription1.unsubscribe(); // unsubscribe to close connection to sink
            //            s2.unsubscribe();
            job1.close();
            //            job2.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
