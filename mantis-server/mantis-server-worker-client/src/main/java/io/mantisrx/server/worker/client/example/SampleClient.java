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

package io.mantisrx.server.worker.client.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.server.worker.client.MetricsClient;
import io.mantisrx.server.worker.client.SseWorkerConnectionFunction;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


// Simple throw-away implementation to show one way to use MantisClient
public class SampleClient {

    private static final Logger logger = LoggerFactory.getLogger(SampleClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";
    @Argument(alias = "j", description = "Specify a job Id", required = false)
    private static String jobId;

    //    @Argument(alias = "s", description = "Filename containing scheduling information", required = false)
    //    private static String schedulingInfoFile;
    @Argument(alias = "c", description = "Command to run submit", required = true)
    private static String cmd;

    public static void main(String[] args) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Args.parse(SampleClient.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(SampleClient.class);
            System.exit(1);
        }
        System.out.println("propfile=" + propFile);
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final WorkerMetricsClient mantisClient = new WorkerMetricsClient(properties);

        try {
            switch (cmd) {
            case "metrics":
                if (jobId == null || jobId.isEmpty()) {
                    logger.error("Must provide jobId to connect to its metrics sink");
                } else {
                    Thread thread = new Thread() {
                        @Override
                        public void run() {
                            getMetricsData(mantisClient, jobId);
                        }
                    };
                    thread.start();
                }
                break;
            default:
                logger.error("Unknown command " + cmd);
                break;
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private static void getMetricsData(WorkerMetricsClient mantisClient, final String localJobId) {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final MetricsClient metricsClient = mantisClient.getMetricsClientByJobId(localJobId,
                new SseWorkerConnectionFunction(true, new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.error("Sink connection error: " + throwable.getMessage());
                        try {Thread.sleep(500);} catch (InterruptedException ie) {
                            logger.error("Interrupted waiting for retrying connection");
                        }
                    }
                }), null);
        logger.info("Getting results observable for job {}", localJobId);
        Observable<MantisServerSentEvent> resultsObservable = Observable.merge(metricsClient
                .getResults());
        logger.info("Subscribing to it");
        final AtomicReference<Subscription> ref = new AtomicReference<>(null);
        final Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {sleep(300000);} catch (InterruptedException ie) {}
                logger.info("Closing client conx");
                try {
                    ref.get().unsubscribe();
                    finishLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.setDaemon(true);
        final Subscription s = resultsObservable
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        finishLatch.countDown();
                    }
                })
                .subscribe(new Action1<MantisServerSentEvent>() {
                    @Override
                    public void call(MantisServerSentEvent event) {
                        if (startLatch.getCount() > 0) {
                            startLatch.countDown();
                        }
                        logger.info("{} Got SSE: {}", localJobId, event.getEventAsString());
                    }
                });
        ref.set(s);
        t.start();
        logger.info("SUBSCRIBED to job metrics changes");
        try {
            finishLatch.await();
            logger.info("Sink observable completed");
        } catch (InterruptedException e) {
            logger.error("thread interrupted", e);
        }
    }

}
