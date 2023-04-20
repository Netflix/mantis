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

import com.mantisrx.common.utils.Services;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.client.SinkConnectionsStatus;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.MasterClientWrapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


public class ConnectToNamedJob {

    private static final Logger logger = LoggerFactory.getLogger(ConnectToNamedJob.class);
    private final static AtomicLong prevDroppedCount = new AtomicLong(0L);
    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";
    @Argument(alias = "n", description = "Job name for submission", required = true)
    private static String jobName;

    public static void main2(final String[] args) {
        List<String> remArgs = Collections.emptyList();
        try {
            remArgs = Args.parse(ConnectToNamedJob.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(SubmitEphemeralJob.class);
            System.exit(1);
        }
        if (remArgs.isEmpty()) {
            System.err.println("Must provide JobId as argument");
            System.exit(1);
        }
        final String jobId = remArgs.get(0);
        Properties properties = new Properties();
        System.out.println("propfile=" + propFile);
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HighAvailabilityServices haServices = HighAvailabilityServicesUtil.createHAServices(
            Configurations.frmProperties(properties, CoreConfiguration.class));
        Services.startAndWait(haServices);
        MasterClientWrapper clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
        clientWrapper.getMasterClientApi()
                .doOnNext(new Action1<MantisMasterGateway>() {
                    @Override
                    public void call(MantisMasterGateway clientApi) {
                        logger.info("************** connecting to schedInfo for job " + jobId);
                        clientApi.schedulingChanges(jobId)
                                .doOnNext(new Action1<JobSchedulingInfo>() {
                                    @Override
                                    public void call(JobSchedulingInfo schedulingInfo) {
                                        final WorkerAssignments workerAssignments = schedulingInfo.getWorkerAssignments().get(1);
                                        for (Map.Entry<Integer, WorkerHost> entry : workerAssignments.getHosts().entrySet()) {
                                            System.out.println("Worker " + entry.getKey() +
                                                    ": state=" + entry.getValue().getState() +
                                                    ", host=" + entry.getValue().getHost() +
                                                    ", port=" + entry.getValue().getPort());
                                        }
                                    }
                                })
                                .subscribe();
                        ;
                    }
                })
                .subscribe();
        //        clientWrapper.getMasterClientApi()
        //                .doOnNext(new Action1<MantisMasterClientApi>() {
        //                    @Override
        //                    public void call(MantisMasterClientApi clientApi) {
        //                        logger.info("************* connecting to namedJob info for " + jobId);
        //                        clientApi.namedJobInfo(jobId)
        //                                .doOnNext(new Action1<NamedJobInfo>() {
        //                                    @Override
        //                                    public void call(NamedJobInfo namedJobInfo) {
        //                                        System.out.println(namedJobInfo.getJobId());
        //                                    }
        //                                })
        //                                .subscribe();
        //                    }
        //                })
        //                .subscribe();
        try {Thread.sleep(10000000);} catch (InterruptedException ie) {}
    }

    public static void main(String[] args) {


        //SinkParameters params = new SinkParameters.Builder().withParameter("filter", "windows8").build();

        final AtomicLong eventCounter = new AtomicLong(0L);
        System.setProperty("log4j.logger.io", "DEBUG");
        try {
            Args.parse(ConnectToNamedJob.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(SubmitEphemeralJob.class);
            System.exit(1);
        }
        Properties properties = new Properties();
        System.out.println("propfile=" + propFile);
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final CountDownLatch latch = new CountDownLatch(1);
        MantisSSEJob job = null;
        try {
            job = new MantisSSEJob.Builder(properties)
                    .name(jobName)
                    .onConnectionReset(new Action1<Throwable>() {
                        @Override
                        public void call(Throwable throwable) {
                            System.err.println("Reconnecting due to error: " + throwable.getMessage());
                        }
                    })
                    .sinkConnectionsStatusObserver(new Observer<SinkConnectionsStatus>() {
                        @Override
                        public void onCompleted() {
                            System.out.println("ConnectionStatusObserver completed");
                        }

                        @Override
                        public void onError(Throwable e) {
                            System.err.println("ConnectionStatusObserver error: " + e.getMessage());
                        }

                        @Override
                        public void onNext(SinkConnectionsStatus status) {
                            System.out.println("ConnectionStatusObserver: receiving from " +
                                    status.getRecevingDataFrom() + ", connected to " +
                                    status.getNumConnected() + " of " + status.getTotal());
                        }
                    })
                    .sinkDataRecvTimeoutSecs(11)
                    //          .sinkParams(params)
                    //.sinkParams(new SinkParameters.Builder().withParameter("subscriptionId", "abc").withParameter("filter", "true").build()) // for zuul source job
                    .buildJobConnector();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //try{Thread.sleep(3000);}catch(InterruptedException ie){}
        System.out.println("Subscribing now");
        Subscription subscription = job.connectAndGet()
                .doOnNext(new Action1<Observable<MantisServerSentEvent>>() {
                    @Override
                    public void call(Observable<MantisServerSentEvent> o) {
                        o
                                .doOnNext(new Action1<MantisServerSentEvent>() {
                                    @Override
                                    public void call(MantisServerSentEvent data) {
                                        logger.info("Got event:  + " + data);
                                        latch.countDown();
                                        //                                        if(eventCounter.incrementAndGet()>4)
                                        //                                            throw new RuntimeException("Test exception");
                                    }
                                })
                                .subscribe();
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.error(throwable.getMessage());
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("Completed");
                        System.exit(0);
                    }
                })
                .subscribe();
        //        Subscription s2 = job.connectAndGetObservable()
        //                .doOnNext(new Action1<ServerSentEvent>() {
        //                    @Override
        //                    public void call(ServerSentEvent event) {
        //                        logger.info("    2nd: Got event: type=" + event.getEventType() + " data: " + event.getEventData());
        //                        latch.countDown();
        //                    }
        //                })
        //                .doOnError(new Action1<Throwable>() {
        //                    @Override
        //                    public void call(Throwable throwable) {
        //                        logger.error(throwable.getMessage());
        //                    }
        //                })
        //                .subscribe();
        try {
            boolean await = latch.await(30, TimeUnit.SECONDS);
            if (await)
                System.out.println("PASSED");
            else
                System.err.println("FAILED!");
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        subscription.unsubscribe();
        System.out.println("Unsubscribed");
        try {Thread.sleep(80000);} catch (InterruptedException ie) {}
        System.exit(0);
    }
}
