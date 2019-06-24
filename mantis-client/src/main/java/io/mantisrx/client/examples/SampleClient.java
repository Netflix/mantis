///*
// * Copyright 2019 Netflix, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.mantisrx.client.examples;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.Properties;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicReference;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
//import com.sampullara.cli.Args;
//import com.sampullara.cli.Argument;
//import io.mantisrx.client.MantisClient;
//import io.mantisrx.client.SinkClient;
//import io.mantisrx.client.SseSinkConnectionFunction;
//import io.mantisrx.common.MantisServerSentEvent;
////import io.mantisrx.master.api.proto.CreateJobClusterRequest;
////import io.mantisrx.master.api.proto.UpdateJobClusterRequest;
////import io.mantisrx.master.core.proto.JobDefinition;
////import io.mantisrx.master.core.proto.JobOwner;
//import io.mantisrx.runtime.JobOwner;
//import io.mantisrx.runtime.JobSla;
//import io.mantisrx.runtime.MantisJobDurationType;
//import io.mantisrx.runtime.descriptor.SchedulingInfo;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observable;
//import rx.Subscription;
//import rx.functions.Action0;
//import rx.functions.Action1;
//
//
//// Simple throw-away implementation to show one way to use MantisClient
//public class SampleClient {
//
//    private static final Logger logger = LoggerFactory.getLogger(SampleClient.class);
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//    @Argument(alias = "p", description = "Specify a configuration file", required = true)
//    private static String propFile = "";
//    @Argument(alias = "j", description = "Specify a job Id", required = false)
//    private static String jobId;
//    @Argument(alias = "c", description = "Command to run create/submit/sink", required = true)
//    private static String cmd;
//    @Argument(alias = "n", description = "Job name for submission", required = false)
//    private static String jobName;
//    @Argument(alias = "u", description = "Job JAR URL for submission", required = false)
//    private static String jobUrl;
//    @Argument(alias = "s", description = "Filename containing scheduling information", required = false)
//    private static String schedulingInfoFile;
//    @Argument(alias = "d", description = "Job duration type", required = false)
//    private static String durationTypeString;
//    private static MantisJobDurationType durationType = MantisJobDurationType.Perpetual;
//    private static SchedulingInfo schedulingInfo;
//
//    public static void main(String[] args) {
//        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        objectMapper.registerModule(new Jdk8Module());
//        try {
//            Args.parse(SampleClient.class, args);
//        } catch (IllegalArgumentException e) {
//            Args.usage(SampleClient.class);
//            System.exit(1);
//        }
//        System.out.println("propfile=" + propFile);
//        Properties properties = new Properties();
//        try (InputStream inputStream = new FileInputStream(propFile)) {
//            properties.load(inputStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        final MantisClient mantisClient = new MantisClient(properties);
//        if (durationTypeString != null && !durationTypeString.isEmpty())
//            durationType = MantisJobDurationType.valueOf(durationTypeString);
//        try {
//            switch (cmd) {
//            case "create":
//                if (jobName == null || jobName.isEmpty() || jobUrl == null || jobUrl.isEmpty() ||
//                        schedulingInfoFile == null || schedulingInfoFile.isEmpty()) {
//                    System.err.println("Must provide job name, url, and scheduling info filename for Job Cluster creation");
//                } else {
//                    schedulingInfo = loadSchedulingInfo(schedulingInfoFile);
//                    doCreate(mantisClient);
//                }
//                break;
//            case "update":
//                if (jobName == null || jobName.isEmpty() || jobUrl == null || jobUrl.isEmpty() ||
//                        schedulingInfoFile == null || schedulingInfoFile.isEmpty()) {
//                    System.err.println("Must provide job name, url, and scheduling info filename for Job Cluster update");
//                } else {
//                    schedulingInfo = loadSchedulingInfo(schedulingInfoFile);
//                    doUpdate(mantisClient);
//                }
//                break;
//            case "submit":
//                if (jobName == null || jobName.isEmpty()) {
//                    System.err.println("Must provide job name for submit");
//                } else {
//                    if (schedulingInfoFile != null && !schedulingInfoFile.isEmpty())
//                        schedulingInfo = loadSchedulingInfo(schedulingInfoFile);
//                    jobId = doSubmit(mantisClient);
//                    //System.out.println("Calling sink connect on the job " + jobId);
//                    //doGetSinkData(mantisClient, jobId);
//                }
//                break;
//            case "sink":
//                if (jobId == null || jobId.isEmpty()) {
//                    System.err.println("Must provide jobId to connect to its sink");
//                } else {
//                    //                        Thread t1 = new Thread() {
//                    //                            @Override
//                    //                            public void run() {
//                    //                                doGetSinkData(mantisClient, "ea261597-daf7-4635-b07c-0d0d6f73ca3d");
//                    //                            }
//                    //                        };
//                    Thread t2 = new Thread() {
//                        @Override
//                        public void run() {
//                            doGetSinkData(mantisClient, jobId);
//                        }
//                    };
//                    //t1.start();
//                    t2.start();
//                }
//                break;
//            default:
//                logger.error("Unknown command " + cmd);
//                break;
//            }
//        } catch (Throwable t) {
//            t.printStackTrace();
//        }
//    }
//
//    private static SchedulingInfo loadSchedulingInfo(String schedulingInfoFile) throws IOException {
//        File file = new File(schedulingInfoFile);
//        if (file.canRead()) {
//            return objectMapper.readValue(file, SchedulingInfo.class);
//        } else
//            throw new IOException("Can't read scheduling info file " + schedulingInfoFile);
//    }
//
//    private static void doCreate(MantisClient mantisClient) throws Throwable {
//        JobDefinition jobDefinition = JobDefinition.newBuilder()
//                .setName(jobName)
//                .setVersion("0.0.1")
//                .setJobSla(io.mantisrx.master.core.proto.JobSla.newBuilder()
//                        .setUserProvidedType("")
//                        .setDurationTypeValue(durationType.ordinal())
//                        .setSlaType(io.mantisrx.master.core.proto.JobSla.StreamSLAType.Lossy)
//                        .setMinRuntimeSecs(0)
//                        .setRuntimeLimitSecs(0))
//                .setSchedulingInfo(io.mantisrx.master.core.proto.SchedulingInfo.parseFrom(objectMapper.writeValueAsBytes(schedulingInfo)))
//                .build();
//        JobOwner owner = JobOwner.newBuilder()
//                .setName("Test")
//                .setContactEmail("test@netflix.com")
//                .setDescription("")
//                .setRepo("http://www.example.com")
//                .build();
//        CreateJobClusterRequest req = CreateJobClusterRequest.newBuilder()
//                .setJobDefinition(jobDefinition)
//                .setOwner(owner)
//                .build();
//        mantisClient.createNamedJob(req);
//        System.out.println(jobName + " created");
//    }
//
//    private static void doUpdate(MantisClient mantisClient) throws Throwable {
//        JobDefinition jobDefinition = JobDefinition.newBuilder()
//                .setName(jobName)
//                .setVersion("0.0.1")
//                .setJobSla(io.mantisrx.master.core.proto.JobSla.newBuilder()
//                        .setUserProvidedType("")
//                        .setDurationTypeValue(durationType.ordinal())
//                        .setSlaType(io.mantisrx.master.core.proto.JobSla.StreamSLAType.Lossy)
//                        .setMinRuntimeSecs(0)
//                        .setRuntimeLimitSecs(0))
//                .setSchedulingInfo(io.mantisrx.master.core.proto.SchedulingInfo.parseFrom(objectMapper.writeValueAsBytes(schedulingInfo)))
//                .build();
//        JobOwner owner = JobOwner.newBuilder()
//                .setName("Test")
//                .setContactEmail("test@netflix.com")
//                .setDescription("")
//                .setRepo("http://www.example.com")
//                .build();
//        UpdateJobClusterRequest req = UpdateJobClusterRequest.newBuilder()
//                .setJobDefinition(jobDefinition)
//                .setOwner(owner)
//                .build();
//        mantisClient.updateNamedJob(req);
//        System.out.println(jobName + " updated");
//    }
//
//    private static String doSubmit(MantisClient mantisClient) throws Throwable {
//        String id = mantisClient.submitJob(jobName, null, null, new JobSla(0L, 0L, JobSla.StreamSLAType.Lossy, durationType, ""), schedulingInfo);
//        System.out.println("Job ID: " + id);
//        return id;
//    }
//
//    private static void doGetSinkData(MantisClient mantisClient, final String localJobId) {
//        final CountDownLatch startLatch = new CountDownLatch(1);
//        final CountDownLatch finishLatch = new CountDownLatch(1);
//        final SinkClient sinkClient = mantisClient.getSinkClientByJobId(localJobId,
//                new SseSinkConnectionFunction(true, new Action1<Throwable>() {
//                    @Override
//                    public void call(Throwable throwable) {
//                        System.err.println("Sink connection error: " + throwable.getMessage());
//                        try {Thread.sleep(500);} catch (InterruptedException ie) {
//                            System.err.println("Interrupted waiting for retrying connection");
//                        }
//                    }
//                }), null);
//        System.out.println("GETTING results observable for job " + localJobId);
//        Observable<MantisServerSentEvent> resultsObservable = Observable.merge(sinkClient
//                .getResults());
//        System.out.println("SUBSCRIBING to it");
//        final AtomicReference<Subscription> ref = new AtomicReference<>(null);
//        final Thread t = new Thread() {
//            @Override
//            public void run() {
//                try {
//                    startLatch.await();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                try {sleep(300000);} catch (InterruptedException ie) {}
//                System.out.println("Closing client conx");
//                try {
//                    ref.get().unsubscribe();
//                    finishLatch.countDown();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        t.setDaemon(true);
//        final Subscription s = resultsObservable
//                .doOnCompleted(new Action0() {
//                    @Override
//                    public void call() {
//                        finishLatch.countDown();
//                    }
//                })
//                .subscribe(new Action1<MantisServerSentEvent>() {
//                    @Override
//                    public void call(MantisServerSentEvent event) {
//                        if (startLatch.getCount() > 0) {
//                            startLatch.countDown();
//                        }
//                        System.out.println(localJobId.substring(0, 5) + ": Got SSE (event=" + event.getEventAsString() + "): ");
//                    }
//                });
//        ref.set(s);
//        t.start();
//        System.out.println("SUBSCRIBED to job sink changes");
//        try {
//            finishLatch.await();
//            System.err.println("Sink observable completed");
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//}
