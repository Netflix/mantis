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

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.master.client.ConditionalRetry;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.NoSuchJobException;
import io.reactivx.mantis.operators.DropOperator;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action1;
import rx.schedulers.Schedulers;


public class MantisSSEJob implements Closeable {

    private static final String ConnectTimeoutSecsPropertyName = "MantisClientConnectTimeoutSecs";
    private static final Logger logger = LoggerFactory.getLogger(MantisSSEJob.class);

    private final Builder builder;
    private final Mode mode;
    private Observable<Observable<MantisServerSentEvent>> resultsObservable;
    private String jobId = null;
    private int forPartition = -1;
    private int totalPartitions = 0;
    private MantisSSEJob(Builder builder, Mode mode) {
        this.builder = builder;
        this.mode = mode;
        if (builder.connectTimeoutSecs > 0)
            System.setProperty(ConnectTimeoutSecsPropertyName, String.valueOf(builder.connectTimeoutSecs));
    }

    @Override
    public synchronized void close() {
        if (mode == Mode.Submit && builder.ephemeral) {
            if (jobId != null) {
                builder.mantisClient.killJob(jobId);
                logger.info("Sent kill to master for job " + jobId);
            } else
                logger.warn("Unexpected to not have JobId to kill ephemeral job");
        }
    }

    public String getJobId() {
        return jobId;
    }

    private Observable<Observable<MantisServerSentEvent>> sinksToObservable(final Observable<SinkClient<MantisServerSentEvent>> sinkClients) {
        final ConditionalRetry retryObject = new ConditionalRetry(null, "SinkClient_" + builder.name);
        return sinkClients
                .switchMap((SinkClient<MantisServerSentEvent> serverSentEventSinkClient) -> {
                    if (serverSentEventSinkClient.hasError())
                        return Observable.just(Observable.just(new MantisServerSentEvent(serverSentEventSinkClient.getError())));
                    return serverSentEventSinkClient.getPartitionedResults(forPartition, totalPartitions);
                })
                .doOnError((Throwable throwable) -> {
                    logger.warn("Error getting sink Observable: " + throwable.getMessage());
                    if (!(throwable instanceof NoSuchJobException))
                        retryObject.setErrorRef(throwable); // don't retry if not NoSuchJobException
                })
                .retryWhen(retryObject.getRetryLogic())
                ;
    }

    @Deprecated
    public synchronized Observable<MantisServerSentEvent> connectAndGetObservable() throws IllegalStateException {
        return connectAndGet().flatMap(o -> o);
    }

    public synchronized Observable<Observable<MantisServerSentEvent>> connectAndGet() throws IllegalStateException {
        if (mode != Mode.Connect)
            throw new IllegalStateException("Can't call connect to sink");
        if (resultsObservable == null) {
            logger.info("Getting sink for job name " + builder.name);
            final Boolean exists = builder.mantisClient.namedJobExists(builder.name)
                    .take(1)
                    .toBlocking()
                    .first();
            resultsObservable = exists ?
                    sinksToObservable(builder.mantisClient.getSinkClientByJobName(
                            builder.name,
                            new SseSinkConnectionFunction(true, builder.onConnectionReset, builder.sinkParameters),
                            builder.sinkConnectionsStatusObserver, builder.dataRecvTimeoutSecs)
                    )
                            .share()
                    //.lift(new DropOperator<Observable<MantisServerSentEvent>>("client_connect_sse_share"))
                    :
                    Observable.just(Observable.just(new MantisServerSentEvent("No such job name " + builder.name)));
        }
        return resultsObservable;
    }

    @Deprecated
    public synchronized Observable<MantisServerSentEvent> submitAndGetObservable() throws IllegalStateException {
        return submitAndGet().flatMap(o -> o);
    }

    public synchronized Observable<Observable<MantisServerSentEvent>> submitAndGet() throws IllegalStateException {
        if (mode != Mode.Submit)
            throw new IllegalStateException("Can't submit job");
        if (resultsObservable != null)
            return resultsObservable;
        return Observable
                .create(new Observable.OnSubscribe<Observable<MantisServerSentEvent>>() {
                    @Override
                    public void call(Subscriber<? super Observable<MantisServerSentEvent>> subscriber) {
                        try {
                            JobSla jobSla = builder.jobSla == null ?
                                    new JobSla(0L, 0L, JobSla.StreamSLAType.Lossy,
                                            builder.ephemeral ? MantisJobDurationType.Transient : MantisJobDurationType.Perpetual,
                                            "") :
                                    new JobSla(builder.jobSla.getRuntimeLimitSecs(), builder.jobSla.getMinRuntimeSecs(),
                                            builder.jobSla.getSlaType(),
                                            builder.ephemeral ? MantisJobDurationType.Transient : MantisJobDurationType.Perpetual,
                                            builder.jobSla.getUserProvidedType());
                            jobId = builder.mantisClient.submitJob(builder.name, builder.jarVersion, builder.parameters,
                                    jobSla, builder.schedulingInfo);
                            logger.info("Submitted job name " + builder.name + " and got jobId: " + jobId);
                            resultsObservable = builder.mantisClient
                                    .getSinkClientByJobId(jobId, new SseSinkConnectionFunction(true, builder.onConnectionReset),
                                            builder.sinkConnectionsStatusObserver, builder.dataRecvTimeoutSecs)
                                    .getResults();
                            resultsObservable.subscribe(subscriber);
                        } catch (Exception e) {
                            subscriber.onError(e);
                        }
                    }
                })
                .doOnError((Throwable throwable) -> {
                    logger.warn(throwable.getMessage());
                })
                .lift(new DropOperator<>("client_submit_sse_share"))
                .share()
                .observeOn(Schedulers.io())
                ;
    }

    enum Mode {Submit, Connect}

    public static class Builder {

        private final MantisClient mantisClient;
        private final List<Parameter> parameters = new ArrayList<>();
        private String name;
        private String jarVersion;
        private SinkParameters sinkParameters = new SinkParameters.Builder().build();
        private Action1<Throwable> onConnectionReset;
        private boolean ephemeral = false;
        private SchedulingInfo schedulingInfo;
        private JobSla jobSla;
        private long connectTimeoutSecs = 0L;
        private Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver = null;
        private long dataRecvTimeoutSecs = 5;

        public Builder() {
            Properties properties = new Properties();
            properties.setProperty("mantis.zookeeper.connectionTimeMs", "1000");
            properties.setProperty("mantis.zookeeper.connection.retrySleepMs", "500");
            properties.setProperty("mantis.zookeeper.connection.retryCount", "500");
            properties.setProperty("mantis.zookeeper.connectString", System.getenv("mantis.zookeeper.connectString"));
            properties.setProperty("mantis.zookeeper.root", System.getenv("mantis.zookeeper.root"));
            properties.setProperty("mantis.zookeeper.leader.announcement.path",
                System.getenv("mantis.zookeeper.leader.announcement.path"));
            mantisClient = new MantisClient(properties);
        }

        public Builder(HighAvailabilityServices haServices) {
            this(new MantisClient(haServices));
        }

        public Builder(Properties properties) {
            this(new MantisClient(properties));
        }

        public Builder(MantisClient mantisClient) {
            this.mantisClient = mantisClient;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder jarVersion(String jarVersion) {
            this.jarVersion = jarVersion;
            return this;
        }

        public Builder parameters(Parameter... params) {
            this.parameters.addAll(Arrays.asList(params));
            return this;
        }

        public Builder sinkParams(SinkParameters queryParams) {
            this.sinkParameters = queryParams;
            return this;
        }

        public Builder onCloseKillJob() {
            this.ephemeral = true;
            return this;
        }

        public Builder schedulingInfo(SchedulingInfo schedulingInfo) {
            this.schedulingInfo = schedulingInfo;
            return this;
        }

        public Builder jobSla(JobSla jobSla) {
            this.jobSla = jobSla;
            if (jobSla != null)
                this.ephemeral = jobSla.getDurationType() == MantisJobDurationType.Transient;
            return this;
        }

        public Builder connectTimeoutSecs(long connectTimeoutSecs) {
            this.connectTimeoutSecs = connectTimeoutSecs;
            return this;
        }

        public Builder onConnectionReset(Action1<Throwable> onConnectionReset) {
            this.onConnectionReset = onConnectionReset;
            return this;
        }

        public Builder sinkConnectionsStatusObserver(Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver) {
            this.sinkConnectionsStatusObserver = sinkConnectionsStatusObserver;
            return this;
        }

        public Builder sinkDataRecvTimeoutSecs(long t) {
            dataRecvTimeoutSecs = t;
            return this;
        }

        public MantisSSEJob buildJobSubmitter() {
            return new MantisSSEJob(this, Mode.Submit);
        }

        public MantisSSEJob buildJobConnector(int forPartition, int totalPartitions) {
            if (forPartition >= totalPartitions)
                throw new IllegalArgumentException("forPartition " + forPartition + " must be less than totalPartitions " + totalPartitions);
            MantisSSEJob job = new MantisSSEJob(this, Mode.Connect);
            job.forPartition = forPartition;
            job.totalPartitions = totalPartitions;
            return job;
        }

        public MantisSSEJob buildJobConnector() {
            return new MantisSSEJob(this, Mode.Connect);
        }

    }

}
