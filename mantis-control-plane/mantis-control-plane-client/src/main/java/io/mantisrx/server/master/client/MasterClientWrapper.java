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

import com.mantisrx.common.utils.Services;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.common.network.WorkerEndpoint;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.ToDeltaEndpointInjector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class MasterClientWrapper {

    public static final String InvalidNamedJob = "No_such_named_job";
    private static final Logger logger = LoggerFactory.getLogger(MasterClientWrapper.class);
    private final Counter masterConnectRetryCounter;
    private final MantisMasterGateway masterClientApi;
    private final PublishSubject<JobSinkNumWorkers> numSinkWorkersSubject = PublishSubject.create();
    private final PublishSubject<JobNumWorkers> numWorkersSubject = PublishSubject.create();

    // blocks until getting master info from zookeeper
    public MasterClientWrapper(MantisMasterGateway gateway) {
        this.masterClientApi = gateway;
        Metrics m = new Metrics.Builder()
                .name(MasterClientWrapper.class.getCanonicalName())
                .addCounter("MasterConnectRetryCount")
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        masterConnectRetryCounter = m.getCounter("MasterConnectRetryCount");
    }

    public static String getWrappedHost(String host, int workerNumber) {
        return host + "-" + workerNumber;
    }

    public static String getUnwrappedHost(String wrappedHost) {
        final int i = wrappedHost.lastIndexOf('-');
        if (i < 0)
            return wrappedHost;
        return wrappedHost.substring(0, i);
    }

    public static void main(String[] args) throws InterruptedException {
        Properties zkProps = new Properties();
        zkProps.put("mantis.zookeeper.connectString", "ec2-50-19-255-1.compute-1.amazonaws.com:2181,ec2-54-235-159-245.compute-1.amazonaws.com:2181,ec2-50-19-255-97.compute-1.amazonaws.com:2181,ec2-184-73-152-248.compute-1.amazonaws.com:2181,ec2-50-17-247-179.compute-1.amazonaws.com:2181");
        zkProps.put("mantis.zookeeper.leader.announcement.path", "/leader");
        zkProps.put("mantis.zookeeper.root", "/mantis/master");
        String jobId = "GroupByIPNJ-12";
        HighAvailabilityServices haServices =
            HighAvailabilityServicesUtil.createHAServices(Configurations.frmProperties(zkProps, CoreConfiguration.class));
        Services.startAndWait(haServices);
        MasterClientWrapper clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
        clientWrapper.getMasterClientApi()
                .flatMap(new Func1<MantisMasterGateway, Observable<EndpointChange>>() {
                    @Override
                    public Observable<EndpointChange> call(MantisMasterGateway mantisMasterClientApi) {
                        Integer sinkStage = null;
                        return mantisMasterClientApi.getSinkStageNum(jobId)
                                .take(1) // only need to figure out sink stage number once
                                .flatMap(new Func1<Integer, Observable<EndpointChange>>() {
                                    @Override
                                    public Observable<EndpointChange> call(Integer integer) {
                                        logger.info("Getting sink locations for " + jobId);
                                        return clientWrapper.getSinkLocations(jobId, integer, 0, 0);
                                    }
                                });
                    }
                }).toBlocking().subscribe((ep) -> {
            System.out.println("Endpoint Change -> " + ep);
        });
        Thread.sleep(50000);
    }

    public void addNumSinkWorkersObserver(Observer<JobSinkNumWorkers> numSinkWorkersObserver) {
        numSinkWorkersSubject.subscribe(numSinkWorkersObserver);
    }

    public void addNumWorkersObserver(Observer<JobNumWorkers> numWorkersObserver) {
        numWorkersSubject.subscribe(numWorkersObserver);
    }

    /**
     * Returns an Observable that emits only once, after the MasterClientApi has been initialized
     */
    public Observable<MantisMasterGateway> getMasterClientApi() {
        return Observable.just(masterClientApi);
    }

    private List<Endpoint> getAllNonJobMasterEndpoints(final String jobId, final Map<Integer, WorkerAssignments> workerAssignments) {
        List<Endpoint> endpoints = new ArrayList<>();
        int totalWorkers = 0;

        for (Map.Entry<Integer, WorkerAssignments> workerAssignment : workerAssignments.entrySet()) {
            final Integer stageNum = workerAssignment.getKey();
            // skip workers for stage 0
            if (stageNum == 0) {
                continue;
            }

            final WorkerAssignments assignments = workerAssignment.getValue();
            logger.info("job {} Creating endpoints conx from {} worker assignments for stage {}",
                    jobId, assignments.getHosts().size(), stageNum);
            if (logger.isDebugEnabled()) {
                logger.debug("stage {} hosts: {}", stageNum, assignments.getHosts());
            }
            totalWorkers += assignments.getNumWorkers();

            for (WorkerHost host : assignments.getHosts().values()) {
                final int workerIndex = host.getWorkerIndex();
                if (host.getState() == MantisJobState.Started) {
                    logger.info("job " + jobId + ": creating new endpoint for worker number=" + host.getWorkerNumber()
                            + ", index=" + host.getWorkerIndex() + ", host:port=" + host.getHost() + ":" +
                            host.getPort().get(0));
                    Endpoint ep = new WorkerEndpoint(getWrappedHost(host.getHost(), host.getWorkerNumber()), host.getPort().get(0),
                            stageNum, host.getMetricsPort(), host.getWorkerIndex(), host.getWorkerNumber(),
                            // completed callback
                            new Action0() {
                                @Override
                                public void call() {
                                    logger.info("job " + jobId + " WorkerIndex " + workerIndex + " completed");
                                }
                            },
                            // error callback
                            new Action1<Throwable>() {
                                @Override
                                public void call(Throwable t1) {
                                    logger.info("job " + jobId + " WorkerIndex " + workerIndex + " failed");
                                }
                            }
                    );
                    endpoints.add(ep);
                }
            }
        }
        numWorkersSubject.onNext(new JobNumWorkers(jobId, totalWorkers));
        return endpoints;
    }

    public Observable<EndpointChange> getAllWorkerMetricLocations(final String jobId) {
        final ConditionalRetry schedInfoRetry = new ConditionalRetry(masterConnectRetryCounter, "AllSchedInfoRetry", 10);
        Observable<List<Endpoint>> schedulingUpdates =
                                masterClientApi
                                        .schedulingChanges(jobId)
                                        .doOnError(new Action1<Throwable>() {
                                            @Override
                                            public void call(Throwable throwable) {
                                                logger.warn("Error on scheduling changes observable: " + throwable);
                                            }
                                        })
                                        .retryWhen(schedInfoRetry.getRetryLogic())
                                        .map(new Func1<JobSchedulingInfo, Map<Integer, WorkerAssignments>>() {
                                            @Override
                                            public Map<Integer, WorkerAssignments> call(JobSchedulingInfo jobSchedulingInfo) {
                                                logger.info("Got scheduling info for " + jobId);
                                                return jobSchedulingInfo.getWorkerAssignments();
                                            }
                                        })
                                        .filter(new Func1<Map<Integer, WorkerAssignments>, Boolean>() {
                                            @Override
                                            public Boolean call(Map<Integer, WorkerAssignments> workerAssignments) {
                                                return workerAssignments != null;
                                            }
                                        })
                                        .map(new Func1<Map<Integer, WorkerAssignments>, List<Endpoint>>() {
                                            @Override
                                            public List<Endpoint> call(Map<Integer, WorkerAssignments> workerAssignments) {
                                                return getAllNonJobMasterEndpoints(jobId, workerAssignments);
                                            }
                                        })
                                        .doOnError(new Action1<Throwable>() {
                                            @Override
                                            public void call(Throwable throwable) {
                                                logger.error(throwable.getMessage(), throwable);
                                            }
                                        });

        return (new ToDeltaEndpointInjector(schedulingUpdates)).deltas();
    }

    public Observable<EndpointChange> getSinkLocations(final String jobId, final int sinkStage,
                                                       final int forPartition, final int totalPartitions) {
        final ConditionalRetry schedInfoRetry = new ConditionalRetry(masterConnectRetryCounter, "SchedInfoRetry", 10);
        Observable<List<Endpoint>> schedulingUpdates =
                masterClientApi
                                    .schedulingChanges(jobId)
                                    .doOnError((Throwable throwable) -> {
                                        logger.warn(throwable.getMessage());
                                    })
                                    .retryWhen(schedInfoRetry.getRetryLogic())
                                    .map((JobSchedulingInfo jobSchedulingInfo) -> {
                                        logger.info("Got scheduling info for {}", jobId);
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("Worker Assignments {}", jobSchedulingInfo.getWorkerAssignments().get(sinkStage));
                                        }

                                        return jobSchedulingInfo.getWorkerAssignments().get(sinkStage);
                                    })
                                    // Worker assignments can be empty if the job has completed so do not filter these events out
                                    .map((WorkerAssignments workerAssignments) -> {
                                        List<Endpoint> endpoints = new ArrayList<>();
                                        if (workerAssignments != null) {
                                            logger.info("job " + jobId + " Creating endpoints conx from " + workerAssignments.getHosts().size() + " worker assignments");
                                            for (WorkerHost host : workerAssignments.getHosts().values()) {
                                                final int workerIndex = host.getWorkerIndex();
                                                final int totalFromPartitions = workerAssignments.getNumWorkers();
                                                final int runningWorkers = (int) workerAssignments
                                                    .getHosts()
                                                    .values()
                                                    .stream()
                                                    .filter(e -> MantisJobState.isOnStartedState(e.getState()))
                                                    .count();
                                                numSinkWorkersSubject.onNext(new JobSinkNumWorkers(jobId, totalFromPartitions, runningWorkers));
                                                if (usePartition(workerIndex, totalFromPartitions, forPartition, totalPartitions)) {
                                                    //logger.info("Using partition " + workerIndex);
                                                    if (host.getState() == MantisJobState.Started) {
                                                        Endpoint ep = new Endpoint(getWrappedHost(host.getHost(), host.getWorkerNumber()), host.getPort().get(0),
                                                                // completed callback
                                                                () -> logger.info("job " + jobId + " WorkerIndex " + workerIndex + " completed"),
                                                                // error callback
                                                                t1 -> logger.info("job " + jobId + " WorkerIndex " + workerIndex + " failed")
                                                        );
                                                        endpoints.add(ep);
                                                    }
                                                }
                                            }
                                        } else {
                                            logger.info("job " + jobId + " Has no active workers!");
                                        }
                                        return endpoints;
                                    })
                                    .doOnError((Throwable throwable) -> {
                                        logger.error(throwable.getMessage(), throwable);
                                    });

        return (new ToDeltaEndpointInjector(schedulingUpdates)).deltas();
    }

    private boolean usePartition(int fromPartition, int fromTotalPartitions, int toPartition, int toTotalPartitions) {
        if (toPartition < 0 || toTotalPartitions == 0)
            return true; // not partitioning
        long n = Math.round((double) fromTotalPartitions / (double) toTotalPartitions);
        long beg = toPartition * n;
        long end = toPartition == toTotalPartitions - 1 ? fromTotalPartitions : (toPartition + 1) * n;
        return beg < fromTotalPartitions && fromPartition >= beg && fromPartition < end;
    }

    public Observable<Boolean> namedJobExists(final String jobName) {
        final ConditionalRetry namedJobRetry = new ConditionalRetry(masterConnectRetryCounter, "NamedJobExists", Integer.MAX_VALUE);
        logger.info("verifying if job name exists: " + jobName);
        return masterClientApi.namedJobExists(jobName).retryWhen(namedJobRetry.getRetryLogic());
    }

    public Observable<String> getNamedJobsIds(final String jobName) {
        final ConditionalRetry namedJobsIdsRetry = new ConditionalRetry(masterConnectRetryCounter, "NamedJobsIds", Integer.MAX_VALUE);
        logger.info("verifying if job name exists: " + jobName);
        return masterClientApi.namedJobExists(jobName)
                .onErrorResumeNext((Throwable throwable) -> {
                    logger.error(throwable.getMessage());
                    return Observable.empty();
                })
                .take(1)
                .map((exists) -> {
                    if (!exists) {
                        final Exception exception = new Exception("No such Job Cluster " + jobName);
                        namedJobsIdsRetry.setErrorRef(exception);
                        return Observable.just(new NamedJobInfo(jobName, InvalidNamedJob));
                    }
                    logger.info("Getting Job cluster info for " + jobName);
                    return masterClientApi.namedJobInfo(jobName);
                })
                .doOnError((Throwable throwable) -> {
                    logger.error(throwable.getMessage(), throwable);
                })
                .retryWhen(namedJobsIdsRetry.getRetryLogic())
                .flatMap((Observable<NamedJobInfo> namedJobInfo) -> {
                    return namedJobInfo.map((NamedJobInfo nji) -> {
                        return nji.getJobId();
                    });
                });
    }

    public static class JobSinkNumWorkers {

        protected final int numSinkWorkers;
        protected final int numSinkRunningWorkers;
        private final String jobId;

        public JobSinkNumWorkers(String jobId, int numSinkWorkers, int numSinkRunningWorkers) {
            this.jobId = jobId;
            this.numSinkWorkers = numSinkWorkers;
            this.numSinkRunningWorkers = numSinkRunningWorkers;
        }

        public String getJobId() {
            return jobId;
        }

        public int getNumSinkWorkers() {
            return numSinkWorkers;
        }

        public int getNumSinkRunningWorkers() {
            return numSinkRunningWorkers;
        }
    }

    public static class JobNumWorkers {

        protected final int numWorkers;
        private final String jobId;

        public JobNumWorkers(String jobId, int numWorkers) {
            this.jobId = jobId;
            this.numWorkers = numWorkers;
        }

        public String getJobId() {
            return jobId;
        }

        public int getNumWorkers() {
            return numWorkers;
        }
    }


}
