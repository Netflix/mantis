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

import com.mantisrx.common.utils.Services;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.reactivex.mantis.remote.observable.EndpointChange;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.subjects.PublishSubject;


public class MantisClient {

    private static final Logger logger = LoggerFactory.getLogger(MantisClient.class);

    private static final String ENABLE_PINGS_KEY = "mantis.sse.disablePingFiltering";
    private final boolean disablePingFiltering;

    private final MasterClientWrapper clientWrapper;
    private final JobSinkLocator jobSinkLocator = new JobSinkLocator() {
        @Override
        public Observable<EndpointChange> locateSinkForJob(final String jobId) {
            return locatePartitionedSinkForJob(jobId, -1, 0);
        }

        @Override
        public Observable<EndpointChange> locatePartitionedSinkForJob(final String jobId, final int forPartition, final int totalPartitions) {
            return clientWrapper.getMasterClientApi()
                    .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                        return mantisMasterClientApi.getSinkStageNum(jobId)
                                .take(1) // only need to figure out sink stage number once
                                .flatMap((Integer integer) -> {
                                    logger.info("Getting sink locations for " + jobId);
                                    return clientWrapper.getSinkLocations(jobId, integer, forPartition, totalPartitions);
                                });
                    });
        }
    };

    /**
     * The following properties are required:
     * <UL>
     * <LI>
     * #default 1000<br>
     * mantis.zookeeper.connectionTimeMs=1000
     * </LI>
     * <LI>
     * # default 500<br>
     * mantis.zookeeper.connection.retrySleepMs=500
     * </LI>
     * <LI>
     * # default 5<br>
     * mantis.zookeeper.connection.retryCount=5
     * </LI>
     * <LI>
     * # default NONE<br>
     * mantis.zookeeper.connectString=
     * </LI>
     * <LI>
     * #default NONE<br>
     * mantis.zookeeper.root=
     * </LI>
     * <LI>
     * #default /leader <br>
     * mantis.zookeeper.leader.announcement.path=
     * </LI>
     * </UL>
     *
     * @param properties
     */
    public MantisClient(Properties properties) {
        HighAvailabilityServices haServices =
            HighAvailabilityServicesUtil.createHAServices(
                Configurations.frmProperties(properties, CoreConfiguration.class));

        Services.startAndWait(haServices);
        clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
        this.disablePingFiltering = Boolean.parseBoolean(properties.getProperty(ENABLE_PINGS_KEY));
    }

    public MantisClient(MasterClientWrapper clientWrapper, boolean disablePingFiltering) {
        this.disablePingFiltering = disablePingFiltering;
        this.clientWrapper = clientWrapper;
    }

    public MantisClient(HighAvailabilityServices haServices) {
        haServices.awaitRunning();
        clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
        this.disablePingFiltering = false;
    }

    public MantisClient(MasterClientWrapper clientWrapper) {
        this(clientWrapper, false);
    }

    public JobSinkLocator getSinkLocator() {
        return jobSinkLocator;
    }

    /* package */ MasterClientWrapper getClientWrapper() {
        return clientWrapper;
    }

    private MantisMasterGateway blockAndGetMasterApi() {
        return clientWrapper
                .getMasterClientApi()
                .toBlocking()
                .first();
    }

    public Observable<Boolean> namedJobExists(final String jobName) {
        return clientWrapper.namedJobExists(jobName);
    }

    public <T> Observable<SinkClient<T>> getSinkClientByJobName(final String jobName, final SinkConnectionFunc<T> sinkConnectionFunc,
                                                                final Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver) {
        return getSinkClientByJobName(jobName, sinkConnectionFunc, sinkConnectionsStatusObserver, 5);
    }

    public <T> Observable<SinkClient<T>> getSinkClientByJobName(final String jobName, final SinkConnectionFunc<T> sinkConnectionFunc,
                                                                final Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver, final long dataRecvTimeoutSecs) {
        final AtomicReference<String> lastJobIdRef = new AtomicReference<>();
        return clientWrapper.getNamedJobsIds(jobName)
                .doOnUnsubscribe(() -> lastJobIdRef.set(null))
                //                .lift(new Observable.Operator<String, String>() {
                //                    @Override
                //                    public Subscriber<? super String> call(Subscriber<? super String> subscriber) {
                //                        subscriber.add(Subscriptions.create(new Action0() {
                //                            @Override
                //                            public void call() {
                //                                lastJobIdRef.set(null);
                //                            }
                //                        }));
                //                        return subscriber;
                //                    }
                //                })
                .filter((String newJobId) -> {
                    logger.info("Got job cluster's new jobId=" + newJobId);
                    return newJobIdIsGreater(lastJobIdRef.get(), newJobId);
                })
                .map((final String jobId) -> {
                    if (MasterClientWrapper.InvalidNamedJob.equals(jobId))
                        return getErrorSinkClient(jobId);
                    lastJobIdRef.set(jobId);
                    logger.info("Connecting to job " + jobName + " with new jobId=" + jobId);
                    return getSinkClientByJobId(jobId, sinkConnectionFunc, sinkConnectionsStatusObserver, dataRecvTimeoutSecs);
                });
    }

    private Boolean newJobIdIsGreater(String oldJobId, String newJobId) {
        if (oldJobId == null)
            return true;
        final int oldIdx = oldJobId.lastIndexOf('-');
        if (oldIdx < 0)
            return true;
        final int newIdx = newJobId.lastIndexOf('-');
        if (newIdx < 0)
            return true;
        try {
            int old = Integer.parseInt(oldJobId.substring(oldIdx + 1));
            int newJ = Integer.parseInt(newJobId.substring(newIdx + 1));
            return newJ > old;
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            return true; // can't parse numbers, assume it is not the same as old job id
        }
    }

    private <T> SinkClient<T> getErrorSinkClient(final String mesg) {
        return new SinkClient<T>() {
            @Override
            public boolean hasError() {
                return true;
            }

            @Override
            public String getError() {
                return mesg;
            }

            @Override
            public Observable<Observable<T>> getResults() {
                return null;
            }

            @Override
            public Observable<Observable<T>> getPartitionedResults(int forPartition, int totalPartitions) {
                return null;
            }
        };
    }

    public <T> SinkClient<T> getSinkClientByJobId(final String jobId, final SinkConnectionFunc<T> sinkConnectionFunc,
                                                  Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver) {
        return getSinkClientByJobId(jobId, sinkConnectionFunc, sinkConnectionsStatusObserver, 5);
    }

    public <T> SinkClient<T> getSinkClientByJobId(final String jobId, final SinkConnectionFunc<T> sinkConnectionFunc,
                                                  Observer<SinkConnectionsStatus> sinkConnectionsStatusObserver, long dataRecvTimeoutSecs) {
        PublishSubject<MasterClientWrapper.JobSinkNumWorkers> numSinkWrkrsSubject = PublishSubject.create();
        clientWrapper.addNumSinkWorkersObserver(numSinkWrkrsSubject);
        return new SinkClientImpl<T>(jobId, sinkConnectionFunc, getSinkLocator(),
                numSinkWrkrsSubject
                        .filter((jobSinkNumWorkers) -> jobId.equals(jobSinkNumWorkers.getJobId())),
                sinkConnectionsStatusObserver, dataRecvTimeoutSecs, this.disablePingFiltering);
    }

    public String submitJob(final String name, final String version, final List<Parameter> parameters,
                            final JobSla jobSla, final SchedulingInfo schedulingInfo) throws Exception {
        return clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                    return mantisMasterClientApi.submitJob(name, version, parameters, jobSla, schedulingInfo)
                            .onErrorResumeNext((t) -> {
                                logger.warn(t.getMessage());
                                return Observable.empty();
                            });
                })
                .take(1)
                .toBlocking()
                .first()
                .getJobId();
    }

    public String submitJob(final String name, final String version, final List<Parameter> parameters,
                            final JobSla jobSla, final long subscriptionTimeoutSecs,
                            final SchedulingInfo schedulingInfo) throws Exception {
        return clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                            return mantisMasterClientApi.submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo)
                                    .onErrorResumeNext((Throwable t) -> {
                                        logger.warn(t.getMessage());
                                        return Observable.empty();
                                    });
                        }
                )
                .take(1)
                .toBlocking()
                .first()
                .getJobId();
    }

    public String submitJob(final String name, final String version, final List<Parameter> parameters,
                            final JobSla jobSla, final long subscriptionTimeoutSecs,
                            final SchedulingInfo schedulingInfo, final boolean readyForJobMaster) throws Exception {
        return clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                    return mantisMasterClientApi.submitJob(name, version, parameters, jobSla, subscriptionTimeoutSecs, schedulingInfo, readyForJobMaster)
                            .onErrorResumeNext((Throwable t) -> {
                                logger.warn(t.getMessage());
                                return Observable.empty();
                            });
                })
                .take(1)
                .toBlocking()
                .first()
                .getJobId();
    }

    public void killJob(final String jobId) {
        clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                    return mantisMasterClientApi.killJob(jobId)
                            .onErrorResumeNext((Throwable t) -> {
                                logger.warn(t.getMessage());
                                return Observable.empty();
                            });
                })
                .take(1)
                .toBlocking()
                .first();
    }

//    public void createNamedJob(final CreateJobClusterRequest request) {
//        clientWrapper.getMasterClientApi()
//                .flatMap((MantisMasterClientApi mantisMasterClientApi) -> {
//                    return mantisMasterClientApi.createNamedJob(request)
//                            .onErrorResumeNext((t) -> {
//                                logger.warn(t.getMessage());
//                                return Observable.empty();
//                            });
//                })
//                .take(1)
//                .toBlocking()
//                .first();
//    }
//
//    public void updateNamedJob(final UpdateJobClusterRequest request) {
//        clientWrapper.getMasterClientApi()
//                .flatMap((MantisMasterClientApi mantisMasterClientApi) -> {
//                    return mantisMasterClientApi.updateNamedJob(request)
//                            .onErrorResumeNext((t) -> {
//                                logger.warn(t.getMessage());
//                                return Observable.empty();
//                            });
//                })
//                .take(1)
//                .toBlocking()
//                .first();
//    }

    /**
     * Get json array data for a list of jobs for a given job cluster.
     *
     * @param name  Name of the job
     * @param state State of jobs to match; null matches any state
     *
     * @return Json array data of the list of jobs, if any.
     */
    public Observable<String> getJobsOfNamedJob(final String name, final MantisJobState.MetaState state) {
        return clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                    return mantisMasterClientApi.getJobsOfNamedJob(name, state);
                })
                .first();
    }

    /**
     * A stream of changes associated with the given JobId
     */
    public Observable<String> getJobStatusObservable(final String jobId) {
        return clientWrapper.getMasterClientApi()
                .flatMap((MantisMasterGateway mantisMasterClientApi) -> {
                    return mantisMasterClientApi.getJobStatusObservable(jobId);
                });

    }

    /**
     * A stream of scheduling changes for the given Jobid
     */
    public Observable<JobSchedulingInfo> getSchedulingChanges(final String jobId) {
        return clientWrapper.getMasterClientApi()
                .flatMap((masterClientApi) -> masterClientApi.schedulingChanges(jobId));
    }

    /**
     * A stream of discovery updates for the latest job ID of given Job cluster
     *
     * @param jobCluster Job cluster name
     *
     * @return Observable<JobSchedulingInfo> stream of JobSchedulingInfo for latest jobID of job cluster
     */
    public Observable<JobSchedulingInfo> jobClusterDiscoveryInfoStream(final String jobCluster) {
        final AtomicReference<String> lastJobIdRef = new AtomicReference<>();
        return clientWrapper.getNamedJobsIds(jobCluster)
                .doOnUnsubscribe(() -> lastJobIdRef.set(null))
                .filter((String newJobId) -> {
                    logger.info("Got job cluster {}'s new jobId : {}", jobCluster, newJobId);
                    return newJobIdIsGreater(lastJobIdRef.get(), newJobId);
                })
                .switchMap((final String jobId) -> {
                    if (MasterClientWrapper.InvalidNamedJob.equals(jobId)) {
                        return Observable.error(new Exception("No such job cluster " + jobCluster));
                    }
                    lastJobIdRef.set(jobId);
                    logger.info("[{}] switched to streaming discovery info for {}", jobCluster, jobId);
                    return getSchedulingChanges(jobId);
                });
    }

}
