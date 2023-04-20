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

package io.mantisrx.server.worker.client;

import com.mantisrx.common.utils.Services;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.reactivex.mantis.remote.observable.EndpointChange;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;
import rx.subjects.PublishSubject;


public class WorkerMetricsClient {

    private static final Logger logger = LoggerFactory.getLogger(WorkerMetricsClient.class);

    private final MasterClientWrapper clientWrapper;

    private final JobWorkerMetricsLocator jobWrokerMetricsLocator = new JobWorkerMetricsLocator() {
        @Override
        public Observable<EndpointChange> locateWorkerMetricsForJob(final String jobId) {
            return clientWrapper.getMasterClientApi()
                    .flatMap(new Func1<MantisMasterGateway, Observable<EndpointChange>>() {
                        @Override
                        public Observable<EndpointChange> call(MantisMasterGateway mantisMasterClientApi) {
                            logger.info("Getting worker metrics locations for " + jobId);
                            return clientWrapper.getAllWorkerMetricLocations(jobId);
                        }
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
    public WorkerMetricsClient(Properties properties) {
        this(Configurations.frmProperties(properties, CoreConfiguration.class));
    }

    public WorkerMetricsClient(CoreConfiguration configuration) {
        HighAvailabilityServices haServices =
            HighAvailabilityServicesUtil.createHAServices(configuration);
        Services.startAndWait(haServices);
        clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
    }

    public WorkerMetricsClient(MantisMasterGateway gateway) {
        clientWrapper = new MasterClientWrapper(gateway);
    }


    public JobWorkerMetricsLocator getWorkerMetricsLocator() {
        return jobWrokerMetricsLocator;
    }

    /* package */ MasterClientWrapper getClientWrapper() {
        return clientWrapper;
    }

    public <T> MetricsClient<T> getMetricsClientByJobId(final String jobId, final WorkerConnectionFunc<T> workerConnectionFunc,
                                                        Observer<WorkerConnectionsStatus> workerConnectionsStatusObserver) {
        return getMetricsClientByJobId(jobId, workerConnectionFunc, workerConnectionsStatusObserver, 5);
    }

    public <T> MetricsClient<T> getMetricsClientByJobId(final String jobId, final WorkerConnectionFunc<T> workerConnectionFunc,
                                                        Observer<WorkerConnectionsStatus> workerConnectionsStatusObserver, long dataRecvTimeoutSecs) {
        PublishSubject<MasterClientWrapper.JobNumWorkers> numWrkrsSubject = PublishSubject.create();
        clientWrapper.addNumWorkersObserver(numWrkrsSubject);
        return new MetricsClientImpl<T>(jobId, workerConnectionFunc, getWorkerMetricsLocator(),
                numWrkrsSubject
                        .filter(new Func1<MasterClientWrapper.JobNumWorkers, Boolean>() {
                            @Override
                            public Boolean call(MasterClientWrapper.JobNumWorkers jobNumWorkers) {
                                return jobId.equals(jobNumWorkers.getJobId());
                            }
                        })
                        .map(new Func1<MasterClientWrapper.JobNumWorkers, Integer>() {
                            @Override
                            public Integer call(MasterClientWrapper.JobNumWorkers jobNumWorkers) {
                                return jobNumWorkers.getNumWorkers();
                            }
                        }),
                workerConnectionsStatusObserver, dataRecvTimeoutSecs);
    }
}
