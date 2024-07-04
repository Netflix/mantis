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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.reactivex.mantis.remote.observable.EndpointChange;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;


public class MasterClientWrapperTest {

    private static final int sinkStageNumber = 3;
    static Properties zkProps = new Properties();

    static {
        zkProps.put("mantis.zookeeper.connectString", "100.67.80.172:2181,100.67.71.221:2181,100.67.89.26:2181,100.67.71.34:2181,100.67.80.18:2181");
        zkProps.put("mantis.zookeeper.leader.announcement.path", "/leader");
        zkProps.put("mantis.zookeeper.root", "/mantis/master");
        zkProps.put("mantis.localmode", "false");
        zkProps.put("mantis.leader.monitor.factory","io.mantisrx.server.core.master.ZookeeperLeaderMonitorFactory");
        zkProps.put("mantis.leader.elector.factory", "io.mantisrx.master.zk.ZookeeperLeadershipFactory");
    }

    MasterClientWrapper clientWrapper = null;

    //@Before
    public void init() {
      HighAvailabilityServices haServices = HighAvailabilityServicesUtil.createHAServices(
          Configurations.frmProperties(zkProps, CoreConfiguration.class));
      clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
    }

    //	@Test
    public void getNamedJobIdsTest() {

        String jobname = "APIRequestSource";
        CountDownLatch cdLatch = new CountDownLatch(1);

        clientWrapper
                .getNamedJobsIds(jobname)
                .subscribe((jId) -> {
                    cdLatch.countDown();
                    System.out.println("job id " + jId);
                    assertTrue(jId.startsWith(jobname));
                });

        try {
            cdLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }
    }

    //	@Test
    public void getSinkLocationsTest() {

        String jobname = "APIRequestSource";
        CountDownLatch cdLatch = new CountDownLatch(1);


        clientWrapper
                .getNamedJobsIds(jobname)
                .flatMap((jName) -> {
                    return clientWrapper.getSinkLocations(jName, 1, 0, 0);
                })
                .subscribe((ep) -> {
                    System.out.println("Got EP " + ep.getEndpoint() + " type " + ep.getType());
                    cdLatch.countDown();
                });

        try {
            cdLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }
    }


    //@Test
    public void getSchedulingInfoTest() {
        String jobname = "GroupByIP";

        CountDownLatch cdLatch = new CountDownLatch(3);

        Observable<String> jobidO = clientWrapper.getNamedJobsIds(jobname).take(1).cache().subscribeOn(Schedulers.io());

        Observable<MantisMasterGateway> mmciO = clientWrapper.getMasterClientApi().take(1).cache().subscribeOn(Schedulers.io());

        Observable<EndpointChange> epO = jobidO.map((jId) -> clientWrapper.getSinkLocations(jId, sinkStageNumber, 0, 0))
                .flatMap(e -> e)
                .take(3)
                .doOnNext((ep) -> System.out.println("Ep change: " + ep))
                .doOnNext((ep) -> cdLatch.countDown());

        Observable<Boolean> deleteWorkerO = jobidO.zipWith(mmciO, (String jId, MantisMasterGateway mmci) -> {
            System.out.println("Job id is " + jId);
            return mmci.schedulingChanges(jId)
                    .map(jsi -> {
                        Map<Integer, WorkerAssignments> workerAssignments = jsi.getWorkerAssignments();
                        System.out.println("WorkerAssignments -> " + workerAssignments);
                        WorkerAssignments workerAssignmentsForSink = workerAssignments.get(sinkStageNumber);
                        System.out.println("WorkerAssignmentsForSink -> " + workerAssignmentsForSink);
                        Map<Integer, WorkerHost> hostsForSink = workerAssignmentsForSink.getHosts();
                        System.out.println("Host map -> " + hostsForSink);
                        assertTrue(!hostsForSink.isEmpty());
                        Iterator<Entry<Integer, WorkerHost>> it = hostsForSink.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Integer, WorkerHost> e = it.next();
                            return e.getValue().getWorkerNumber();
                        }
                        return -1;
                    })
                    .take(1)
                    .map((Integer workerNo) -> {
                        System.out.println("Worker no is -> " + workerNo);
                        return mmci.resubmitJobWorker(jId, "tester", workerNo, "testing");
                    }).flatMap(b -> b);
        })
                .flatMap(b -> b)
                .doOnNext((result) -> {
                    assertTrue(result);
                    cdLatch.countDown();
                });

        epO.subscribeOn(Schedulers.io()).subscribe((ep) -> System.out.println(ep), (t) -> t.printStackTrace(), () -> System.out.println("ep change completed"));

        deleteWorkerO.toBlocking().subscribe((n) -> System.out.println(n), (t) -> t.printStackTrace(),
                () -> System.out.println("worker deletion completed"));

        try {
            cdLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }
    }

    //	@Test
    public void testJobStatusEndpoint() {
      HighAvailabilityServices haServices = HighAvailabilityServicesUtil.createHAServices(
          Configurations.frmProperties(zkProps, CoreConfiguration.class));
      MasterClientWrapper clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());
        String jobId = "PriamRequestSource-45";

        clientWrapper.getMasterClientApi()
                .flatMap(new Func1<MantisMasterGateway, Observable<String>>() {
                    @Override
                    public Observable<String> call(MantisMasterGateway mantisMasterClientApi) {
                        Integer sinkStage = null;
                        return mantisMasterClientApi.getJobStatusObservable(jobId)
                                .map((status) -> {
                                    return status;
                                })
                                ;
                    }
                }).take(2).toBlocking().subscribe((ep) -> {
            System.out.println("Endpoint Change -> " + ep);
        });

    }

    @Test
    public void testNamedJobExists() {

      HighAvailabilityServices haServices = HighAvailabilityServicesUtil.createHAServices(
          Configurations.frmProperties(zkProps, CoreConfiguration.class));
      MasterClientWrapper clientWrapper = new MasterClientWrapper(haServices.getMasterClientApi());

        CountDownLatch cdLatch = new CountDownLatch(1);
        clientWrapper.namedJobExists("APIRequestSource")

                .subscribe((exists) -> {
                    assertTrue(exists);
                    cdLatch.countDown();
                });
        try {
            cdLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }
    }

}
