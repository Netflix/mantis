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

import static org.junit.jupiter.api.Assertions.fail;

import io.mantisrx.client.MantisClient;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.client.SinkConnectionsStatus;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.functions.Action1;


public class MantisSSEJobTest {

    private static final Logger logger = LoggerFactory.getLogger(MantisSSEJobTest.class);
    static Properties zkProps = new Properties();

    static {
        zkProps.put("mantis.zookeeper.connectString", "100.67.80.172:2181,100.67.71.221:2181,100.67.89.26:2181,100.67.71.34:2181,100.67.80.18:2181");
        zkProps.put("mantis.zookeeper.leader.announcement.path", "/leader");
        zkProps.put("mantis.zookeeper.root", "/mantis/master");
    }

    //	@Test
    public void jobDiscoveryInfoStreamTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);
        MantisClient mClient = new MantisClient(zkProps);
        mClient.jobClusterDiscoveryInfoStream("SineFnTest")
                .map((schedInfo) -> {
                    latch.countDown();
                    return String.valueOf(schedInfo);
                })
                .doOnError(e -> logger.error("caught error", e))
                .subscribe((sc) -> logger.info("got {}", sc));

        latch.await(5, TimeUnit.MINUTES);
    }

    //	@Test
    public void connectToJobTest() {
        CountDownLatch latch = new CountDownLatch(5);
        CountDownLatch connectionStatusReceived = new CountDownLatch(1);
        MantisSSEJob job = new MantisSSEJob.Builder(zkProps)
                .name("GroupByIP")
                .sinkConnectionsStatusObserver(new Observer<SinkConnectionsStatus>() {

                    @Override
                    public void onCompleted() {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onError(Throwable e) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onNext(SinkConnectionsStatus t) {
                        connectionStatusReceived.countDown();

                    }

                })
                .onConnectionReset(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        System.err.println("Reconnecting due to error: " + throwable.getMessage());
                    }
                })
                //    .sinkParams(params)
                .buildJobConnector();

        job.connectAndGet()
                .flatMap((t) -> {
                    return t.map((mmsse) -> {
                        return mmsse.getEventAsString();
                    });
                })
                .take(5)
                .doOnNext((d) -> {
                    latch.countDown();
                })
                .toBlocking().subscribe((data) -> {
            System.out.println("Got data -> " + data);
        });
        ;
        try {
            latch.await(10, TimeUnit.SECONDS);
            connectionStatusReceived.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }


    }

    //@Test
    public void submitAndConnectTest() {
        CountDownLatch latch = new CountDownLatch(2);
        CountDownLatch connectionStatusReceived = new CountDownLatch(1);
        MantisSSEJob job = new MantisSSEJob.Builder(zkProps)
                .name("mantis-examples-sine-function")
                .sinkConnectionsStatusObserver(new Observer<SinkConnectionsStatus>() {

                    @Override
                    public void onCompleted() {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onError(Throwable e) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onNext(SinkConnectionsStatus t) {
                        connectionStatusReceived.countDown();

                    }

                })
                .onConnectionReset(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        System.err.println("Reconnecting due to error: " + throwable.getMessage());
                    }
                })

                .onCloseKillJob()
                //    .sinkParams(params)
                .buildJobSubmitter();

        job.submitAndGet()
                .flatMap((t) -> {
                    return t.map((mmsse) -> {
                        return mmsse.getEventAsString();
                    });
                })
                .take(2)
                .doOnNext((d) -> {
                    latch.countDown();
                })
                .toBlocking().subscribe((data) -> {
            System.out.println("Got data -> " + data);
        });
        ;
        try {
            latch.await(20, TimeUnit.SECONDS);
            connectionStatusReceived.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }


    }

    static class NoOpSinkConnectionsStatusObserver implements Observer<SinkConnectionsStatus> {

        @Override
        public void onCompleted() {
            logger.warn("Got Completed on SinkConnectionStatus ");

        }

        @Override
        public void onError(Throwable e) {
            logger.error("Got Error on SinkConnectionStatus ", e);

        }

        @Override
        public void onNext(SinkConnectionsStatus t) {
            logger.info("Got Sink Connection Status update " + t);

        }

    }


}
