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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.server.worker.TestSseServerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;


public class SseWorkerConnectionFunctionTest {

    private static final Logger logger = LoggerFactory.getLogger(SseWorkerConnectionFunctionTest.class);

    private static final String TEST_EVENT_DATA = "test event string";

    @Test
    public void testSseConnection() throws InterruptedException {
        final int serverPort = TestSseServerFactory.getServerPort();
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final boolean reconnectOnConnReset = true;

        SseWorkerConnectionFunction connectionFunction = new SseWorkerConnectionFunction(reconnectOnConnReset, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                logger.warn("connection was reset, should be retried", throwable);
                errorLatch.countDown();
            }
        });
        final WorkerConnection<MantisServerSentEvent> conn = connectionFunction.call("localhost", serverPort);

        final Observable<MantisServerSentEvent> events = conn.call();

        events
                .doOnNext(new Action1<MantisServerSentEvent>() {
                    @Override
                    public void call(MantisServerSentEvent e) {
                        logger.info("got event {}", e.getEventAsString());
                        assertEquals(TEST_EVENT_DATA, e.getEventAsString());
                        latch.countDown();
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        logger.error("caught error ", throwable);
                        fail("unexpected error");
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        logger.warn("onCompleted");
                    }
                })
                .subscribe();
        errorLatch.await(30, TimeUnit.SECONDS);

        // newServerWithInitialData test server after client started and unable to connect
        // the client should recover and get expected data
        TestSseServerFactory.newServerWithInitialData(serverPort, TEST_EVENT_DATA);

        latch.await(30, TimeUnit.SECONDS);
        TestSseServerFactory.stopAllRunning();
    }

}
