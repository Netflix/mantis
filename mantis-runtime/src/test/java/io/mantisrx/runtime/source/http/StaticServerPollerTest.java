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

package io.mantisrx.runtime.source.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.runtime.source.http.impl.StaticServerPoller;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.functions.Action1;


public class StaticServerPollerTest {

    private Set<ServerInfo> servers;
    private int pollingInterval = 1;

    @BeforeEach
    public void setUp() throws Exception {
        servers = new HashSet<>();
        for (int i = 0; i < 5; ++i) {
            servers.add(new ServerInfo("host" + i, i));
        }
    }

    @AfterEach
    public void tearDown() throws Exception {

    }

    @Test
    public void pollingIsScheduled() throws Exception {
        StaticServerPoller poller = new StaticServerPoller(servers, pollingInterval);
        final AtomicInteger count = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(5);
        long start = System.currentTimeMillis();
        poller.servers()
                .doOnNext(new Action1<Set<ServerInfo>>() {
                    @Override
                    public void call(Set<ServerInfo> data) {
                        assertEquals(servers, data, "We should always see the same set of servers");
                        count.incrementAndGet();
                        done.countDown();
                    }
                })
                .subscribe();

        done.await();
        long elapsed = (System.currentTimeMillis() - start) / 1000;

        System.out.println(elapsed);
        assertTrue(count.get() == 5 && elapsed <= 6, "The poller should have polled 5 times and the elaspsed time should be greater than 3");
    }
}
