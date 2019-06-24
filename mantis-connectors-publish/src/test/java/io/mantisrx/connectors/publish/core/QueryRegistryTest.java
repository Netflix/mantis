/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.publish.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.mantisrx.publish.proto.MantisServerSubscription;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


class QueryRegistryTest {

    @Test
    void registerQueryTest() {
        try {
            QueryRegistry queryRegistry = new QueryRegistry.Builder().build();
            fail();
        } catch (IllegalArgumentException ignored) {
        }

        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = QueryRegistry.ANY;
        try {
            queryRegistry.registerQuery(targetApp, null, "true");
            fail();
        } catch (Exception ignored) {
        }

        try {
            queryRegistry.registerQuery(targetApp, "subId", null);
            fail();
        } catch (Exception ignored) {
        }

        queryRegistry.registerQuery("myApp", "subId", "true");

        queryRegistry.registerQuery("myApp2", "subId", "false");

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp("myApp");

        assertEquals(1, currentSubs.size());

        List<MantisServerSubscription> currentSubs2 = queryRegistry.getCurrentSubscriptionsForApp("myApp2");

        assertEquals(1, currentSubs2.size());

        Map<String, List<MantisServerSubscription>> allSubscriptions = queryRegistry.getAllSubscriptions();

        assertEquals(2, allSubscriptions.size());

        assertTrue(allSubscriptions.containsKey("myApp"));

        assertTrue(allSubscriptions.containsKey("myApp2"));
    }

    @Test
    void registerQueryForAnyLookupSpecificAppTest() {
        try {
            QueryRegistry queryRegistry = new QueryRegistry.Builder().build();
            fail();
        } catch (IllegalArgumentException ignored) { }

        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = QueryRegistry.ANY;

        queryRegistry.registerQuery(targetApp, "subId", "true");

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp("myApp");

        assertEquals(1, currentSubs.size());
    }


    @Test
    void registerQueryForAppLookupAnyTest() {
        try {
            QueryRegistry queryRegistry = new QueryRegistry.Builder().build();
            fail();
        } catch (IllegalArgumentException ignored) { }

        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = QueryRegistry.ANY;

        queryRegistry.registerQuery("myApp", "subId", "true");

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp(targetApp);

        assertEquals(0, currentSubs.size());
    }


    @Test
    void deregisterQueryTest() throws InterruptedException {
        try {
            QueryRegistry queryRegistry = new QueryRegistry.Builder().build();
            fail();
        } catch (IllegalArgumentException ignored) { }

        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = "myapp";
        try {
            queryRegistry.registerQuery(targetApp, null, "true");
            fail();
        } catch (Exception ignored) {

        }

        try {
            queryRegistry.registerQuery(targetApp, "subId", null);
            fail();
        } catch (Exception ignored) {

        }

        queryRegistry.registerQuery(targetApp, "subId", "true");

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp(targetApp);

        assertEquals(1, currentSubs.size());

        queryRegistry.deregisterQuery(targetApp, "subId", "true");

        Thread.sleep(500);

        currentSubs = queryRegistry.getCurrentSubscriptionsForApp(QueryRegistry.ANY);

        assertEquals(0, currentSubs.size());
    }

    @Test
    void registerIdenticalQueryGetsDedupedTest() {
        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = "myApp";

        int concurrency = 5;
        CountDownLatch latch = new CountDownLatch(1);

        CountDownLatch endLatch = new CountDownLatch(concurrency);

        Runnable task = () -> {
            try {
                latch.await();
                queryRegistry.registerQuery(targetApp, "subId", "true");
                endLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(task);
        }

        latch.countDown();

        try {
            endLatch.await();
            List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp(targetApp);

            assertEquals(1, currentSubs.size());

            assertEquals("myPrefix_subId", currentSubs.get(0).getSubscriptionId());
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void registerIdenticalQueryRemovalTest() throws InterruptedException {
        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = "myApp";
        int concurrency = 5;

        CountDownLatch latch = new CountDownLatch(1);

        CountDownLatch endLatch = new CountDownLatch(concurrency);
        CountDownLatch removeQueryEndLatch = new CountDownLatch(concurrency - 1);

        Runnable addQueryTask = () -> {
            try {
                latch.await();
                queryRegistry.registerQuery(targetApp, "subId", "true");
                endLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        };

        Runnable removeQueryTask = () -> {
            try {
                latch.await();
                queryRegistry.deregisterQuery(targetApp, "subId", "true");

                removeQueryEndLatch.countDown();
            } catch (InterruptedException ignored) {
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency * 2);

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(addQueryTask);
        }

        for (int i = 0; i < concurrency - 1; i++) {
            executorService.submit(removeQueryTask);
        }

        latch.countDown();

        removeQueryEndLatch.await();

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp(targetApp);

        assertEquals(1, currentSubs.size());

        assertEquals("myPrefix_subId", currentSubs.get(0).getSubscriptionId());
    }

    @Test
    @Disabled
    void registerQueryMultipleAppsRemovalTest() throws InterruptedException {
        QueryRegistry queryRegistry = new QueryRegistry.Builder().withClientIdPrefix("myPrefix").build();

        String targetApp = "myApp";
        String targetApp2 = "myApp2";
        int concurrency = 5;

        CountDownLatch latch = new CountDownLatch(1);

        CountDownLatch endLatch = new CountDownLatch(concurrency);
        CountDownLatch removeQueryEndLatch = new CountDownLatch(concurrency - 1);

        Runnable addQueryTask = () -> {
            try {
                latch.await();
                queryRegistry.registerQuery(targetApp, "subId", "true");
                endLatch.countDown();
            } catch (InterruptedException ignored) {
            }
        };

        Runnable addQueryTask2 = () -> {
            try {
                latch.await();
                queryRegistry.registerQuery(targetApp2, "subId", "true");
                endLatch.countDown();
            } catch (InterruptedException ignored) {
            }
        };

        Runnable removeQueryTask = () -> {
            try {
                latch.await();
                queryRegistry.deregisterQuery(targetApp, "subId", "true");

                removeQueryEndLatch.countDown();
            } catch (InterruptedException ignored) {
            }
        };

        Runnable removeQueryTask2 = () -> {
            try {
                latch.await();
                queryRegistry.deregisterQuery(targetApp2, "subId", "true");

                removeQueryEndLatch.countDown();
            } catch (InterruptedException ignored) {
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency * 2);

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(addQueryTask);
            executorService.submit(addQueryTask2);
        }

        for (int i = 0; i < concurrency - 1; i++) {
            executorService.submit(removeQueryTask);
            executorService.submit(removeQueryTask2);
        }

        latch.countDown();

        removeQueryEndLatch.await();

        List<MantisServerSubscription> currentSubs = queryRegistry.getCurrentSubscriptionsForApp(targetApp);

        assertEquals(1, currentSubs.size());

        List<MantisServerSubscription> currentSubs2 = queryRegistry.getCurrentSubscriptionsForApp(targetApp2);

        assertEquals(1, currentSubs2.size());

        assertEquals("myPrefix_subId", currentSubs.get(0).getSubscriptionId());
    }
}
