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

import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ATTEMPTED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ESTABLISHED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_UNSUBSCRIBED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SERVER_FOUND;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SOURCE_COMPLETED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ENDED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ESTABLISHED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.http.impl.HttpClientFactories;
import io.mantisrx.runtime.source.http.impl.HttpRequestFactories;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType;
import io.mantisrx.runtime.source.http.impl.ServerContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;


public class ContextualHttpSourceTest {

    private final static int SEED_PORT = 4000;
    private final static int PORT_RANGE = 1000;
    private static LocalServerProvider localServerProvider;
    private static EventType[] expectedEvents = new EventType[] {CONNECTION_ATTEMPTED, SUBSCRIPTION_ESTABLISHED, CONNECTION_UNSUBSCRIBED, CONNECTION_ESTABLISHED, SERVER_FOUND, SOURCE_COMPLETED, SUBSCRIPTION_ENDED};
    private static Set<EventType> EXPECTED_EVENTS_SETS = new HashSet<>(Arrays.asList(expectedEvents));
    private TestSourceObserver sourceObserver = new TestSourceObserver();
    // Just make sure the unused port is out side the range of possible ports: [SEED_PORT, SEED_PORT + PORT_RANGE)

    @BeforeAll
    public static void init() {
        int portStart = new Random().nextInt(PORT_RANGE) + SEED_PORT;

        localServerProvider = new LocalServerProvider();
        localServerProvider.start(3, portStart);
    }

    @AfterAll
    public static void shutdown() throws Exception {
        localServerProvider.shutDown();
    }

    @BeforeEach
    public void setup() {
        sourceObserver = new TestSourceObserver();
    }

    @Test
    public void canStreamFromMultipleServersWithCorrectContext() throws Exception {
        ContextualHttpSource<ServerSentEvent> source = HttpSources
                .contextualSource(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createGetFactory("test/stream"))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        final CopyOnWriteArraySet<ServerInfo> connectedServers = new CopyOnWriteArraySet<>();

        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .doOnNext(new Action1<ServerContext<ServerSentEvent>>() {
                    @Override
                    public void call(ServerContext<ServerSentEvent> pair) {
                        assertTrue(pair.getValue().contentAsString().contains("line"));
                        counter.incrementAndGet();

                        String msg = pair.getValue().contentAsString();
                        result.putIfAbsent(msg, new AtomicInteger());
                        result.get(msg).incrementAndGet();

                        connectedServers.add(pair.getServer());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        System.out.println("completed");
                    }
                })
                .doAfterTerminate(new Action0() {
                    @Override
                    public void call() {
                        done.countDown();
                    }
                })
                .subscribe();

        long waitSeconds = 3;
        boolean timedout = !done.await(waitSeconds, TimeUnit.SECONDS);
        if (timedout) {
            fail(String.format("Waited at least %d seconds for the test to finish. Something is wrong", waitSeconds));
        }

        assertEquals(localServerProvider.serverSize(), connectedServers.size(), "There should be as many as provided servers");
        assertEquals(counter.get(), RequestProcessor.smallStreamContent.size() * localServerProvider.serverSize(), String.format("%d servers => the result has %d times of a single stream", localServerProvider.serverSize(), localServerProvider.serverSize()));
        for (String data : RequestProcessor.smallStreamContent) {
            assertEquals(localServerProvider.serverSize(), result.get(data).get(), String.format("%d servers => %d identical copies per message", localServerProvider.serverSize(), localServerProvider.serverSize()));
        }

        for (ServerInfo server : localServerProvider.getServerInfos()) {
            assertEquals(1, sourceObserver.getCount(server, EventType.SOURCE_COMPLETED), "There should be one completion per server");
            assertEquals(1, sourceObserver.getCount(server, EventType.CONNECTION_UNSUBSCRIBED), "There should be one un-subscription per server");
            assertEquals(0, sourceObserver.getCount(server, EventType.SUBSCRIPTION_FAILED), "There should be no error");
            assertEquals(1, sourceObserver.getCount(server, EventType.CONNECTION_ESTABLISHED), "There should be one connection per server");
        }

        assertEquals(1, sourceObserver.getCompletionCount());
        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertEquals(EXPECTED_EVENTS_SETS, events);

        for (EventType event : events) {
            assertEquals(localServerProvider.serverSize(), sourceObserver.getEventCount(event), "Each event should be recorded exactly once per server");
        }

    }

}
