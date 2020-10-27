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

package io.mantisrx.runtime.source.http.impl;

import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ATTEMPTED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_ESTABLISHED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.CONNECTION_UNSUBSCRIBED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SERVER_FOUND;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SOURCE_COMPLETED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ENDED;
import static io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType.SUBSCRIPTION_ESTABLISHED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.http.ClientResumePolicy;
import io.mantisrx.runtime.source.http.HttpClientFactory;
import io.mantisrx.runtime.source.http.HttpServerProvider;
import io.mantisrx.runtime.source.http.LocalServerProvider;
import io.mantisrx.runtime.source.http.LocalServerProvider.Server;
import io.mantisrx.runtime.source.http.RequestProcessor;
import io.mantisrx.runtime.source.http.TestSourceObserver;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.Builder;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import mantis.io.reactivex.netty.client.RxClient.ClientConfig;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;


public class HttpSourceImplTest {

    private final static int SEED_PORT = 4000;
    private final static int PORT_RANGE = 1000;
    // Just make sure the unused port is out side the range of possible ports: [SEED_PORT, SEED_PORT + PORT_RANGE)
    private final static int UNUSED_PORT = 8182;
    private static LocalServerProvider localServerProvider;
    private static EventType[] expectedEvents = new EventType[] {CONNECTION_ATTEMPTED, SUBSCRIPTION_ESTABLISHED, CONNECTION_UNSUBSCRIBED, CONNECTION_ESTABLISHED, SERVER_FOUND, SOURCE_COMPLETED, SUBSCRIPTION_ENDED};
    private static Set<EventType> EXPECTED_EVENTS_SETS = new HashSet<>(Arrays.asList(expectedEvents));
    private TestSourceObserver sourceObserver = new TestSourceObserver();

    @BeforeClass
    public static void init() {
        int port = new Random().nextInt(PORT_RANGE) + SEED_PORT;

        System.setProperty("log4j.rootLogger", "INFO, CONSOLE");
        System.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
        System.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
        System.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{HH:mm:ss,SSS} [%t] %-5p %x %C{1} : %m%n");

        localServerProvider = new LocalServerProvider();
        localServerProvider.start(3, port);
    }

    @AfterClass
    public static void shutDown() throws Exception {
        localServerProvider.shutDown();
    }

    @Before
    public void setup() {
        sourceObserver = new TestSourceObserver();
    }

    @Test
    public void testGettingStreamFromMultipleServers() throws Exception {
        HttpSourceImpl<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> source = createStreamingSource();

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .doOnNext(new Action1<ServerContext<ServerSentEvent>>() {
                    @Override
                    public void call(ServerContext<ServerSentEvent> pair) {
                        assertTrue(pair.getValue().contentAsString().contains("line"));
                        counter.incrementAndGet();

                        String msg = pair.getValue().contentAsString();
                        result.putIfAbsent(msg, new AtomicInteger());
                        result.get(msg).incrementAndGet();

                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
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

        assertEquals(String.format("%d servers => the result has %d times of a single stream", localServerProvider.serverSize(), localServerProvider.serverSize()), counter.get(), RequestProcessor.smallStreamContent.size() * localServerProvider.serverSize());
        for (String data : RequestProcessor.smallStreamContent) {
            assertEquals(String.format("%d servers => %d identical copies per message", localServerProvider.serverSize(), localServerProvider.serverSize()), localServerProvider.serverSize(), result.get(data).get());
        }

        for (Server server : localServerProvider.getServers()) {
            assertEquals("There should be one completion per server", 1, sourceObserver.getCount(toServerInfo(server), EventType.SOURCE_COMPLETED));
            assertEquals("There should be one un-subscription per server", 1, sourceObserver.getCount(toServerInfo(server), EventType.CONNECTION_UNSUBSCRIBED));
            assertEquals("There should be no error", 0, sourceObserver.getCount(toServerInfo(server), EventType.SUBSCRIPTION_FAILED));
            assertEquals("There should be one connection per server", 1, sourceObserver.getCount(toServerInfo(server), EventType.CONNECTION_ESTABLISHED));
        }

        assertEquals(1, sourceObserver.getCompletionCount());
        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertEquals(EXPECTED_EVENTS_SETS, events);

        for (EventType event : events) {
            assertEquals("Each event should be recorded exactly once per server", localServerProvider.serverSize(), sourceObserver.getEventCount(event));
        }

        assertEquals("completed source should clean up its retry servers", 0, source.getRetryServers().size());
    }

    @Test
    public void testRemovedServerWillBeUnsubscribed() throws Exception {
        HttpSourceImpl<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> source = createStreamingSource("test/infStream");

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        final ConcurrentMap<ServerInfo, CountDownLatch> serverRemovalLatch = new ConcurrentHashMap<>();
        for (Server server : localServerProvider.getServers()) {
            serverRemovalLatch.put(toServerInfo(server), new CountDownLatch(1));
        }

        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .doOnNext(new Action1<ServerContext<ServerSentEvent>>() {
                    @Override
                    public void call(ServerContext<ServerSentEvent> pair) {
                        try {
                            assertTrue(pair.getValue().contentAsString().contains("line"));
                            counter.incrementAndGet();

                            String msg = pair.getValue().contentAsString();
                            result.putIfAbsent(msg, new AtomicInteger());
                            result.get(msg).incrementAndGet();
                        } finally {
                            serverRemovalLatch.get(pair.getServer()).countDown();
                        }
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
                    }
                })
                .doAfterTerminate(new Action0() {
                    @Override
                    public void call() {
                        done.countDown();
                    }
                })
                .subscribe();

        for (Server server : localServerProvider.getServers()) {
            serverRemovalLatch.get(toServerInfo(server)).await();
            localServerProvider.removeServer(toServerInfo(server));
        }

        long waitSeconds = 5;
        boolean timedout = !done.await(waitSeconds, TimeUnit.SECONDS);
        if (timedout) {
            fail(String.format("Waited at least %d seconds for the test to finish. Connection to at least one server is not unsubscribed.", waitSeconds));
        }

        assertTrue(String.format("Each server should emit at least one event before being canceled. Expected counter >= %d, actual counter: %d", localServerProvider.serverSize(), counter.get()), counter.get() >= localServerProvider.serverSize());
        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            assertEquals("There should be one completion per server", 1, sourceObserver.getCount(serverInfo, EventType.SOURCE_COMPLETED));
            assertEquals("There should be one un-subscription per server", 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_UNSUBSCRIBED));
            assertEquals("There should be no error", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_FAILED));
            assertEquals("There should be one connection per server", 1, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));

            assertEquals(String.format("There should be exactly one cancellation event per server for %d servers. ", localServerProvider.serverSize()), 1, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_CANCELED));
        }

        assertEquals("The source should emit exactly one completion event", 1, sourceObserver.getCompletionCount());
        assertEquals("The server should not have any error event", 0, sourceObserver.getErrorCount());


        Set<EventType> events = sourceObserver.getEvents();
        assertEquals("Each server should have one occurrence per event type except server failure event", (EventType.values().length - 1), events.size());

        for (EventType event : events) {
            assertEquals("Each event should be recorded exactly once", localServerProvider.serverSize(), sourceObserver.getEventCount(event));
        }
    }

    @Test
    public void testGettingSingleEntityFromMultipleServers() throws Exception {
        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = createSingleEntitySource();

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .map(new Func1<ServerContext<ByteBuf>, ServerContext<String>>() {
                    @Override
                    public ServerContext<String> call(ServerContext<ByteBuf> pair) {
                        return new ServerContext<>(pair.getServer(), pair.getValue().toString(Charset.defaultCharset()));
                    }
                })
                .doOnNext(new Action1<ServerContext<String>>() {
                    @Override
                    public void call(ServerContext<String> pair) {
                        counter.incrementAndGet();
                        assertEquals(RequestProcessor.SINGLE_ENTITY_RESPONSE, pair.getValue());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
                    }
                })
                .doAfterTerminate(new Action0() {
                    @Override
                    public void call() {
                        done.countDown();
                    }
                })
                .subscribe();

        done.await(3000, TimeUnit.SECONDS);
        assertEquals(
                String.format("There should be exactly one response from each of the %d servers", localServerProvider.serverSize()),
                localServerProvider.serverSize(),
                counter.get());

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            assertEquals("There should be one completion per server", 1, sourceObserver.getCount(serverInfo, EventType.SOURCE_COMPLETED));
            assertEquals("There should be one un-subscription per server", 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_UNSUBSCRIBED));
            assertEquals("There should be no error", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_FAILED));
            assertEquals("There should be one connection per server", 1, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));
        }

        assertEquals(1, sourceObserver.getCompletionCount());
        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertEquals(EXPECTED_EVENTS_SETS, events);

        for (EventType event : events) {
            assertEquals("Each event should be recorded exactly once per server", localServerProvider.serverSize(), sourceObserver.getEventCount(event));
        }
    }

    @Test
    public void testConnectionFailureShouldBeCaptured() throws Exception {
        HttpClientFactory<ByteBuf, ByteBuf> factory = new HttpClientFactory<ByteBuf, ByteBuf>() {
            @Override
            public HttpClient<ByteBuf, ByteBuf> createClient(ServerInfo server) {
                HttpClientBuilder<ByteBuf, ByteBuf> clientBuilder = new HttpClientBuilder<>(server.getHost(), server.getPort());

                return clientBuilder.channelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100).build();
            }
        };

        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = HttpSourceImpl
                .builder(
                        factory,
                        HttpRequestFactories.createGetFactory("/"),
                        HttpSourceImpl.<ByteBuf>contextWrapper()
                )
                .withActivityObserver(sourceObserver)
                .withServerProvider(
                        new HttpServerProvider() {
                            @Override
                            public Observable<ServerInfo> getServersToAdd() {
                                return Observable.just(new ServerInfo("localhost", UNUSED_PORT));
                            }

                            @Override
                            public Observable<ServerInfo> getServersToRemove() {
                                return Observable.empty();
                            }
                        }
                ).build();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final AtomicReference<ServerContext<ByteBuf>> items = new AtomicReference<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .subscribe(new Subscriber<ServerContext<ByteBuf>>() {
                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        ex.set(e);
                        latch.countDown();
                    }

                    @Override
                    public void onNext(ServerContext<ByteBuf> pair) {
                        items.set(pair);
                    }
                });

        if (!latch.await(5, TimeUnit.HOURS)) {
            fail("The test case should finish way sooner than 5 seconds. ");
        }

        Assert.assertNull("The connection error should be captured per server, therefore not propagated up to the entire source.", ex.get());
        Assert.assertNull("There should be no emitted item due to connection timeout", items.get());
    }

    @Test
    public void testTimeoutShouldUnsubscribeServer() throws Exception {

        HttpClientFactory<ByteBuf, ByteBuf> factory = new HttpClientFactory<ByteBuf, ByteBuf>() {
            @Override
            public HttpClient<ByteBuf, ByteBuf> createClient(ServerInfo server) {
                ClientConfig clientConfig = new ClientConfig.Builder()
                        .readTimeout(10, TimeUnit.MILLISECONDS)
                        .build();

                return new HttpClientBuilder<ByteBuf, ByteBuf>(server.getHost(), server.getPort())
                        .config(clientConfig).build();
            }
        };

        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = HttpSourceImpl
                .builder(
                        factory,
                        HttpRequestFactories.createGetFactory("test/timeout?timeout=10000"),
                        HttpSourceImpl.<ByteBuf>contextWrapper()
                )
                .withActivityObserver(sourceObserver)
                .resumeWith(new ClientResumePolicy<ByteBuf, ByteBuf>() {

                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onError(
                            ServerClientContext<ByteBuf, ByteBuf> clientContext,
                            int attempts, Throwable error) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onCompleted(
                            ServerClientContext<ByteBuf, ByteBuf> clientContext,
                            int attempts) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                })
                .withServerProvider(
                        new HttpServerProvider() {
                            @Override
                            public Observable<ServerInfo> getServersToAdd() {
                                return Observable.from(localServerProvider.getServers()).map(new Func1<Server, ServerInfo>() {
                                    @Override
                                    public ServerInfo call(Server server) {
                                        return toServerInfo(server);
                                    }
                                });
                            }

                            @Override
                            public Observable<ServerInfo> getServersToRemove() {
                                return Observable.empty();
                            }
                        }
                ).build();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final AtomicReference<ServerContext<ByteBuf>> items = new AtomicReference<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .subscribe(new Subscriber<ServerContext<ByteBuf>>() {
                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        ex.set(e);
                        latch.countDown();
                    }

                    @Override
                    public void onNext(ServerContext<ByteBuf> pair) {
                        items.set(pair);
                    }
                });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("The test case should finish way sooner than 5 seconds. ");
        }

        Assert.assertNull("The timeout error should be captured by the client so it does not surface to the source", ex.get());
        Assert.assertNull("There should be no emitted item due to connection timeout", items.get());

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            assertEquals("There should be no source level error", 0, sourceObserver.getErrorCount());
            assertEquals("There should be one connection attempt per server", 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ATTEMPTED));
            assertEquals("There should be no established connection per server due to read timeout. ", 0, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ESTABLISHED));
            assertEquals("There should no subscribed server because of read timeout", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));
        }
    }

    @Test
    public void testStreamingErrorFromServerWillNotCompleteSource() throws Exception {

        int eventCount = 20;
        HttpSourceImpl<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> source = createStreamingSource("test/finiteStream?count=" + eventCount);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final ConcurrentHashMap<ServerInfo, Set<String>> items = new ConcurrentHashMap<>();

        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .subscribe(new Subscriber<ServerContext<ServerSentEvent>>() {
                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        ex.set(e);
                        latch.countDown();
                    }

                    @Override
                    public void onNext(ServerContext<ServerSentEvent> pair) {
                        items.putIfAbsent(pair.getServer(), new HashSet<String>());
                        items.get(pair.getServer()).add(pair.getValue().contentAsString());
                    }
                });

        if (latch.await(5, TimeUnit.SECONDS)) {
            fail("The test case should not finish at all");
        }

        Assert.assertNull("The timeout error should be captured by the client so it does not surface to the source", ex.get());

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            assertEquals("There should be no source level error", 0, sourceObserver.getErrorCount());
            assertEquals("There should be one connection attempt per server", 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ATTEMPTED));
            assertEquals("There should be one established connection per server ", 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ESTABLISHED));
            assertEquals("There should no subscribed server because of read timeout", 1, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));

            assertEquals(String.format("There should be %d item before simulated error per server", eventCount), eventCount, items.get(serverInfo).size());

        }
    }

    @Test
    public void testResumeOnCompletion() throws Exception {
        final int maxRepeat = 10;
        final CountDownLatch countGate = new CountDownLatch(maxRepeat * localServerProvider.serverSize());

        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = createSingleEntitySource(new ClientResumePolicy<ByteBuf, ByteBuf>() {
            @Override
            public Observable<HttpClientResponse<ByteBuf>> onError(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts, Throwable error) {
                return null;
            }

            @Override
            public Observable<HttpClientResponse<ByteBuf>> onCompleted(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts) {
                countGate.countDown();
                if (attempts < maxRepeat) {
                    return clientContext.newResponse();
                } else {
                    return null;
                }
            }
        });

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .map(new Func1<ServerContext<ByteBuf>, ServerContext<String>>() {
                    @Override
                    public ServerContext<String> call(ServerContext<ByteBuf> pair) {
                        try {
                            return new ServerContext<>(pair.getServer(), pair.getValue().retain().toString(Charset.defaultCharset()));
                        } catch (Throwable e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                })
                .doOnNext(new Action1<ServerContext<String>>() {
                    @Override
                    public void call(ServerContext<String> pair) {
                        counter.incrementAndGet();
                        assertEquals(RequestProcessor.SINGLE_ENTITY_RESPONSE, pair.getValue());
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
                    }
                })
                .doAfterTerminate(new Action0() {
                    @Override
                    public void call() {
                        try {
                            countGate.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        done.countDown();
                    }
                })
                .subscribe();

        long wait = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail(String.format("All streaming should be done within %d seconds. ", wait));
        }
        assertEquals(
                String.format("There should be exactly %d response from each of the %d servers",
                        maxRepeat,
                        localServerProvider.serverSize()),
                maxRepeat * localServerProvider.serverSize(),
                counter.get());

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            assertEquals(
                    String.format("There should be %d completion per server as resumption function should called %d times",
                            maxRepeat,
                            maxRepeat - 1),
                    maxRepeat,
                    sourceObserver.getCount(serverInfo, EventType.SOURCE_COMPLETED));

            assertEquals("There should be no error", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_FAILED));
            assertEquals(
                    String.format("Connection per server should have been established %d times", maxRepeat),
                    10,
                    sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));
        }

        assertEquals(
                String.format("There are %d repeats, but there should be only one final completion", maxRepeat - 1),
                1,
                sourceObserver.getCompletionCount());

        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertEquals(EXPECTED_EVENTS_SETS, events);

        assertEquals("Each connection should be unsubscribed once by the subscriber", localServerProvider.serverSize(), sourceObserver.getEventCount(CONNECTION_UNSUBSCRIBED));
        for (EventType eventType : new EventType[] {EventType.SOURCE_COMPLETED, EventType.SUBSCRIPTION_ESTABLISHED, EventType.CONNECTION_ATTEMPTED, EventType.CONNECTION_ESTABLISHED}) {
            assertEquals(
                    String.format("Event %s should be recorded exactly %d times per server", eventType, maxRepeat),
                    maxRepeat * localServerProvider.serverSize(),
                    sourceObserver.getEventCount(eventType));
        }


        assertEquals(
                "Event SERVER_FOUND should be recorded exactly once per server",
                localServerProvider.serverSize(),
                sourceObserver.getEventCount(EventType.SERVER_FOUND));
    }

    @Test
    public void testResumeOnCompletionButNotOnRemovedServers() throws Exception {
        final int maxRepeat = 10;
        final int cutOff = 5;
        final CountDownLatch countGate = new CountDownLatch(localServerProvider.serverSize() * (cutOff - 1));
        final ConcurrentHashMap<ServerInfo, AtomicInteger> resumptionCounts = new ConcurrentHashMap<>();
        final ConcurrentHashMap<ServerInfo, AtomicInteger> counterPerServer = new ConcurrentHashMap<>();

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);
            resumptionCounts.put(serverInfo, new AtomicInteger(0));
            counterPerServer.put(serverInfo, new AtomicInteger(0));
        }

        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = createSingleEntitySource(new ClientResumePolicy<ByteBuf, ByteBuf>() {
            @Override
            public Observable<HttpClientResponse<ByteBuf>> onError(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts, Throwable error) {
                return null;
            }

            @Override
            public Observable<HttpClientResponse<ByteBuf>> onCompleted(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts) {
                resumptionCounts.get(clientContext.getServer()).incrementAndGet();
                countGate.countDown();
                if (attempts < maxRepeat) {
                    return clientContext.newResponse();
                } else {
                    return null;
                }
            }
        });

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);

        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .map(new Func1<ServerContext<ByteBuf>, ServerContext<String>>() {
                    @Override
                    public ServerContext<String> call(ServerContext<ByteBuf> pair) {
                        return new ServerContext<>(pair.getServer(), pair.getValue().toString(Charset.defaultCharset()));
                    }
                })
                .doOnNext(new Action1<ServerContext<String>>() {
                    @Override
                    public void call(ServerContext<String> context) {
                        counter.incrementAndGet();
                        assertEquals(RequestProcessor.SINGLE_ENTITY_RESPONSE, context.getValue());

                        ServerInfo server = context.getServer();
                        counterPerServer.get(server).incrementAndGet();
                        if (counterPerServer.get(server).get() > cutOff) {
                            localServerProvider.removeServer(server);
                        }

                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        fail("Unexpected failure: " + throwable);
                    }
                })
                .doAfterTerminate(new Action0() {
                    @Override
                    public void call() {
                        try {
                            countGate.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        done.countDown();
                    }
                })
                .subscribe();

        long wait = 5;
        if (!done.await(wait, TimeUnit.SECONDS)) {
            fail(String.format("All streaming should be done within %d seconds. ", wait));
        }

        assertEquals("All server should be resumed", localServerProvider.serverSize(), resumptionCounts.size());
        for (ServerInfo server : resumptionCounts.keySet()) {
            assertTrue(
                    String.format("The server %s:%s should be resumed fewer than %d times", server.getHost(), server.getPort(), maxRepeat),
                    maxRepeat > resumptionCounts.get(server).get());
        }
        assertTrue(
                String.format("There should be at least %d response from each of the %d servers",
                        cutOff + 1,
                        localServerProvider.serverSize()),
                (cutOff + 1) * localServerProvider.serverSize() <= counter.get());

        for (Server server : localServerProvider.getServers()) {
            ServerInfo serverInfo = toServerInfo(server);

            assertEquals("There should be no error", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_FAILED));
        }

        assertEquals(
                String.format("There are %d repeats, but there should be only one final completion", maxRepeat - 1),
                1,
                sourceObserver.getCompletionCount());

        assertEquals("There should be no error", 0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        Set<EventType> expectedEvents = new HashSet<>();
        expectedEvents.addAll(Arrays.asList(
                EventType.SUBSCRIPTION_CANCELED,
                EventType.SERVER_FOUND,
                EventType.CONNECTION_ATTEMPTED,
                EventType.CONNECTION_ESTABLISHED,
                EventType.SUBSCRIPTION_ESTABLISHED,
                EventType.SOURCE_COMPLETED,
                EventType.SUBSCRIPTION_ENDED,
                EventType.CONNECTION_UNSUBSCRIBED));

        assertEquals(expectedEvents, events);

        assertEquals("Each connection should be unsubscribed once by the subscriber", localServerProvider.serverSize(), sourceObserver.getEventCount(CONNECTION_UNSUBSCRIBED));
        for (EventType eventType : new EventType[] {EventType.SOURCE_COMPLETED, EventType.SUBSCRIPTION_ESTABLISHED, EventType.CONNECTION_ATTEMPTED, EventType.CONNECTION_ESTABLISHED}) {
            assertTrue(
                    String.format("Event %s should be recorded at least %d times per server", eventType, cutOff),
                    (cutOff + 1) * localServerProvider.serverSize() <= sourceObserver.getEventCount(eventType));
        }

        for (EventType eventType : new EventType[] {EventType.SERVER_FOUND, EventType.SUBSCRIPTION_CANCELED}) {
            assertEquals(
                    String.format("Event %s should be recorded exactly once per server", eventType),
                    localServerProvider.serverSize(),
                    sourceObserver.getEventCount(eventType));
        }
    }

    @Test
    public void testResumeOnTimeout() throws Exception {

        HttpClientFactory<ByteBuf, ByteBuf> factory = new HttpClientFactory<ByteBuf, ByteBuf>() {
            @Override
            public HttpClient<ByteBuf, ByteBuf> createClient(ServerInfo server) {
                ClientConfig clientConfig = new ClientConfig.Builder()
                        .readTimeout(10, TimeUnit.MILLISECONDS)
                        .build();

                return new HttpClientBuilder<ByteBuf, ByteBuf>(server.getHost(), server.getPort())
                        .config(clientConfig).build();
            }
        };

        final ConcurrentMap<ServerInfo, AtomicInteger> resumptions = new ConcurrentHashMap<>();
        for (ServerInfo server : localServerProvider.getServerInfos()) {
            resumptions.put(server, new AtomicInteger());
        }

        final int maxRepeat = 5;
        HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> source = HttpSourceImpl
                .builder(
                        factory,
                        HttpRequestFactories.createGetFactory("test/timeout?timeout=10000"),
                        HttpSourceImpl.<ByteBuf>contextWrapper()
                )
                .withActivityObserver(sourceObserver)
                .resumeWith(new ClientResumePolicy<ByteBuf, ByteBuf>() {
                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onError(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts, Throwable error) {
                        if (attempts <= maxRepeat) {
                            resumptions.get(clientContext.getServer()).incrementAndGet();
                            return clientContext.newResponse();
                        }
                        return null;
                    }

                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> onCompleted(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts) {
                        return null;
                    }
                })
                .withServerProvider(localServerProvider).build();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final AtomicReference<ServerContext<ByteBuf>> items = new AtomicReference<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .subscribe(new Subscriber<ServerContext<ByteBuf>>() {
                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        ex.set(e);
                        latch.countDown();
                    }

                    @Override
                    public void onNext(ServerContext<ByteBuf> pair) {
                        items.set(pair);
                    }
                });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("The test case should finish way sooner than 10 seconds. ");
        }

        Assert.assertNull("The timeout error should be captured by the client so it does not surface to the source", ex.get());
        Assert.assertNull("There should be no emitted item due to connection timeout", items.get());

        for (ServerInfo serverInfo : localServerProvider.getServerInfos()) {
            assertEquals("There should be no source level error", 0, sourceObserver.getErrorCount());
            assertEquals("There should be one connection attempt per server per retry", maxRepeat + 1, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ATTEMPTED));
            assertEquals("There should be no established connection per server due to read timeout. ", 0, sourceObserver.getCount(serverInfo, EventType.CONNECTION_ESTABLISHED));
            assertEquals("There should no subscribed server because of read timeout", 0, sourceObserver.getCount(serverInfo, EventType.SUBSCRIPTION_ESTABLISHED));

            assertEquals("Each server will repeat exactly " + maxRepeat + " times", maxRepeat, resumptions.get(serverInfo).get());
        }
    }

    private ServerInfo toServerInfo(Server server) {
        return new ServerInfo(server.getHost(), server.getPort());
    }

    private HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> createSingleEntitySource() {
        // The default policy resumes nothing
        return createSingleEntitySource(new ClientResumePolicy<ByteBuf, ByteBuf>() {
            @Override
            public Observable<HttpClientResponse<ByteBuf>> onError(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts, Throwable error) {
                return null;
            }

            @Override
            public Observable<HttpClientResponse<ByteBuf>> onCompleted(ServerClientContext<ByteBuf, ByteBuf> clientContext, int attempts) {
                return null;
            }
        });
    }

    private HttpSourceImpl<ByteBuf, ByteBuf, ServerContext<ByteBuf>> createSingleEntitySource(ClientResumePolicy<ByteBuf, ByteBuf> resumePolicy) {
        Builder<ByteBuf, ByteBuf, ServerContext<ByteBuf>> builder =
                HttpSourceImpl.builder(
                        HttpClientFactories.<ByteBuf>defaultFactory(),
                        HttpRequestFactories.createGetFactory("test/singleEntity"),
                        HttpSourceImpl.<ByteBuf>contextWrapper()
                );

        return builder.withActivityObserver(sourceObserver)
                .withServerProvider(localServerProvider)
                .resumeWith(resumePolicy)
                .build();
    }

    private HttpSourceImpl<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> createStreamingSource() {
        String uri = "test/stream";
        return createStreamingSource(uri);
    }

    private HttpSourceImpl<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> createStreamingSource(String uri) {
        Builder<ByteBuf, ServerSentEvent, ServerContext<ServerSentEvent>> builder =
                HttpSourceImpl.builder(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createGetFactory(uri),
                        HttpSourceImpl.<ServerSentEvent>contextWrapper()

                );

        return builder
                .withActivityObserver(sourceObserver)
                .withServerProvider(localServerProvider)
                .resumeWith(new ClientResumePolicy<ByteBuf, ServerSentEvent>() {

                    @Override
                    public Observable<HttpClientResponse<ServerSentEvent>> onError(
                            ServerClientContext<ByteBuf, ServerSentEvent> clientContext,
                            int attempts, Throwable error) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Observable<HttpClientResponse<ServerSentEvent>> onCompleted(
                            ServerClientContext<ByteBuf, ServerSentEvent> clientContext,
                            int attempts) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                })
                .build();
    }
}
