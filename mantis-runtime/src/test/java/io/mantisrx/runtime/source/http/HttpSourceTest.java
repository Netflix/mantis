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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata.Builder;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.sink.ServerSentEventsSink;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.runtime.source.http.impl.HttpClientFactories;
import io.mantisrx.runtime.source.http.impl.HttpRequestFactories;
import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent.EventType;
import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;


public class HttpSourceTest {

    private final static int SEED_PORT = 4000;
    private final static int PORT_RANGE = 1000;
    private static LocalServerProvider localServerProvider;
    private static EventType[] expectedEvents = new EventType[] {CONNECTION_ATTEMPTED, SUBSCRIPTION_ESTABLISHED, CONNECTION_UNSUBSCRIBED, CONNECTION_ESTABLISHED, SERVER_FOUND, SOURCE_COMPLETED, SUBSCRIPTION_ENDED};
    private static Set<EventType> EXPECTED_EVENTS_SETS = new HashSet<>(Arrays.asList(expectedEvents));

    // Just make sure the unused port is out side the range of possible ports: [SEED_PORT, SEED_PORT + PORT_RANGE)
    static {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.addAppender(new ConsoleAppender(
                new PatternLayout("%-6r [%p] %c - %m%n")));
    }

    private TestSourceObserver sourceObserver = new TestSourceObserver();

    @BeforeClass
    public static void init() {
        int portStart = new Random().nextInt(PORT_RANGE) + SEED_PORT;

        localServerProvider = new LocalServerProvider();
        localServerProvider.start(3, portStart);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        localServerProvider.shutDown();
    }

    @Before
    public void setup() {
        sourceObserver = new TestSourceObserver();
    }

    @Test
    public void canStreamFromMultipleServers() throws Exception {
        HttpSource<ServerSentEvent, ServerSentEvent> source = HttpSources
                .source(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createGetFactory("test/stream"))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        sourcingStream(source);
    }

    private void sourceEchoStreamFromPost(HttpSource<ServerSentEvent, ServerSentEvent> source, String postContent) throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .doOnNext(new Action1<ServerSentEvent>() {
                    @Override
                    public void call(ServerSentEvent event) {
                        counter.incrementAndGet();

                        String msg = event.contentAsString();
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

        long waitSeconds = 30000;
        boolean timedout = !done.await(waitSeconds, TimeUnit.SECONDS);
        if (timedout) {
            fail(String.format("Waited at least %d seconds for the test to finish. Something is wrong", waitSeconds));
        }

        Assert.assertEquals(
                String.format("%d servers => the result has %d times of a single echo",
                        localServerProvider.serverSize(),
                        localServerProvider.serverSize()),
                localServerProvider.serverSize(),
                counter.get());

        assertEquals(String.format("%d servers => %d identical copies per message",
                localServerProvider.serverSize(),
                localServerProvider.serverSize()),
                localServerProvider.serverSize(),
                result.get(postContent).get());

        for (ServerInfo server : localServerProvider.getServerInfos()) {
            assertEquals("There should be one completion per server", 1, sourceObserver.getCount(server, EventType.SOURCE_COMPLETED));
            assertEquals("There should be one un-subscription per server", 1, sourceObserver.getCount(server, EventType.CONNECTION_UNSUBSCRIBED));
            assertEquals("There should be no error", 0, sourceObserver.getCount(server, EventType.SUBSCRIPTION_FAILED));
            assertEquals("There should be one connection per server", 1, sourceObserver.getCount(server, EventType.CONNECTION_ESTABLISHED));
        }

        assertEquals(1, sourceObserver.getCompletionCount());
        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertEquals(EXPECTED_EVENTS_SETS, events);

        for (EventType event : events) {
            assertEquals("Each event should be recorded exactly once per server", localServerProvider.serverSize(), sourceObserver.getEventCount(event));
        }
    }

    private void sourcingStream(HttpSource<ServerSentEvent, ServerSentEvent> source) throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        Observable.merge(source.call(new Context(), new Index(1, 1)))
                .doOnNext(new Action1<ServerSentEvent>() {
                    @Override
                    public void call(ServerSentEvent event) {
                        counter.incrementAndGet();

                        String msg = event.contentAsString();
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

        long waitSeconds = 30000;
        boolean timedout = !done.await(waitSeconds, TimeUnit.SECONDS);
        if (timedout) {
            fail(String.format("Waited at least %d seconds for the test to finish. Something is wrong", waitSeconds));
        }

        Assert.assertEquals(
                String.format("%d servers => the result has %d times of a single stream",
                        localServerProvider.serverSize(),
                        localServerProvider.serverSize()),
                counter.get(),
                RequestProcessor.smallStreamContent.size() * localServerProvider.serverSize());

        for (String data : RequestProcessor.smallStreamContent) {
            assertEquals(String.format("%d servers => %d identical copies per message", localServerProvider.serverSize(), localServerProvider.serverSize()), localServerProvider.serverSize(), result.get(data).get());
        }

        for (ServerInfo server : localServerProvider.getServerInfos()) {
            assertEquals("There should be one completion per server", 1, sourceObserver.getCount(server, EventType.SOURCE_COMPLETED));
            assertEquals("There should be one un-subscription per server", 1, sourceObserver.getCount(server, EventType.CONNECTION_UNSUBSCRIBED));
            assertEquals("There should be no error", 0, sourceObserver.getCount(server, EventType.SUBSCRIPTION_FAILED));
            assertEquals("There should be one connection per server", 1, sourceObserver.getCount(server, EventType.CONNECTION_ESTABLISHED));
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
    public void canStreamFromPostingToMultipleServers() throws Exception {
        HttpSource<ServerSentEvent, ServerSentEvent> source = HttpSources
                .source(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createPostFactory("test/postStream"))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        sourcingStream(source);
    }

    @Test
    public void canStreamFromPostingWithContentToMultipleServers() throws Exception {
        String postContent = "test/postcontent";
        HttpSource<ServerSentEvent, ServerSentEvent> source = HttpSources
                .source(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createPostFactory("test/postContent", postContent.getBytes()))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        sourceEchoStreamFromPost(source, postContent);
    }

    @Test
    public void pollingSourceWillWork() throws Exception {
        ServerInfo server = localServerProvider.getServerInfos().get(0);
        HttpSource<ByteBuf, String> source = HttpSources
                .pollingSource(server.getHost(), server.getPort(), "test/singleEntity")
                .withActivityObserver(sourceObserver)
                .build();


        final AtomicInteger counter = new AtomicInteger();
        final int maxRepeat = 10;
        final CountDownLatch done = new CountDownLatch(maxRepeat);
        final ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<>();
        Subscription subscription =
                Observable.merge(source.call(new Context(), new Index(1, 1)))
                        .doOnNext(new Action1<String>() {
                            @Override
                            public void call(String content) {
                                assertEquals(RequestProcessor.SINGLE_ENTITY_RESPONSE, content);
                                counter.incrementAndGet();
                                result.putIfAbsent(content, new AtomicInteger());
                                result.get(content).incrementAndGet();

                                done.countDown();
                            }
                        })
                        .doOnError(new Action1<Throwable>() {
                            @Override
                            public void call(Throwable throwable) {
                                fail("Unexpected failure: " + throwable);
                            }
                        })
                        .subscribe();

        long waitSeconds = 3;
        boolean timedout = !done.await(waitSeconds, TimeUnit.SECONDS);
        if (timedout) {
            fail(String.format("Waited at least %d seconds for the test to finish. Something is wrong", waitSeconds));
        }

        Assert.assertEquals(String.format("%d servers => the result has %d times of a single stream", localServerProvider.serverSize(), localServerProvider.serverSize()), counter.get(), maxRepeat);

        assertTrue(
                String.format("There should be at least %d completions after %d repeats (The last one may not have completion. Actual completion count: %d",
                        maxRepeat - 1,
                        maxRepeat,
                        sourceObserver.getCount(server, EventType.SOURCE_COMPLETED)),
                maxRepeat - 1 <= sourceObserver.getCount(server, EventType.SOURCE_COMPLETED));

        assertEquals("There should be no error", 0, sourceObserver.getCount(server, EventType.SUBSCRIPTION_FAILED));
        assertEquals("There should be " + maxRepeat + " connection establishment in total", maxRepeat, sourceObserver.getCount(server, EventType.CONNECTION_ESTABLISHED));

        assertEquals("There should no final completion", 0, sourceObserver.getCompletionCount());
        assertEquals(0, sourceObserver.getErrorCount());

        Set<EventType> events = sourceObserver.getEvents();
        assertTrue("Polling Source always has subscriptions, so there won't be subscription_ended event. But other events should all be there", EXPECTED_EVENTS_SETS.containsAll(events));
    }

    @Test
    public void testResubscribeShouldAlwaysWork() throws Exception {
        HttpSource<ServerSentEvent, ServerSentEvent> source = HttpSources
                .source(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createGetFactory("test/stream"))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        int totalCount = 5;
        final CountDownLatch latch = new CountDownLatch(totalCount);
        Observable<ServerSentEvent> stream = Observable.merge(source.call(new Context(), new Index(1, 1)));
        Subscription sub = stream.subscribe(new Action1<ServerSentEvent>() {
            @Override
            public void call(ServerSentEvent event) {
                latch.countDown();
            }
        });

        long waitSeconds = 10;
        boolean countedDown = latch.await(waitSeconds, TimeUnit.SECONDS);
        if (!countedDown) {
            fail(String.format("Waited too long to receive %d events within %d seconds. Total counted: %d", totalCount, waitSeconds, latch.getCount()));
        }

        sub.unsubscribe();

        final CountDownLatch newLatch = new CountDownLatch(totalCount);
        sub = stream.subscribe(new Action1<ServerSentEvent>() {
            @Override
            public void call(ServerSentEvent event) {
                newLatch.countDown();
            }
        });

        countedDown = newLatch.await(5, TimeUnit.SECONDS);
        if (!countedDown) {
            fail("Waited too long to receive enough events. Counted: " + latch.getCount());
        }
        sub.unsubscribe();

    }

    /**
     * TODO: To test the resubscription, we need to run curl http://localhost:port. When we see stream of lines,
     * terminate the curl connection, and then rerun curl. The second curl command should get stuck. This
     * test is not automated yet because running an HTTP client (including a raw URLConnection) in the test
     * can't reproduce the test for some reason.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void testWithJobExecutionWillWorkForResubscription() throws Exception {
        final HttpSource<ServerSentEvent, ServerSentEvent> source = HttpSources
                .source(
                        HttpClientFactories.sseClientFactory(),
                        HttpRequestFactories.createGetFactory("test/infStream"))
                .withServerProvider(localServerProvider)
                .withActivityObserver(sourceObserver)
                .build();

        final ServerSentEventsSink<String> sink = new ServerSentEventsSink<>(
                new Func1<String, String>() {
                    @Override
                    public String call(String o) {
                        return o;
                    }
                });

        LocalJobExecutorNetworked.execute(new HttpEchoJob(source, sink).getJobInstance());

    }

    @Test
    @Ignore
    public void testDummySource() throws Exception {
        Source<String> dummySource = new Source<String>() {
            @Override
            public Observable<Observable<String>> call(Context context, Index index) {
                return Observable.just(Observable.interval(1, TimeUnit.SECONDS)
                        .map(new Func1<Long, String>() {
                            @Override
                            public String call(Long aLong) {
                                return aLong.toString();
                            }
                        }));
            }
        };

        LocalJobExecutorNetworked.execute(new DummyEchoJob(dummySource).getJobInstance());
    }

    public static class DummyEchoJob extends MantisJobProvider<String> {

        private final Source<String> source;

        public DummyEchoJob(Source<String> source) {

            this.source = source;
        }

        @Override
        public Job<String> getJobInstance() {
            return MantisJob
                    .source(source)
                    .stage(
                            new ScalarComputation<String, String>() {
                                @Override
                                public Observable<String> call(Context context, Observable<String> stream) {
                                    return stream.map(new Func1<String, String>() {
                                        @Override
                                        public String call(String event) {
                                            return "echoed: " + event;
                                        }
                                    });

                                }
                            },
                            new ScalarToScalar.Config<String, String>()
                                    .codec(Codecs.string())
                                    .description("Just a test config"))
                    .sink(new ServerSentEventsSink<>(
                            new Func1<String, String>() {
                                @Override
                                public String call(String o) {
                                    return o;
                                }
                            }))
                    .metadata(new Builder()
                            .description("Counts frequency of words as they are observed.")
                            .build())
                    .create();

        }
    }

    public static class HttpEchoJob extends MantisJobProvider<String> {

        private final HttpSource<ServerSentEvent, ServerSentEvent> source;
        private final ServerSentEventsSink<String> sink;

        public HttpEchoJob(HttpSource<ServerSentEvent, ServerSentEvent> source, ServerSentEventsSink<String> sink) {
            this.source = source;
            this.sink = sink;
        }

        public int getSinkPort() {
            return sink.getServerPort();
        }

        @Override
        public Job<String> getJobInstance() {
            return MantisJob
                    .source(source)
                    .stage(
                            new ScalarComputation<ServerSentEvent, String>() {
                                @Override
                                public Observable<String> call(Context context, Observable<ServerSentEvent> stream) {
                                    return stream.map(new Func1<ServerSentEvent, String>() {
                                        @Override
                                        public String call(ServerSentEvent event) {
                                            return event.contentAsString();
                                        }
                                    });

                                }
                            },
                            new ScalarToScalar.Config<ServerSentEvent, String>()
                                    .codec(Codecs.string())
                                    .description("Just a test config")
                    )
                    .sink(sink)
                    .metadata(new Builder()
                            .description("Counts frequency of words as they are observed.")
                            .build())
                    .create();

        }
    }
}
