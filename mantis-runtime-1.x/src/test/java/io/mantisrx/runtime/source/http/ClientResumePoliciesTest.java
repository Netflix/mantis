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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.mantisrx.runtime.source.http.impl.HttpSourceImpl.HttpSourceEvent;
import io.mantisrx.runtime.source.http.impl.ServerClientContext;
import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func0;


public class ClientResumePoliciesTest {

    public static final String RESPONSE_CONTENT = "value";
    private Observer<HttpSourceEvent> observer;
    private HttpClient<String, String> client;
    private HttpRequestFactory<String> factory;
    private HttpClientResponse<String> response;
    private HttpClientRequest<String> request;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        observer = new TestSourceObserver();
        client = mock(HttpClient.class);
        factory = mock(HttpRequestFactory.class);
        response = mock(HttpClientResponse.class);
        request = mock(HttpClientRequest.class);

        when(factory.create()).thenReturn(request);
        when(response.getContent()).thenReturn(Observable.just(RESPONSE_CONTENT));
        when(client.submit(any(HttpClientRequest.class))).thenReturn(Observable.just(response));
    }

    @Test
    public void testMaxRepeatOnCompletionAndError() throws Exception {
        int max = 10;
        ClientResumePolicy<String, String> policy = ClientResumePolicies.maxRepeat(max);

        ServerClientContext<String, String> context = new ServerClientContext<>(new ServerInfo("localhost", 1000), client, factory, observer);
        for (int i = 0; i < 20; ++i) {
            Observable<HttpClientResponse<String>> resumedOnCompleted = policy.onCompleted(context, i);
            Observable<HttpClientResponse<String>> resumedOnError = policy.onError(context, i, new Throwable("error"));
            if (i <= max) {
                assertNotNull(resumedOnCompleted);
                assertEquals(RESPONSE_CONTENT, resumedOnCompleted.toBlocking().first().getContent().toBlocking().first());
                assertNotNull(resumedOnError);
                assertEquals(RESPONSE_CONTENT, resumedOnError.toBlocking().first().getContent().toBlocking().first());
            } else {
                assertNull("The resumed on completion should be null as max repeat is passed", resumedOnCompleted);
                assertNull("The resumed on error should be null as max repeat is passed", resumedOnError);
            }
        }

    }

    @Test
    public void testMaxCombinator() throws Exception {
        final AtomicLong start = new AtomicLong();
        final AtomicLong end = new AtomicLong();

        final long delay = 100;
        final int repeat = 20;
        final CountDownLatch done = new CountDownLatch(repeat);

        ClientResumePolicy<String, String> delayedPolicy = ClientResumePolicies.delayed(new Func0<Long>() {
            @Override
            public Long call() {
                return delay;
            }
        }, TimeUnit.MILLISECONDS);

        ClientResumePolicy<String, String> policy = ClientResumePolicies.maxRepeat(delayedPolicy, repeat);

        ServerClientContext<String, String> context = new ServerClientContext<>(new ServerInfo("localhost", 1000), client, factory, observer);
        start.set(System.currentTimeMillis());
        end.set(0);

        for (int i = 0; i < repeat; ++i) {
            Observable<HttpClientResponse<String>> resumedOnCompleted = policy.onCompleted(context, i);
            Observable<HttpClientResponse<String>> resumedOnError = policy.onError(context, i, new Throwable("error"));
            if (i <= repeat) {
                assertNotNull(resumedOnCompleted);
                assertNotNull(resumedOnError);
            } else {
                assertNull("The resumed on completion should be null as max repeat is passed", resumedOnCompleted);
                assertNull("The resumed on error should be null as max repeat is passed", resumedOnError);
            }

            resumedOnCompleted.subscribe(new Subscriber<HttpClientResponse<String>>() {
                @Override
                public void onCompleted() {
                    end.getAndAdd(System.currentTimeMillis() - start.get());
                    done.countDown();
                }

                @Override
                public void onError(Throwable e) {

                }

                @Override
                public void onNext(HttpClientResponse<String> stringHttpClientResponse) {

                }
            });
        }

        long wait = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail("It should take far less than " + wait + " seconds to run the test. ");
        }

        long elapsed = end.get();
        long maxDelay = delay + delay / 2;
        assertTrue(String.format("The delay should be more than %d milliseconds, but no more than %d milliseconds. The actual: %d", repeat * delay, repeat * maxDelay, elapsed), elapsed >= repeat * delay && elapsed <= repeat * maxDelay);
    }

    @Test
    public void testDelayedOnCompletion() throws Exception {
        final AtomicLong start = new AtomicLong();
        final AtomicLong end = new AtomicLong();
        final long delay = 100;
        final int repeat = 20;
        final CountDownLatch done = new CountDownLatch(repeat);

        ClientResumePolicy<String, String> policy = ClientResumePolicies.delayed(new Func0<Long>() {
            @Override
            public Long call() {
                return delay;
            }
        }, TimeUnit.MILLISECONDS);

        ServerClientContext<String, String> context = new ServerClientContext<>(new ServerInfo("localhost", 1000), client, factory, observer);

        start.set(System.currentTimeMillis());
        end.set(0);
        for (int i = 0; i < repeat; ++i) {
            Observable<HttpClientResponse<String>> resumedOnCompleted = policy.onCompleted(context, i);
            resumedOnCompleted.subscribe(new Subscriber<HttpClientResponse<String>>() {
                @Override
                public void onCompleted() {
                    end.getAndAdd(System.currentTimeMillis() - start.get());
                    done.countDown();
                }

                @Override
                public void onError(Throwable e) {

                }

                @Override
                public void onNext(HttpClientResponse<String> stringHttpClientResponse) {

                }
            });
        }

        long wait = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail("It should take far less than " + wait + " seconds to run the test. ");
        }

        long elapsed = end.get();
        long maxDelay = delay + delay / 2;
        assertTrue(String.format("The delay should be more than %d milliseconds, but no more than %d milliseconds. The actual: %d", repeat * delay, repeat * maxDelay, elapsed), elapsed >= repeat * delay && elapsed <= repeat * maxDelay);
    }

    @Test
    public void testDelayedOnError() throws Exception {
        final AtomicLong start = new AtomicLong();
        final AtomicLong end = new AtomicLong();
        final long delay = 100;
        final int repeat = 20;
        final CountDownLatch done = new CountDownLatch(repeat);

        ClientResumePolicy<String, String> policy = ClientResumePolicies.delayed(new Func0<Long>() {
            @Override
            public Long call() {
                return delay;
            }
        }, TimeUnit.MILLISECONDS);

        ServerClientContext<String, String> context = new ServerClientContext<>(new ServerInfo("localhost", 1000), client, factory, observer);

        start.set(System.currentTimeMillis());
        end.set(0);
        for (int i = 0; i < repeat; ++i) {
            Observable<HttpClientResponse<String>> resumedOnCompleted = policy.onError(context, i, new Throwable("error"));
            resumedOnCompleted.subscribe(new Subscriber<HttpClientResponse<String>>() {
                @Override
                public void onCompleted() {
                    end.getAndAdd(System.currentTimeMillis() - start.get());
                    done.countDown();
                }

                @Override
                public void onError(Throwable e) {

                }

                @Override
                public void onNext(HttpClientResponse<String> stringHttpClientResponse) {

                }
            });
        }

        long wait = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail("It should take far less than " + wait + " seconds to run the test. ");
        }

        long elapsed = end.get();
        long maxDelay = delay + delay / 2;
        assertTrue(String.format("The delay should be more than %d millionseconds, but no more than %d millionseconds. The actual: %d", repeat * delay, repeat * maxDelay, elapsed), elapsed >= repeat * delay && elapsed <= repeat * maxDelay);
    }
}
