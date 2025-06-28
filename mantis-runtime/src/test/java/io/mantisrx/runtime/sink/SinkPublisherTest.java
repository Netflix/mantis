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

package io.mantisrx.runtime.sink;

import io.mantisrx.common.SystemParameters;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.SinkHolder;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.executor.PortSelector;
import io.mantisrx.runtime.executor.SinkPublisher;
import io.mantisrx.runtime.parameter.Parameters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;

public class SinkPublisherTest {

    @Test
    public void testDelayedEagerSubscriptionForPerpetualJob() throws Exception {
        // Setup
        AtomicBoolean sinkInitialized = new AtomicBoolean(false);
        AtomicBoolean sinkCalled = new AtomicBoolean(false);
        AtomicInteger subscriptionCount = new AtomicInteger(0);
        CountDownLatch subscriptionLatch = new CountDownLatch(1);

        // Mock sink that tracks initialization and calls
        Sink<String> mockSink = new Sink<String>() {
            @Override
            public void init(Context context) {
                sinkInitialized.set(true);
            }

            @Override
            public void call(Context context, PortRequest portRequest, Observable<String> observable) {
                sinkCalled.set(true);
                // Don't subscribe immediately - this simulates SSE sink behavior
            }
        };

        SinkHolder<String> sinkHolder = new SinkHolder<>(mockSink);

        // Mock context for perpetual job
        Context context = createPerpetualJobContext();

        // Mock port selector
        PortSelector portSelector = Mockito.mock(PortSelector.class);
        Mockito.when(portSelector.acquirePort()).thenReturn(8080);

        // Create observable that tracks subscriptions
        Observable<String> testObservable = Observable.create(subscriber -> {
            subscriptionCount.incrementAndGet();
            subscriptionLatch.countDown();
            subscriber.onNext("test");
            subscriber.onCompleted();
        });

        Observable<Observable<String>> observablesToPublish = Observable.just(testObservable);

        // Create SinkPublisher
        SinkPublisher<String> sinkPublisher = new SinkPublisher<>(
            sinkHolder,
            portSelector,
            context,
            () -> {}, // observableTerminatedCallback
            () -> {}, // onSubscribeAction
            () -> {}, // onUnsubscribeAction
            () -> {}, // observableOnCompleteCallback
            throwable -> {} // observableOnErrorCallback
        );

        // Create mock stage config
        StageConfig<String, String> stageConfig = Mockito.mock(StageConfig.class);

        // Act - start the SinkPublisher
        sinkPublisher.start(stageConfig, observablesToPublish);

        // Assert - For perpetual jobs, eager subscription should be delayed
        Assert.assertTrue("Sink should be initialized", sinkInitialized.get());
        Assert.assertTrue("Sink should be called", sinkCalled.get());

        // Verify no subscription has happened yet (delayed eager subscription)
        Thread.sleep(100); // Give time for any potential subscription
        Assert.assertEquals("No subscription should occur for perpetual job before activation", 0, subscriptionCount.get());

        // Act - Simulate first client connection by activating eager subscription
        context.activateEagerSubscription();

        // Assert - Now subscription should happen
        Assert.assertTrue("Subscription should occur after activation",
                         subscriptionLatch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals("Exactly one subscription should occur", 1, subscriptionCount.get());

        // Cleanup
        sinkPublisher.close();
    }

    @Test
    public void testNoDelayedSubscriptionForTransientJob() throws Exception {
        // Setup
        AtomicInteger subscriptionCount = new AtomicInteger(0);
        CountDownLatch subscriptionLatch = new CountDownLatch(1);

        // Mock sink
        Sink<String> mockSink = new Sink<String>() {
            @Override
            public void init(Context context) {}

            @Override
            public void call(Context context, PortRequest portRequest, Observable<String> observable) {
                // Transient jobs should subscribe immediately
                observable.subscribe();
            }
        };

        SinkHolder<String> sinkHolder = new SinkHolder<>(mockSink);

        // Mock context for transient job
        Context context = createTransientJobContext();

        // Mock port selector
        PortSelector portSelector = Mockito.mock(PortSelector.class);

        // Create observable that tracks subscriptions
        Observable<String> testObservable = Observable.create(subscriber -> {
            subscriptionCount.incrementAndGet();
            subscriptionLatch.countDown();
            subscriber.onNext("test");
            subscriber.onCompleted();
        });

        Observable<Observable<String>> observablesToPublish = Observable.just(testObservable);

        // Create SinkPublisher
        SinkPublisher<String> sinkPublisher = new SinkPublisher<>(
            sinkHolder,
            portSelector,
            context,
            () -> {}, // observableTerminatedCallback
            () -> {}, // onSubscribeAction
            () -> {}, // onUnsubscribeAction
            () -> {}, // observableOnCompleteCallback
            throwable -> {} // observableOnErrorCallback
        );

        // Create mock stage config
        StageConfig<String, String> stageConfig = Mockito.mock(StageConfig.class);

        // Act - start the SinkPublisher
        sinkPublisher.start(stageConfig, observablesToPublish);

        // Assert - For transient jobs, subscription should happen immediately through sink
        Assert.assertTrue("Subscription should occur immediately for transient job",
                         subscriptionLatch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals("Exactly one subscription should occur", 1, subscriptionCount.get());

        // Cleanup
        sinkPublisher.close();
    }

    @Test
    public void testPerpetualJobImmediateStrategy() throws Exception {
        // Setup
        AtomicInteger subscriptionCount = new AtomicInteger(0);
        CountDownLatch subscriptionLatch = new CountDownLatch(1);

        // Mock sink
        Sink<String> mockSink = new Sink<String>() {
            @Override
            public void init(Context context) {}

            @Override
            public void call(Context context, PortRequest portRequest, Observable<String> observable) {}
        };

        SinkHolder<String> sinkHolder = new SinkHolder<>(mockSink);

        // Mock context for perpetual job with IMMEDIATE strategy
        Context context = createPerpetualJobContextWithStrategy("IMMEDIATE");

        // Mock port selector
        PortSelector portSelector = Mockito.mock(PortSelector.class);

        // Create observable that tracks subscriptions
        Observable<String> testObservable = Observable.create(subscriber -> {
            subscriptionCount.incrementAndGet();
            subscriptionLatch.countDown();
            subscriber.onNext("test");
            subscriber.onCompleted();
        });

        Observable<Observable<String>> observablesToPublish = Observable.just(testObservable);

        // Create SinkPublisher
        SinkPublisher<String> sinkPublisher = new SinkPublisher<>(
            sinkHolder,
            portSelector,
            context,
            () -> {}, () -> {}, () -> {}, () -> {}, throwable -> {}
        );

        StageConfig<String, String> stageConfig = Mockito.mock(StageConfig.class);

        // Act
        sinkPublisher.start(stageConfig, observablesToPublish);

        // Assert - For IMMEDIATE strategy, subscription should happen immediately
        Assert.assertTrue("Subscription should occur immediately for IMMEDIATE strategy",
                         subscriptionLatch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals("Exactly one subscription should occur", 1, subscriptionCount.get());

        // Cleanup
        sinkPublisher.close();
    }

    @Test
    public void testActivateEagerSubscriptionOnlyOnce() throws Exception {
        // Setup
        AtomicInteger subscriptionCount = new AtomicInteger(0);
        CountDownLatch firstSubscriptionLatch = new CountDownLatch(1);

        // Mock sink
        Sink<String> mockSink = new Sink<String>() {
            @Override
            public void init(Context context) {}

            @Override
            public void call(Context context, PortRequest portRequest, Observable<String> observable) {}
        };

        SinkHolder<String> sinkHolder = new SinkHolder<>(mockSink);
        Context context = createPerpetualJobContext();
        PortSelector portSelector = Mockito.mock(PortSelector.class);

        // Create observable that tracks subscriptions
        Observable<String> testObservable = Observable.create(subscriber -> {
            subscriptionCount.incrementAndGet();
            if (subscriptionCount.get() == 1) {
                firstSubscriptionLatch.countDown();
            }
            subscriber.onNext("test");
            subscriber.onCompleted();
        });

        Observable<Observable<String>> observablesToPublish = Observable.just(testObservable);

        // Create SinkPublisher
        SinkPublisher<String> sinkPublisher = new SinkPublisher<>(
            sinkHolder,
            portSelector,
            context,
            () -> {}, () -> {}, () -> {}, () -> {}, throwable -> {}
        );

        StageConfig<String, String> stageConfig = Mockito.mock(StageConfig.class);

        // Act
        sinkPublisher.start(stageConfig, observablesToPublish);

        // Activate eager subscription multiple times
        context.activateEagerSubscription();
        context.activateEagerSubscription();
        context.activateEagerSubscription();

        // Assert
        Assert.assertTrue("First subscription should occur",
                         firstSubscriptionLatch.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give time for any additional subscriptions
        Assert.assertEquals("Only one subscription should occur despite multiple activations",
                           1, subscriptionCount.get());

        // Cleanup
        sinkPublisher.close();
    }

    @Test
    public void testCallbackRegistrationInContext() {
        // Setup
        Context context = createPerpetualJobContext();
        AtomicBoolean callbackCalled = new AtomicBoolean(false);

        // Act - Set callback
        context.setEagerSubscriptionActivationCallback(() -> callbackCalled.set(true));

        // Activate the callback
        context.activateEagerSubscription();

        // Assert
        Assert.assertTrue("Callback should be called when activateEagerSubscription is invoked",
                         callbackCalled.get());
    }

    @Test
    public void testNoCallbackForTransientJob() {
        // Setup
        Context context = createTransientJobContext();

        // Act - Try to activate (should be no-op for transient jobs)
        context.activateEagerSubscription();

        // Assert - No exception should be thrown, method should handle null callback gracefully
        // This test passes if no exception is thrown
    }

    // Helper methods
    private Context createPerpetualJobContext() {
        return createPerpetualJobContextWithStrategy("ON_FIRST_CLIENT");
    }

    private Context createPerpetualJobContextWithStrategy(String strategy) {
        WorkerInfo workerInfo = Mockito.mock(WorkerInfo.class);
        Mockito.when(workerInfo.getDurationType()).thenReturn(MantisJobDurationType.Perpetual);
        Mockito.when(workerInfo.getJobId()).thenReturn("testJob");

        // Create parameters with the strategy
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put(SystemParameters.JOB_WORKER_EAGER_SUBSCRIPTION_STRATEGY, strategy);
        Set<String> paramDefs = new HashSet<>();
        paramDefs.add(SystemParameters.JOB_WORKER_EAGER_SUBSCRIPTION_STRATEGY);
        Parameters parameters = new Parameters(paramMap, new HashSet<>(), paramDefs);

        return new Context(
            parameters,
            null, // serviceLocator
            workerInfo,
            null, // metricRegistry
            () -> {} // completeAndExitAction
        );
    }

    private Context createTransientJobContext() {
        WorkerInfo workerInfo = Mockito.mock(WorkerInfo.class);
        Mockito.when(workerInfo.getDurationType()).thenReturn(MantisJobDurationType.Transient);
        Mockito.when(workerInfo.getJobId()).thenReturn("testJob");

        return new Context(
            new Parameters(),
            null, // serviceLocator
            workerInfo,
            null, // metricRegistry
            () -> {} // completeAndExitAction
        );
    }
}
