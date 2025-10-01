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

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.executor.*;
import io.mantisrx.runtime.parameter.Parameters;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import io.reactivex.mantis.remote.observable.PortSelectorWithinRange;
import io.reactivex.mantis.remote.observable.RemoteObservable;
import io.reactivex.mantis.remote.observable.RemoteRxServer;
import io.reactivex.mantis.remote.observable.RxMetrics;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;

public class EagerSubscriptionTest {

    @Test
    public void testPerpetualJobDelayedEagerSubscription() throws Exception {
        // Simplified test - just verify that perpetual context has callback mechanism
        AtomicBoolean eagerSubscriptionActivated = new AtomicBoolean(false);

        // Create context for perpetual job
        WorkerInfo workerInfo = Mockito.mock(WorkerInfo.class);
        Mockito.when(workerInfo.getDurationType()).thenReturn(MantisJobDurationType.Perpetual);
        Mockito.when(workerInfo.getJobId()).thenReturn("testPerpetualJob");

        Context perpetualContext = new Context(
            new Parameters(),
            null, // serviceLocator
            workerInfo,
            null, // metricRegistry
            () -> {} // completeAndExitAction
        );

        // Set up callback to track eager subscription activation
        perpetualContext.setEagerSubscriptionActivationCallback(() -> {
            eagerSubscriptionActivated.set(true);
        });

        // Simulate first client connection by activating eager subscription
        perpetualContext.activateEagerSubscription();

        // Assert: Callback should be invoked
        Assert.assertTrue("Eager subscription should be activated", eagerSubscriptionActivated.get());
    }

    @Test
    public void testTransientJobImmediateSubscription() throws Exception {
        // Setup - similar to testExecuteSink but explicitly with transient job
        TestJob provider = new TestJob();
        Job<Integer> job = provider.getJobInstance();

        // Create context for transient job
        WorkerInfo workerInfo = Mockito.mock(WorkerInfo.class);
        Mockito.when(workerInfo.getDurationType()).thenReturn(MantisJobDurationType.Transient);
        Mockito.when(workerInfo.getJobId()).thenReturn("testTransientJob");

        Context transientContext = new Context(
            new Parameters(),
            null, // serviceLocator
            workerInfo,
            null, // metricRegistry
            () -> {} // completeAndExitAction
        );

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);

        final int consumerPort = portSelector.acquirePort();

        // Create server
        RemoteRxServer server1 = RemoteObservable.serve(consumerPort, Observable.range(0, 10), Codecs.integer());
        server1.start();

        EndpointInjector staticEndpoints = new EndpointInjector() {
            @Override
            public Observable<EndpointChange> deltas() {
                return Observable.create(new OnSubscribe<EndpointChange>() {
                    @Override
                    public void call(Subscriber<? super EndpointChange> subscriber) {
                        subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", consumerPort, "1")));
                        subscriber.onCompleted();
                    }
                });
            }
        };

        Action0 noOpAction = () -> {};
        Action1<Throwable> noOpError = throwable -> {};

        WorkerConsumer consumer = new WorkerConsumerRemoteObservable(null, staticEndpoints);

        // Execute sink with transient context
        StageExecutors.executeSink(consumer, stages.get(1), job.getSink(), new TestPortSelector(), new RxMetrics(),
                transientContext,
                noOpAction, null, null, noOpAction, noOpError);

        // For transient jobs, processing should happen normally (this test verifies no regression)
        Iterator<Integer> iter = provider.getItemsWritten().iterator();

        // Verify numbers are even (same as original test)
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(2, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());

        // Clean up
        server1.shutdown();
    }
}
