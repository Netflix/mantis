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

package io.mantisrx.runtime.executor;

import java.util.Iterator;
import java.util.List;

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.StageConfig;
import io.reactivex.mantis.remote.observable.ConnectToObservable;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import io.reactivex.mantis.remote.observable.PortSelectorWithinRange;
import io.reactivex.mantis.remote.observable.RemoteObservable;
import io.reactivex.mantis.remote.observable.RemoteRxServer;
import io.reactivex.mantis.remote.observable.RxMetrics;
import junit.framework.Assert;
import org.junit.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;


public class StageExecutorsTest {

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Test
    public void testExecuteSource() {

        TestJob provider = new TestJob();
        Job<Integer> job = provider.getJobInstance();

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        WorkerPublisher producer = new WorkerPublisherRemoteObservable(serverPort, null,
                Observable.just(1), null);
        // execute source
        BehaviorSubject<Integer> workersInStageOneObservable = BehaviorSubject.create(1);
        StageExecutors.executeSource(0, job.getSource(), stages.get(0), producer,
                new Context(), workersInStageOneObservable);

        Iterator<Integer> iter = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .slotId("0")
                .port(serverPort)
                .decoder(Codecs.integer())
                .build())
                .getObservable()
                .toBlocking()
                .getIterator();

        // verify numbers are doubled
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(1, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());
        Assert.assertEquals(9, iter.next().intValue());
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Test
    public void testExecuteIntermediatStage() throws InterruptedException {

        TestJob provider = new TestJob();
        Job<Integer> job = provider.getJobInstance();

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);

        final int publishPort = portSelector.acquirePort();
        final int consumerPort = portSelector.acquirePort();

        // mimic previous stage with a server
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

        WorkerConsumer consumer = new WorkerConsumerRemoteObservable(null, staticEndpoints);
        WorkerPublisher producer = new WorkerPublisherRemoteObservable(publishPort, null,
                Observable.just(1), null);
        // execute intermediate, flatten results
        StageExecutors.executeIntermediate(consumer, stages.get(1), producer,
                new Context());

        Iterator<Integer> iter = RemoteObservable.connect(new ConnectToObservable.Builder<Integer>()
                .host("localhost")
                .slotId("0")
                .port(publishPort)
                .decoder(Codecs.integer())
                .build())
                .getObservable()
                .toBlocking()
                .getIterator();

        // verify numbers are even
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(2, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Test
    public void testExecuteSink() throws InterruptedException {

        TestJob provider = new TestJob();
        Job<Integer> job = provider.getJobInstance();

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);

        final int consumerPort = portSelector.acquirePort();

        // mimic previous stage with a server
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

        Action0 noOpAction = new Action0() {
            @Override
            public void call() {}
        };

        Action1<Throwable> noOpError = new Action1<Throwable>() {
            @Override
            public void call(Throwable t) {}
        };

        WorkerConsumer consumer = new WorkerConsumerRemoteObservable(null, staticEndpoints);
        // execute source
        StageExecutors.executeSink(consumer, stages.get(1), job.getSink(), new TestPortSelector(), new RxMetrics(),
                new Context(),
                noOpAction, null, null, noOpAction, noOpError);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();

        // verify numbers are even
        Assert.assertEquals(0, iter.next().intValue());
        Assert.assertEquals(2, iter.next().intValue());
        Assert.assertEquals(4, iter.next().intValue());
    }


}
