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

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.StageConfig;
import io.reactivex.mantis.remote.observable.ConnectToGroupedObservable;
import io.reactivex.mantis.remote.observable.EndpointChange;
import io.reactivex.mantis.remote.observable.EndpointInjector;
import io.reactivex.mantis.remote.observable.PortSelectorWithinRange;
import io.reactivex.mantis.remote.observable.RemoteObservable;
import io.reactivex.mantis.remote.observable.RemoteRxServer;
import io.reactivex.mantis.remote.observable.ServeGroupedObservable;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.subjects.BehaviorSubject;


public class StageExecutorsGroupByTest {

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Test
    public void testExecuteSource() {

        TestGroupByJob provider = new TestGroupByJob();
        Job<Pair> job = provider.getJobInstance();

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);
        int serverPort = portSelector.acquirePort();

        WorkerPublisher producer = new WorkerPublisherRemoteObservable(serverPort, null,
                Observable.just(1), null);
        // execute source

        BehaviorSubject<Integer> workersInStageOneObservable = BehaviorSubject.create(1);
        StageExecutors.executeSource(0, job.getSource(), stages.get(0), producer,
                new Context(), workersInStageOneObservable);

        ConnectToGroupedObservable<String, Integer> config = new ConnectToGroupedObservable
                .Builder<String, Integer>()
                .slotId("0")
                .host("localhost")
                .port(serverPort)
                .keyDecoder(Codecs.string())
                .valueDecoder(Codecs.integer())
                .build();

        Iterator<GroupedObservable<String, Integer>> iter =
                RemoteObservable.connect(config).getObservable()
                        .toBlocking()
                        .getIterator();

        Assert.assertTrue(iter.hasNext());
        // verify numbers are grouped by even/odd
        GroupedObservable<String, Integer> even = iter.next(); // even is first due to zero
        Assert.assertEquals("even", even.getKey());
        Iterator<Integer> evenIter = even.toBlocking().getIterator();
        Assert.assertEquals(0, evenIter.next().intValue());
        Assert.assertEquals(2, evenIter.next().intValue());
        Assert.assertEquals(4, evenIter.next().intValue());
        Assert.assertEquals(6, evenIter.next().intValue());

        GroupedObservable<String, Integer> odd = iter.next();
        Assert.assertEquals("odd", odd.getKey());
        Iterator<Integer> oddIter = odd.toBlocking().getIterator();
        Assert.assertEquals(1, oddIter.next().intValue());
        Assert.assertEquals(3, oddIter.next().intValue());
        Assert.assertEquals(5, oddIter.next().intValue());
        Assert.assertEquals(7, oddIter.next().intValue());

        Assert.assertEquals(false, iter.hasNext());  // should only have two groups
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Test
    public void testExecuteIntermediatStage() throws InterruptedException {

        // Note, this test has a timing issue, client starts
        // sending data before server is ready, resulting
        // in a RST (connection reset by peer)

        TestGroupByJob provider = new TestGroupByJob();
        Job<Pair> job = provider.getJobInstance();

        List<StageConfig<?, ?>> stages = job.getStages();
        PortSelectorWithinRange portSelector = new PortSelectorWithinRange(8000, 9000);

        final int publishPort = portSelector.acquirePort();
        final int consumerPort = portSelector.acquirePort();

        Observable<Observable<GroupedObservable<String, Integer>>> go = Observable.just(Observable.range(0, 10)
                .groupBy(new Func1<Integer, String>() {
                    @Override
                    public String call(Integer t1) {
                        if ((t1 % 2) == 0) {
                            return "even";
                        } else {
                            return "odd";
                        }
                    }
                }));

        // mimic previous stage with a server
        ServeGroupedObservable<String, Integer> config = new ServeGroupedObservable.Builder<String, Integer>()
                .keyEncoder(Codecs.string())
                .valueEncoder(Codecs.integer())
                .observable(go)
                .build();
        RemoteRxServer server = new RemoteRxServer.Builder()
                .addObservable(config)
                .port(consumerPort)
                .build();
        server.start();

        EndpointInjector staticEndpoints = new EndpointInjector() {
            @Override
            public Observable<EndpointChange> deltas() {
                return Observable.create(new OnSubscribe<EndpointChange>() {
                    @Override
                    public void call(Subscriber<? super EndpointChange> subscriber) {
                        subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", consumerPort, "0")));
                        subscriber.onNext(new EndpointChange(EndpointChange.Type.add, new Endpoint("localhost", consumerPort, "1")));
                        subscriber.onCompleted();
                    }
                });
            }
        };

        WorkerConsumer consumer = new WorkerConsumerRemoteObservable(null, staticEndpoints);
        WorkerPublisher producer = new WorkerPublisherRemoteObservable(publishPort, null,
                Observable.just(1), null);
        // execute source
        StageExecutors.executeIntermediate(consumer, stages.get(1), producer,
                new Context());

        ConnectToGroupedObservable<String, Integer> connectConfig = new ConnectToGroupedObservable
                .Builder<String, Integer>()
                .host("localhost")
                .port(publishPort)
                .keyDecoder(Codecs.string())
                .valueDecoder(Codecs.integer())
                .build();

        Iterator<GroupedObservable<String, Integer>> iter =
                RemoteObservable.connect(connectConfig).getObservable()
                        .toBlocking()
                        .getIterator();

        // verify numbers are grouped by even/odd
        GroupedObservable<String, Integer> even = iter.next(); // even is first due to zero
        Assert.assertEquals("even", even.getKey());
        Iterator<Integer> evenIter = even.toBlocking().getIterator();
        Assert.assertEquals(0, evenIter.next().intValue());
        Assert.assertEquals(4, evenIter.next().intValue());
        Assert.assertEquals(16, evenIter.next().intValue());
        Assert.assertEquals(36, evenIter.next().intValue());

        GroupedObservable<String, Integer> odd = iter.next();
        Assert.assertEquals("odd", odd.getKey());
        Iterator<Integer> oddIter = odd.toBlocking().getIterator();
        Assert.assertEquals(1, oddIter.next().intValue());
        Assert.assertEquals(9, oddIter.next().intValue());
        Assert.assertEquals(25, oddIter.next().intValue());
        Assert.assertEquals(49, oddIter.next().intValue());

        Assert.assertEquals(false, iter.hasNext());  // should only have two groups
    }
}
