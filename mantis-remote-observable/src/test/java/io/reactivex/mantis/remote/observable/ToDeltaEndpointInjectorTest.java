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

package io.reactivex.mantis.remote.observable;

import io.mantisrx.common.network.Endpoint;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import rx.observables.BlockingObservable;
import rx.subjects.ReplaySubject;


public class ToDeltaEndpointInjectorTest {

    @Test
    public void deltaTest() {

        ReplaySubject<List<Endpoint>> subject = ReplaySubject.create();
        ToDeltaEndpointInjector service = new ToDeltaEndpointInjector(subject);

        // 1 add endpoints
        List<Endpoint> endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234));
        endpoints.add(new Endpoint("localhost", 2468));
        subject.onNext(endpoints);
        // 2 remove endpoint by leaving out second endpoint
        endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234));
        subject.onNext(endpoints);
        // 3 remove other
        endpoints = new LinkedList<Endpoint>();
        subject.onNext(endpoints);
        // 4 add back
        endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234));
        endpoints.add(new Endpoint("localhost", 2468));
        subject.onNext(endpoints);
        subject.onCompleted();

        BlockingObservable<EndpointChange> be = service.deltas().toBlocking();

        // check for two adds
        Iterator<EndpointChange> iter = be.getIterator();
        Assertions.assertTrue(iter.hasNext());
        EndpointChange ce1 = iter.next();
        Assertions.assertEquals(ce1.getEndpoint().getSlotId(), "localhost:1234");
        Assertions.assertEquals(ce1.getType(), EndpointChange.Type.add);
        EndpointChange ce2 = iter.next();
        Assertions.assertEquals(ce2.getEndpoint().getSlotId(), "localhost:2468");
        Assertions.assertEquals(ce2.getType(), EndpointChange.Type.add);

        // check for complete
        EndpointChange ce3 = iter.next();
        Assertions.assertEquals(ce3.getEndpoint().getSlotId(), "localhost:2468");
        Assertions.assertEquals(ce3.getType(), EndpointChange.Type.complete);

        // check for complete
        EndpointChange ce4 = iter.next();
        Assertions.assertEquals(ce4.getEndpoint().getSlotId(), "localhost:1234");
        Assertions.assertEquals(ce4.getType(), EndpointChange.Type.complete);

        // check for add
        EndpointChange ce5 = iter.next();
        Assertions.assertEquals(ce5.getEndpoint().getSlotId(), "localhost:1234");
        Assertions.assertEquals(ce5.getType(), EndpointChange.Type.add);
        EndpointChange ce6 = iter.next();
        Assertions.assertEquals(ce6.getEndpoint().getSlotId(), "localhost:2468");
        Assertions.assertEquals(ce6.getType(), EndpointChange.Type.add);
    }

    @Test
    public void deltaTestWithIds() {

        ReplaySubject<List<Endpoint>> subject = ReplaySubject.create();
        ToDeltaEndpointInjector service = new ToDeltaEndpointInjector(subject);

        // 1. add endpoints
        List<Endpoint> endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234, "abc"));
        endpoints.add(new Endpoint("localhost", 2468, "xyz"));
        subject.onNext(endpoints);
        // 2. nothing changes
        endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234, "abc"));
        endpoints.add(new Endpoint("localhost", 2468, "xyz"));
        subject.onNext(endpoints);
        // 3. remove endpoint by leaving out second endpoint
        endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234, "abc"));
        subject.onNext(endpoints);
        // 4. remove all
        endpoints = new LinkedList<Endpoint>();
        subject.onNext(endpoints);
        // 5. add back
        endpoints = new LinkedList<Endpoint>();
        endpoints.add(new Endpoint("localhost", 1234, "abc"));
        endpoints.add(new Endpoint("localhost", 2468, "xyz"));
        subject.onNext(endpoints);
        subject.onCompleted();

        BlockingObservable<EndpointChange> be = service.deltas().toBlocking();

        Iterator<EndpointChange> iter = be.getIterator();
        Assertions.assertTrue(iter.hasNext());

        EndpointChange ce2 = iter.next();
        Assertions.assertEquals(ce2.getEndpoint().getSlotId(), "xyz");
        Assertions.assertEquals(ce2.getType(), EndpointChange.Type.add);

        EndpointChange ce1 = iter.next();
        Assertions.assertEquals(ce1.getEndpoint().getSlotId(), "abc");
        Assertions.assertEquals(ce1.getType(), EndpointChange.Type.add);


        EndpointChange ce3 = iter.next();
        Assertions.assertEquals(ce3.getEndpoint().getSlotId(), "xyz");
        Assertions.assertEquals(ce3.getType(), EndpointChange.Type.complete);


        EndpointChange ce4 = iter.next();
        Assertions.assertEquals(ce4.getEndpoint().getSlotId(), "abc");
        Assertions.assertEquals(ce4.getType(), EndpointChange.Type.complete);

        EndpointChange ce6 = iter.next();
        Assertions.assertEquals(ce6.getEndpoint().getSlotId(), "xyz");
        Assertions.assertEquals(ce6.getType(), EndpointChange.Type.add);

        EndpointChange ce5 = iter.next();
        Assertions.assertEquals(ce5.getEndpoint().getSlotId(), "abc");
        Assertions.assertEquals(ce5.getType(), EndpointChange.Type.add);

    }
}
