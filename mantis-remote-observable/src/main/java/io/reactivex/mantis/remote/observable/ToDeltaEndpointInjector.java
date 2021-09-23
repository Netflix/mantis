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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


public class ToDeltaEndpointInjector implements EndpointInjector {

    private static final Logger logger = LoggerFactory.getLogger(ToDeltaEndpointInjector.class);

    private Observable<List<Endpoint>> endpointObservable;
    private Observable<EndpointChange> reconcileChanges;

    public ToDeltaEndpointInjector(Observable<List<Endpoint>> endpointObservable) {
        this(endpointObservable, Observable.<EndpointChange>empty());
    }

    public ToDeltaEndpointInjector(Observable<List<Endpoint>> endpointObservable,
                                   Observable<EndpointChange> reconcileChanges) {
        this.endpointObservable = endpointObservable;
        this.reconcileChanges = reconcileChanges;
    }

    private String uniqueHost(String host, int port, String slotId) {
        return host + ":" + port + ":" + slotId;
    }

    private List<EndpointChange> changes(List<Endpoint> previous, List<Endpoint> current) {


        logger.info("Sets to evaluate for differences, current: " + current + " previous: " + previous);

        Map<String, Endpoint> previousSet = new HashMap<>();
        // fill previous
        for (Endpoint endpoint : previous) {
            previousSet.put(uniqueHost(endpoint.getHost(), endpoint.getPort(),
                    endpoint.getSlotId()), endpoint);
        }

        // collect into two buckets: add, complete
        List<EndpointChange> toAdd = new LinkedList<>();
        Set<String> completeCheck = new HashSet<>();
        for (Endpoint endpoint : current) {
            String id = uniqueHost(endpoint.getHost(), endpoint.getPort(),
                    endpoint.getSlotId());
            if (!previousSet.containsKey(id)) {
                EndpointChange ce = new EndpointChange(EndpointChange.Type.add, endpoint);
                toAdd.add(ce);
            } else {
                completeCheck.add(id);
            }
        }
        List<EndpointChange> toComplete = new LinkedList<EndpointChange>();
        // check if need to complete any from current set: set difference
        for (Map.Entry<String, Endpoint> controlledEndpoint : previousSet.entrySet()) {
            if (!completeCheck.contains(controlledEndpoint.getKey())) {
                Endpoint ce = controlledEndpoint.getValue();
                EndpointChange ceToCompletd = new EndpointChange(EndpointChange.Type.complete, ce);
                toComplete.add(ceToCompletd);
            }
        }

        Map<String, EndpointChange> nextSet = new HashMap<>();
        // apply completes
        for (EndpointChange controlledEndpoint : toComplete) {
            nextSet.put(uniqueHost(controlledEndpoint.getEndpoint().getHost(), controlledEndpoint.getEndpoint().getPort(),
                    controlledEndpoint.getEndpoint().getSlotId()), controlledEndpoint);
        }
        // apply adds
        for (EndpointChange controlledEndpoint : toAdd) {
            nextSet.put(uniqueHost(controlledEndpoint.getEndpoint().getHost(), controlledEndpoint.getEndpoint().getPort(),
                    controlledEndpoint.getEndpoint().getSlotId()), controlledEndpoint);
        }
        logger.info("Differences to be applied: " + nextSet);
        return new ArrayList<>(nextSet.values());
    }

    @Override
    public Observable<EndpointChange> deltas() {
        return
                Observable.merge(
                        reconcileChanges,
                        endpointObservable
                                .startWith(Collections.<Endpoint>emptyList())
                                .buffer(2, 1) // stagger current and previous
                                .flatMap(new Func1<List<List<Endpoint>>, Observable<EndpointChange>>() {
                                    @Override
                                    public Observable<EndpointChange> call(List<List<Endpoint>> previousAndCurrent) {
                                        if (previousAndCurrent.size() == 2) {
                                            List<Endpoint> previous = previousAndCurrent.get(0);
                                            List<Endpoint> current = previousAndCurrent.get(1);
                                            return Observable.from(changes(previous, current));
                                        } else {
                                            // skipping last entry, already processed on the
                                            //  last previousAndCurrent pair
                                            return Observable.empty();
                                        }
                                    }
                                }));
    }
}