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

import io.mantisrx.common.network.WritableEndpoint;
import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import java.util.List;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;


public class WriteBytesObserver<T> extends SafeWriter implements Action1<List<RemoteRxEvent>> {

    private final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection;
    private final MutableReference<Subscription> subReference;
    private final RxMetrics serverMetrics;
    private final SlottingStrategy<T> slottingStrategy;
    private final WritableEndpoint<T> endpoint;

    public WriteBytesObserver(
            ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
            MutableReference<Subscription> subReference,
            RxMetrics serverMetrics, SlottingStrategy<T> slottingStrategy,
            WritableEndpoint<T> endpoint) {
        this.connection = connection;
        this.subReference = subReference;
        this.serverMetrics = serverMetrics;
        this.slottingStrategy = slottingStrategy;
        this.endpoint = endpoint;
    }

    @Override
    public void call(final List<RemoteRxEvent> events) {
        if (!safeWrite(connection,
                events,
                subReference,
                // successful write callback
                new Action0() {
                    @Override
                    public void call() {
                        serverMetrics.incrementNextCount(events.size());
                    }
                },
                // failed write callback
                new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        serverMetrics.incrementNextFailureCount(events.size());
                        logger.warn("Failed to write onNext event to remote observable: " + endpoint + ""
                                + " at address: " + connection.getChannel().remoteAddress() + " reason: " + t1.getMessage() +
                                " force unsubscribe", t1);
                        // TODO add callback here to notify when complete even fails
                        subReference.getValue().unsubscribe();
                    }
                },
                slottingStrategy,
                endpoint
        )) {
            // check if connection is closed
            if (connection.isCloseIssued()) {
                slottingStrategy.removeConnection(endpoint);
            }
            serverMetrics.incrementNextFailureCount();
        }
        ;
    }

}
