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

import java.util.List;

import mantis.io.reactivex.netty.channel.ObservableConnection;
import rx.Subscription;
import rx.subscriptions.BooleanSubscription;


public class RemoteUnsubscribe implements Subscription {

    private ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection;
    private BooleanSubscription subscription = new BooleanSubscription();
    private String observableName;

    public RemoteUnsubscribe(String observableName) {
        this.observableName = observableName;
    }

    @Override
    public void unsubscribe() {
        if (connection != null) {
            connection.writeAndFlush(RemoteRxEvent.unsubscribed(observableName)); // write unsubscribe event to server
        }
        subscription.unsubscribe();
    }

    @Override
    public boolean isUnsubscribed() {
        return subscription.isUnsubscribed();
    }

    void setConnection(
            ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
        this.connection = connection;
    }

}
