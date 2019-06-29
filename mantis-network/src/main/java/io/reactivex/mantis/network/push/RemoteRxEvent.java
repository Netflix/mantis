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

package io.reactivex.mantis.network.push;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class RemoteRxEvent {

    private String name;
    private Type type;
    private byte[] data;
    private Map<String, String> subscriptionParameters;
    public RemoteRxEvent(String name,
                         Type type, byte[] data, Map<String, String> subscriptionParameters) {
        this.name = name;
        this.type = type;
        this.data = data;
        this.subscriptionParameters = subscriptionParameters;
    }

    public static List<RemoteRxEvent> heartbeat() {
        List<RemoteRxEvent> list = new ArrayList<RemoteRxEvent>(1);
        list.add(new RemoteRxEvent(null, Type.heartbeat, null, null));
        return list;
    }

    public static RemoteRxEvent nonDataError(String name, byte[] errorData) {
        return new RemoteRxEvent(null, Type.nonDataError, errorData, null);
    }

    public static RemoteRxEvent next(String name, byte[] nextData) {
        return new RemoteRxEvent(name, Type.next, nextData, null);
    }

    public static RemoteRxEvent completed(String name) {
        return new RemoteRxEvent(name, Type.completed, null, null);
    }

    public static RemoteRxEvent error(String name, byte[] errorData) {
        return new RemoteRxEvent(name, Type.error, errorData, null);
    }

    public static List<RemoteRxEvent> subscribed(String name, Map<String, String> subscriptionParameters) {
        List<RemoteRxEvent> list = new ArrayList<RemoteRxEvent>(1);
        list.add(new RemoteRxEvent(name, Type.subscribed, null, subscriptionParameters));
        return list;
    }

    public static List<RemoteRxEvent> unsubscribed(String name) {
        List<RemoteRxEvent> list = new ArrayList<RemoteRxEvent>(1);
        list.add(new RemoteRxEvent(name, Type.unsubscribed, null, null));
        return list;
    }

    public byte[] getData() {
        return data;
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    Map<String, String> getSubscribeParameters() {
        return subscriptionParameters;
    }

    @Override
    public String toString() {
        return "RemoteRxEvent [name=" + name + ", type=" + type
                + ", subscriptionParameters=" + subscriptionParameters + "]";
    }

    public enum Type {
        next, completed, error, subscribed, unsubscribed,
        heartbeat,        // used by clients to close connection
        // during a network partition to avoid
        // scenario where client subscription is active
        // but server has forced unsubscribe due to partition.
        // A server will force unsubscribe if unable to
        // write to client after a certain number of attempts.

        nonDataError    // used by server to inform client of errors
        // occurred that are not on the data
        // stream, example: slotting error
    }
}
