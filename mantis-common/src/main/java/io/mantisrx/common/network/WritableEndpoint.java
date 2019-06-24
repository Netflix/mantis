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

package io.mantisrx.common.network;

import java.util.Optional;

import com.netflix.spectator.api.BasicTag;
import io.reactivx.mantis.operators.DropOperator;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;


public class WritableEndpoint<T> extends Endpoint implements Comparable<WritableEndpoint<T>> {

    private Subject<T, T> subject;
    private ObservableConnection<?, ?> connection;

    public WritableEndpoint(String host, int port, String slotId) {
        this(host, port, slotId, null);
    }

    public WritableEndpoint(String host, int port, String slotId,
                            ObservableConnection<?, ?> connection) {
        super(host, port, slotId);
        subject = new SerializedSubject<T, T>(PublishSubject.<T>create());
        this.connection = connection;
    }

    public WritableEndpoint(String host, int port) {
        super(host, port);
        subject = new SerializedSubject<T, T>(PublishSubject.<T>create());
    }

    public void write(T value) {
        subject.onNext(value);
    }

    public void explicitClose() {
        if (connection != null) {
            connection.close(true);
        }
    }

    public void complete() {
        subject.onCompleted();
        explicitClose();
    }

    public Observable<T> read() {
        return subject
                .lift(new DropOperator<>("outgoing_subject", new BasicTag("slotId", Optional.ofNullable(slotId).orElse("none"))));
    }

    @Override
    public String toString() {
        return "WritableEndpoint [" + super.toString() + "]";
    }

    public void error(Throwable e) {
        subject.onError(e);
        explicitClose();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((slotId == null) ? 0 : slotId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Endpoint other = (Endpoint) obj;
        if (slotId == null) {
            if (other.slotId != null)
                return false;
        } else if (!slotId.equals(other.slotId))
            return false;
        return true;
    }

    @Override
    public int compareTo(WritableEndpoint<T> o) {
        if (this.equals(o)) {
            return 0;
        } else {
            return o.getSlotId().compareTo(getSlotId());
        }
    }


}
