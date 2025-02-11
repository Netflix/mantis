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

import java.util.List;
import rx.Observer;
import rx.functions.Func1;


public class AsyncConnection<T> {

    private String host;
    private int port;

    // connections sharing data
    // should have the same group Id:  test-job
    // a fixed number of slotId's:	[1-5]
    // and a unique id:	monotonically increasing number
    private String groupId;
    private String slotId;
    private String id;

    private String availabilityZone;

    private Observer<List<byte[]>> subject;
    private Func1<T, Boolean> predicate;

    public AsyncConnection(String host, int port, String id,
                           String slotId,
                           String groupId, Observer<List<byte[]>> subject,
                           Func1<T, Boolean> predicate) {
        this(host, port, id, slotId, groupId, subject, predicate, null);
    }

    public AsyncConnection(String host, int port, String id,
                           String slotId,
                           String groupId, Observer<List<byte[]>> subject,
                           Func1<T, Boolean> predicate, String availabilityZone) {
        this.host = host;
        this.port = port;
        this.id = id;
        this.groupId = groupId;
        this.subject = subject;
        this.predicate = predicate;
        this.slotId = slotId;
        this.availabilityZone = availabilityZone;
    }

    public Func1<T, Boolean> getPredicate() {
        return predicate;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getHost() {
        return host;
    }

    public String getSlotId() {
        return slotId;
    }

    public int getPort() {
        return port;
    }

    public String getId() {
        return id;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public void close() {
        subject.onCompleted();
    }

    public void write(List<byte[]> data) {
        subject.onNext(data);
    }

    @Override
    public String toString() {
        return "AsyncConnection [host=" + host + ", port=" + port
            + ", groupId=" + groupId + ", slotId=" + slotId + ", id=" + id
            + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AsyncConnection<T> other = (AsyncConnection<T>) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }
}
