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


public class EndpointChange {

    private final Type type;
    private final Endpoint endpoint;
    public EndpointChange(final Type type, final Endpoint endpoint) {
        this.type = type;
        this.endpoint = endpoint;
    }

    public Type getType() {
        return type;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndpointChange that = (EndpointChange) o;

        if (type != that.type) return false;
        return endpoint != null ? endpoint.equals(that.endpoint) : that.endpoint == null;

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (endpoint != null ? endpoint.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EndpointChange{" +
                "type=" + type +
                ", endpoint=" + endpoint +
                '}';
    }

    public enum Type {add, complete}
}
