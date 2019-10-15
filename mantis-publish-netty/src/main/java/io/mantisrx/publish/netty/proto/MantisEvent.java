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

package io.mantisrx.publish.netty.proto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;


public class MantisEvent {

    private final int id;

    private final String data;

    @JsonCreator
    public MantisEvent(@JsonProperty("id") int id, @JsonProperty("data") String data) {
        this.id = id;
        this.data = data;
    }

    public int getId() {
        return id;
    }

    public String getData() {
        return data;
    }

    /**
     * Estimate the size (in Bytes) of this object using the size of its fields.
     */
    public int size() {
        return Integer.BYTES                // id
                + data.getBytes().length;   // data
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisEvent that = (MantisEvent) o;
        return getId() == that.getId() &&
                Objects.equals(getData(), that.getData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getData());
    }

    @Override
    public String toString() {
        return "MantisEvent{" +
                "id=" + id +
                ", data='" + data + '\'' +
                '}';
    }
}
