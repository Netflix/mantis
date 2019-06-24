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

package io.mantisrx.master.api.akka.route.proto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.server.core.Status;

import java.util.Objects;

public class JobStatus {
    private final Status status;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobStatus(@JsonProperty("status") final Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final JobStatus jobStatus = (JobStatus) o;
        return Objects.equals(status, jobStatus.status);
    }

    @Override
    public int hashCode() {

        return Objects.hash(status);
    }

    @Override
    public String toString() {
        return "JobStatus{" +
            "status=" + status +
            '}';
    }
}
