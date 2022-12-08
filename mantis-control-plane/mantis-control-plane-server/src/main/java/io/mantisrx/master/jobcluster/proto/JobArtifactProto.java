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

package io.mantisrx.master.jobcluster.proto;

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.JobArtifact;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;

public class JobArtifactProto {

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class SearchJobArtifactsRequest extends BaseRequest {
        String name;
        String version;

        public SearchJobArtifactsRequest(String name, String version) {
            super();
            Preconditions.checkNotNull(name, "JobArtifact name cannot be null");
            this.name = name;
            this.version = version;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class SearchJobArtifactsResponse extends BaseResponse {
        List<JobArtifact> jobArtifacts;

        // TODO(fdichiara): add paginated list.
        public SearchJobArtifactsResponse(
            long requestId,
            ResponseCode responseCode,
            String message,
            List<JobArtifact> jobArtifacts) {
            super(requestId, responseCode, message);
            this.jobArtifacts = jobArtifacts;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class ListJobArtifactsByNameRequest extends BaseRequest {
        String prefix;
        String contains;

        public ListJobArtifactsByNameRequest(String prefix, String contains) {
            super();
            this.prefix = prefix;
            this.contains = contains;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class ListJobArtifactsByNameResponse extends BaseResponse {
        List<String> names;

        // TODO(fdichiara): add paginated list.
        public ListJobArtifactsByNameResponse(
            long requestId,
            ResponseCode responseCode,
            String message,
            List<String> names) {
            super(requestId, responseCode, message);
            this.names = names;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class UpsertJobArtifactRequest extends BaseRequest {
        JobArtifact jobArtifact;

        public UpsertJobArtifactRequest(final JobArtifact jobArtifact) {
            super();
            Preconditions.checkNotNull(jobArtifact, "JobArtifact cannot be null");
            this.jobArtifact = jobArtifact;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class UpsertJobArtifactResponse extends BaseResponse {
        ArtifactID artifactID;

        public UpsertJobArtifactResponse(
            final long requestId,
            final ResponseCode responseCode,
            final String message,
            final ArtifactID artifactID
        ) {
            super(requestId, responseCode, message);
            this.artifactID = artifactID;
        }
    }
}
