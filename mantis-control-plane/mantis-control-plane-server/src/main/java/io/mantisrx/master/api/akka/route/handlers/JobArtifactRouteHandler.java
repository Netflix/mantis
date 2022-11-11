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

package io.mantisrx.master.api.akka.route.handlers;

import io.mantisrx.master.jobcluster.proto.JobArtifactProto;
import java.util.concurrent.CompletionStage;

public interface JobArtifactRouteHandler {
    /**
     * Upsert given job artifact to the metadata store.
     */
    CompletionStage<JobArtifactProto.UpsertJobArtifactResponse> upsert(final JobArtifactProto.UpsertJobArtifactRequest request);

    /**
     * Search job artifacts with given name an optionally given version.
     * If version is not provided the result will contain the list of
     * all job artifacts matching the name.
     */
    CompletionStage<JobArtifactProto.SearchJobArtifactsResponse> search(final JobArtifactProto.SearchJobArtifactsRequest request);

    /**
     * Search job artifact names by given prefix. Returns only the names for faster lookups.
     */
    CompletionStage<JobArtifactProto.ListJobArtifactsByNameResponse> listArtifactsByName(final JobArtifactProto.ListJobArtifactsByNameRequest request);
}
