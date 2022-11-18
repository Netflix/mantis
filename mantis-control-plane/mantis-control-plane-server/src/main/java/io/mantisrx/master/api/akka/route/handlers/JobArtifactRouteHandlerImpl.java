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

import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;

import io.mantisrx.master.jobcluster.proto.JobArtifactProto;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobArtifactRouteHandlerImpl implements JobArtifactRouteHandler {
    private final IMantisPersistenceProvider mantisStorageProvider;

    public JobArtifactRouteHandlerImpl(IMantisPersistenceProvider mantisStorageProvider) {
        this.mantisStorageProvider = mantisStorageProvider;
    }

    @Override
    public CompletionStage<JobArtifactProto.SearchJobArtifactsResponse> search(JobArtifactProto.SearchJobArtifactsRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final List<JobArtifact> jobArtifactList = mantisStorageProvider.listJobArtifacts(request.getName(), request.getVersion());
                return new JobArtifactProto.SearchJobArtifactsResponse(request.requestId, SUCCESS, "", jobArtifactList);
            } catch (IOException e) {
                log.warn("Error while fetching job artifacts. Traceback: {}", e.getMessage(), e);
                return new JobArtifactProto.SearchJobArtifactsResponse(request.requestId, SERVER_ERROR, e.getMessage(), Collections.emptyList());
            }
        });
    }

    @Override
    public CompletionStage<JobArtifactProto.ListJobArtifactsByNameResponse> listArtifactsByName(JobArtifactProto.ListJobArtifactsByNameRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final List<String> artifactNames = mantisStorageProvider.listJobArtifactsByName(request.getPrefix());
                return new JobArtifactProto.ListJobArtifactsByNameResponse(request.requestId, SUCCESS, "", artifactNames);
            } catch (IOException e) {
                log.warn("Error while searching job artifact names. Traceback: {}", e.getMessage(), e);
                return new JobArtifactProto.ListJobArtifactsByNameResponse(request.requestId, SERVER_ERROR, e.getMessage(), Collections.emptyList());
            }
        });
    }

    @Override
    public CompletionStage<JobArtifactProto.UpsertJobArtifactResponse> upsert(JobArtifactProto.UpsertJobArtifactRequest request) {
        final JobArtifact jobArtifact = request.getJobArtifact();
        return CompletableFuture.supplyAsync(() -> {
            try {
                mantisStorageProvider.addNewJobArtifact(jobArtifact);
                return new JobArtifactProto.UpsertJobArtifactResponse(request.requestId, SUCCESS, "", jobArtifact.getArtifactID());
            } catch (IOException e) {
                log.warn("Error while storing new job artifact. Traceback: {}", e.getMessage(), e);
                return new JobArtifactProto.UpsertJobArtifactResponse(request.requestId, SERVER_ERROR, e.getMessage(), jobArtifact.getArtifactID());
            }
        });
    }
}
