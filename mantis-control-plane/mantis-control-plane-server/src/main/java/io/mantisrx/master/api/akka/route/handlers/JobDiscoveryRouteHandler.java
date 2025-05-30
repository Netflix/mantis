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

import static io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto.SchedInfoResponse;

import io.mantisrx.master.api.akka.route.proto.JobDiscoveryRouteProto;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto;
import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;

import java.util.concurrent.CompletionStage;

public interface JobDiscoveryRouteHandler {
    CompletionStage<SchedInfoResponse> schedulingInfoStream(final JobClusterManagerProto.GetJobSchedInfoRequest request,
                                                           final boolean sendHeartbeats);
    CompletionStage<JobDiscoveryRouteProto.JobClusterInfoResponse> lastSubmittedJobIdStream(final JobClusterManagerProto.GetLastSubmittedJobIdStreamRequest request,
                                                                                            final boolean sendHeartbeats);

    CompletionStage<JobClusterScalerRuleProto.GetJobScalerRuleStreamResponse> jobScalerRuleStream(
        final JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest request,
        final boolean sendHeartbeats);
}
