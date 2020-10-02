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

import io.mantisrx.master.jobcluster.proto.BaseResponse;
import io.mantisrx.server.core.JobSchedulingInfo;
import rx.Observable;

import java.util.Optional;

public class JobDiscoveryRouteProto {
    public static class SchedInfoResponse extends BaseResponse {
        private final Optional<Observable<JobSchedulingInfo>> schedInfoStream;

        public SchedInfoResponse(final long requestId,
                                 final ResponseCode responseCode,
                                 final String message,
                                 final Observable<JobSchedulingInfo> schedInfoStream) {
            super(requestId, responseCode, message);
            this.schedInfoStream = Optional.ofNullable(schedInfoStream);
        }

        public SchedInfoResponse(final long requestId,
                                 final ResponseCode responseCode,
                                 final String message) {
            super(requestId, responseCode, message);
            this.schedInfoStream = Optional.empty();
        }

        public Optional<Observable<JobSchedulingInfo>> getSchedInfoStream() {
            return schedInfoStream;
        }
    }

    public static class JobClusterInfoResponse extends BaseResponse {
        private final Optional<Observable<JobClusterInfo>> jobClusterInfoObs;

        public JobClusterInfoResponse(final long requestId,
                                 final ResponseCode responseCode,
                                 final String message,
                                 final Observable<JobClusterInfo> jobClusterInfoObservable) {
            super(requestId, responseCode, message);
            this.jobClusterInfoObs = Optional.ofNullable(jobClusterInfoObservable);
        }

        public JobClusterInfoResponse(final long requestId,
                                 final ResponseCode responseCode,
                                 final String message) {
            super(requestId, responseCode, message);
            this.jobClusterInfoObs = Optional.empty();
        }

        public Optional<Observable<JobClusterInfo>> getJobClusterInfoObs() {
            return jobClusterInfoObs;
        }
    }
}
