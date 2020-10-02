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

package io.mantisrx.master.api.akka.route.utils;

import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.server.core.JobSchedulingInfo;

public class JobDiscoveryHeartbeats {
    public static final JobClusterInfo JOB_CLUSTER_INFO_HB_INSTANCE = new JobClusterInfo(JobSchedulingInfo.HB_JobId, null);
    public static final JobSchedulingInfo SCHED_INFO_HB_INSTANCE = new JobSchedulingInfo(JobSchedulingInfo.HB_JobId, null);
}
