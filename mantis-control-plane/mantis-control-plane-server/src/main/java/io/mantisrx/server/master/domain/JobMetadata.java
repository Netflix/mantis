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

package io.mantisrx.server.master.domain;

import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import java.net.URL;
import java.util.List;


public class JobMetadata {

    private final JobId jobId;
    private final URL jobJarUrl;
    private final int totalStages;
    private final String user;
    private final SchedulingInfo schedulingInfo;
    private final List<Parameter> parameters;
    private final long subscriptionTimeoutSecs;
    private final long minRuntimeSecs;

    public JobMetadata(final JobId jobId,
                       final URL jobJarUrl,
                       final int totalStages,
                       final String user,
                       final SchedulingInfo schedulingInfo,
                       final List<Parameter> parameters,
                       final long subscriptionTimeoutSecs,
                       final long minRuntimeSecs) {
        this.jobId = jobId;
        this.jobJarUrl = jobJarUrl;
        this.totalStages = totalStages;
        this.user = user;
        this.schedulingInfo = schedulingInfo;
        this.parameters = parameters;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.minRuntimeSecs = minRuntimeSecs;
    }

    public JobId getJobId() {
        return jobId;
    }

    public URL getJobJarUrl() {
        return jobJarUrl;
    }

    public int getTotalStages() {
        return totalStages;
    }

    public String getUser() {
        return user;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    public long getMinRuntimeSecs() {
        return minRuntimeSecs;
    }
}
