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

package io.mantisrx.server.master;

import java.net.URL;


public class WorkerJobDetails {

    private final String jobName;
    private final String jobId;
    private final String user;
    private final URL jobJarUrl;
    private final JobRequest request;

    public WorkerJobDetails(String user, String jobId, URL jobJarUrl,
                            JobRequest request, String jobName) {
        this.user = user;
        this.jobId = jobId;
        this.jobJarUrl = jobJarUrl;
        this.request = request;
        this.jobName = jobName;
    }

    public String getUser() {
        return user;
    }

    public String getJobId() {
        return jobId;
    }

    public URL getJobJarUrl() {
        return jobJarUrl;
    }

    public JobRequest getRequest() {
        return request;
    }

    public MantisJobMgr getJobMgr() {
        return request.getJobMgr();
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public String toString() {
        return "jobName=" + jobName + ", jobId=" + jobId + ", jobUrl=" + jobJarUrl;
    }
}
