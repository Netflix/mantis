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

import io.mantisrx.server.core.Status;
import rx.Observable;


public class MantisJobStatus {

    private String jobId;
    private String name;
    private long timestamp;
    private Observable<Status> status;
    private String fatalError = null;

    MantisJobStatus(String jobId, Observable<Status> status,
                    String name) {
        this.jobId = jobId;
        this.status = status;
        this.name = name;
        timestamp = System.currentTimeMillis();
    }

    public String getJobId() {
        return jobId;
    }

    public Observable<Status> getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean hasFatalError() {
        return fatalError != null;
    }

    public String getFatalError() {
        return fatalError;
    }

    public void setFatalError(String fatalError) {
        this.fatalError = fatalError;
    }
}
