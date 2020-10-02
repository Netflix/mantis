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

package io.mantisrx.server.master.store;

public class InvalidJobException extends Exception {

    public InvalidJobException(String id) {
        super(id);
    }

    public InvalidJobException(String id, Throwable cause) {
        super(id, cause);
    }

    public InvalidJobException(String jobId, int stageNum, int workerId) {
        super(jobId + ((stageNum >= 0) ? "-stage-" + stageNum : "") + ((workerId >= 0) ? "-worker-" + workerId : ""));
    }

    public InvalidJobException(String jobId, int stageNum, int workerId, Throwable cause) {
        super(jobId + ((stageNum >= 0) ? "-stage-" + stageNum : "") + ((workerId >= 0) ? "-worker-" + workerId : ""), cause);
    }
}
