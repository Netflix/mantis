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

public class MantisAuditLogEvent {

    private final Type type;
    private final String operand;
    private final String data;
    public MantisAuditLogEvent(Type type, String operand, String data) {
        this.type = type;
        this.operand = operand;
        this.data = data;
    }

    public Type getType() {
        return type;
    }

    public String getOperand() {
        return operand;
    }

    public String getData() {
        return data;
    }

    public enum Type {
        NAMED_JOB_CREATE,
        NAMED_JOB_UPDATE,
        NAMED_JOB_DELETE,
        NAMED_JOB_DISABLED,
        NAMED_JOB_ENABLED,
        JOB_SUBMIT,
        JOB_TERMINATE,
        JOB_DELETE,
        JOB_SCALE_UP,
        JOB_SCALE_DOWN,
        JOB_SCALE_UPDATE,
        WORKER_START,
        WORKER_TERMINATE,
        CLUSTER_SCALE_UP,
        CLUSTER_SCALE_DOWN,
        CLUSTER_ACTIVE_VMS
    }
}
