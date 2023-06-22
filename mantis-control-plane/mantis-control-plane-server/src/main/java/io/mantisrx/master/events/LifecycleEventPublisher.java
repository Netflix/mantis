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

package io.mantisrx.master.events;

import io.mantisrx.master.events.LifecycleEventsProto.AuditEvent;
import io.mantisrx.master.events.LifecycleEventsProto.StatusEvent;
import io.mantisrx.master.events.LifecycleEventsProto.WorkerListChangedEvent;

public interface LifecycleEventPublisher {

    void publishAuditEvent(LifecycleEventsProto.AuditEvent auditEvent);

    void publishStatusEvent(LifecycleEventsProto.StatusEvent statusEvent);

    void publishWorkerListChangedEvent(
        LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent);

    public static LifecycleEventPublisher noop() {
        return NOOP;
    }

    static LifecycleEventPublisher NOOP = new NoopLifecycleEventPublisher();

    class NoopLifecycleEventPublisher implements LifecycleEventPublisher {

        @Override
        public void publishAuditEvent(AuditEvent auditEvent) {
        }

        @Override
        public void publishStatusEvent(StatusEvent statusEvent) {
        }

        @Override
        public void publishWorkerListChangedEvent(WorkerListChangedEvent workerListChangedEvent) {
        }
    }
}
