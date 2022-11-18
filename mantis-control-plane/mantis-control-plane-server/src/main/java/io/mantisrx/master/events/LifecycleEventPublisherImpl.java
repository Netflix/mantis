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

import io.mantisrx.master.events.LifecycleEventsProto.WorkerStatusEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LifecycleEventPublisherImpl implements LifecycleEventPublisher {

    private final AuditEventSubscriber auditEventSubscriber;
    private final StatusEventSubscriber statusEventSubscriber;
    private final WorkerEventSubscriber workerEventSubscriber;

    public LifecycleEventPublisherImpl(final AuditEventSubscriber auditEventSubscriber,
                                       final StatusEventSubscriber statusEventSubscriber,
                                       final WorkerEventSubscriber workerEventSubscriber) {
        this.auditEventSubscriber = auditEventSubscriber;
        this.statusEventSubscriber = statusEventSubscriber;
        this.workerEventSubscriber = workerEventSubscriber;
    }

    @Override
    public void publishAuditEvent(final LifecycleEventsProto.AuditEvent auditEvent) {
        auditEventSubscriber.process(auditEvent);
    }

    @Override
    public void publishStatusEvent(final LifecycleEventsProto.StatusEvent statusEvent) {
        try {
            statusEventSubscriber.process(statusEvent);
            if (statusEvent instanceof LifecycleEventsProto.JobStatusEvent) {
                LifecycleEventsProto.JobStatusEvent jobStatusEvent = (LifecycleEventsProto.JobStatusEvent) statusEvent;
                workerEventSubscriber.process(jobStatusEvent);
            } else if (statusEvent instanceof WorkerStatusEvent) {
                workerEventSubscriber.process((WorkerStatusEvent) statusEvent);
            }
        } catch (Exception e) {
            log.error("Failed to publish the event={}; Ignoring the failure as this is just a listener interface", statusEvent, e);
        }
    }

    @Override
    public void publishWorkerListChangedEvent(LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent) {
        workerEventSubscriber.process(workerListChangedEvent);
    }


}
