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

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.dispatch.BoundedMessageQueueSemantics;
import org.apache.pekko.dispatch.RequiresMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AuditEventBrokerActor extends AbstractActor
    implements RequiresMessageQueue<BoundedMessageQueueSemantics> {

    private static final Logger logger = LoggerFactory.getLogger(AuditEventBrokerActor.class);
    private final AuditEventSubscriber auditEventSubscriber;

    public static Props props(AuditEventSubscriber auditEventSubscriber) {
        return Props.create(AuditEventBrokerActor.class, auditEventSubscriber);
    }

    public AuditEventBrokerActor(AuditEventSubscriber auditEventSubscriber) {
        this.auditEventSubscriber = auditEventSubscriber;
    }


    private void onAuditEvent(final LifecycleEventsProto.AuditEvent auditEvent) {
        this.auditEventSubscriber.process(auditEvent);
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(LifecycleEventsProto.AuditEvent.class, auditEvent -> onAuditEvent(auditEvent))
            .build();
    }
}
