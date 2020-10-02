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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEventSubscriberLoggingImpl implements StatusEventSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(StatusEventSubscriberLoggingImpl.class);

    @Override
    public void process(final LifecycleEventsProto.StatusEvent statusEvent) {
        String message = " " + statusEvent.statusEventType + " " + statusEvent.message + " ";
        if (statusEvent instanceof LifecycleEventsProto.WorkerStatusEvent) {
            LifecycleEventsProto.WorkerStatusEvent wse = (LifecycleEventsProto.WorkerStatusEvent) statusEvent;
            message = wse.getWorkerId().getId() + message + wse.getWorkerState();
        } else if (statusEvent instanceof LifecycleEventsProto.JobStatusEvent) {
            LifecycleEventsProto.JobStatusEvent jse = (LifecycleEventsProto.JobStatusEvent) statusEvent;
            message = jse.getJobId() + message + jse.getJobState();
        } else if (statusEvent instanceof LifecycleEventsProto.JobClusterStatusEvent) {
            LifecycleEventsProto.JobClusterStatusEvent jcse = (LifecycleEventsProto.JobClusterStatusEvent) statusEvent;
            message = jcse.getJobCluster() + message;
        }
        logger.info("[STATUS] {}", message);
    }
}
