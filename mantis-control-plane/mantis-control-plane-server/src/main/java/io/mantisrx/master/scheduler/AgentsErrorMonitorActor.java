/*
 * Copyright 2021 Netflix, Inc.
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

package io.mantisrx.master.scheduler;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import lombok.extern.slf4j.Slf4j;

/**
 * This actor is only serving the functionality right now to report worker error.
 */
@Slf4j
public class AgentsErrorMonitorActor extends AbstractActor {

    public static Props props() {
        return Props.create(AgentsErrorMonitorActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LifecycleEventsProto.WorkerStatusEvent.class, js -> onWorkerEvent(js))
                .matchAny(x -> log.warn("unexpected message '{}' received by AgentsErrorMonitorActor actor ", x))
                .build();
    }

    public void onWorkerEvent(LifecycleEventsProto.WorkerStatusEvent workerEvent) {
        if(log.isTraceEnabled()) {
            log.trace("onWorkerEvent {} is error state {}",
                workerEvent, WorkerState.isErrorState(workerEvent.getWorkerState()));
        }
        if(workerEvent.getHostName().isPresent()  && WorkerState.isErrorState(workerEvent.getWorkerState())) {
            log.warn("Worker error state reported: {}", workerEvent);
        }
    }
}
