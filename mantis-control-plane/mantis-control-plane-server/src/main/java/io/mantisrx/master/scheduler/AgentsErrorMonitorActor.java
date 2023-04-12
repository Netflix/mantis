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

import static java.util.Optional.empty;
import static java.util.Optional.of;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

public class AgentsErrorMonitorActor extends AbstractActorWithTimers implements IAgentsErrorMonitor {

    private final Logger logger = LoggerFactory.getLogger(AgentsErrorMonitorActor.class);
    private static final long ERROR_CHECK_WINDOW_MILLIS=120000; // 2 mins
    private static final int ERROR_CHECK_WINDOW_COUNT=3;
    private static final long TOO_OLD_MILLIS = 3600000;
    private static final long DISABLE_DURATION_MILLIS = 60*1000; // 1mins
    private  Action1<String> slaveEnabler = s -> logger.warn("SlaveEnabler not initialized yet!");
    private  Action1<String> slaveDisabler = s -> logger.warn("SlaveDisabler not initialized yet!");
    private long too_old_mills;
    private int error_check_window_count;
    private long error_check_window_millis;
    private long disableDurationMillis;
    private final Map<String, HostErrors> hostErrorMap = new HashMap<>();
    private static final String CHECK_HOST_TIMER_KEY = "CHECK_HOST";
    private Optional<MantisScheduler> mantisSchedulerOptional = empty();
    // Behavior after being initialized
    Receive initializedBehavior;

    public static Props props(long too_old_millis, int error_check_window_count,  long error_check_window_millis, long disableDurationMillis) {
        return Props.create(AgentsErrorMonitorActor.class, too_old_millis, error_check_window_count,error_check_window_millis, disableDurationMillis);
    }

    public static Props props() {
        return Props.create(AgentsErrorMonitorActor.class, TOO_OLD_MILLIS, ERROR_CHECK_WINDOW_COUNT,ERROR_CHECK_WINDOW_MILLIS, DISABLE_DURATION_MILLIS);
    }

    public AgentsErrorMonitorActor() {
        this(TOO_OLD_MILLIS,ERROR_CHECK_WINDOW_COUNT,ERROR_CHECK_WINDOW_MILLIS, DISABLE_DURATION_MILLIS);
    }

    public AgentsErrorMonitorActor(long too_old_millis, int error_check_window_count,  long error_check_window_millis, long disableDurationMillis) {

        this.too_old_mills = (too_old_millis>0)? too_old_millis : TOO_OLD_MILLIS;
        this.error_check_window_count = (error_check_window_count>0)? error_check_window_count : ERROR_CHECK_WINDOW_COUNT;
        this.error_check_window_millis = (error_check_window_millis>1000)? error_check_window_millis : ERROR_CHECK_WINDOW_MILLIS;
        this.disableDurationMillis = (disableDurationMillis>-1) ? disableDurationMillis : DISABLE_DURATION_MILLIS;

        this.initializedBehavior = receiveBuilder()
                .match(LifecycleEventsProto.WorkerStatusEvent.class, js -> onWorkerEvent(js))
                .match(CheckHostHealthMessage.class, js -> onCheckHostHealth())
                .match(HostErrorMapRequest.class, js -> onHostErrorMapRequest())
                .matchAny(x -> logger.warn("unexpected message '{}' received by AgentsErrorMonitorActor actor ", x))
                .build();
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InitializeAgentsErrorMonitor.class, js -> onInitialize(js))
                .matchAny(x -> logger.warn("unexpected message '{}' received by AgentsErrorMonitorActor actor ", x))
                .build();
    }

    public void onInitialize(InitializeAgentsErrorMonitor initializeAgentsErrorMonitor) {
        this.mantisSchedulerOptional = of(initializeAgentsErrorMonitor.getScheduler());
        slaveDisabler = hostName -> mantisSchedulerOptional.get().disableVM(hostName,disableDurationMillis);
        slaveEnabler = hostName -> mantisSchedulerOptional.get().enableVM(hostName);
        getContext().become(initializedBehavior);
        getTimers().startTimerAtFixedRate(CHECK_HOST_TIMER_KEY, new CheckHostHealthMessage(),
            scala.concurrent.duration.Duration.create(error_check_window_millis, TimeUnit.MILLISECONDS));
    }

    @Override
    public void onCheckHostHealth() {
        Instant currentTime = Instant.now();
        Iterator<HostErrors> it = hostErrorMap.values().iterator();

        while(it.hasNext()) {
            HostErrors hErrors = it.next();
            long lastActivityAt = hErrors.getLastActivityAt();
            long timeSinceLastEvent = currentTime.toEpochMilli() - lastActivityAt;
            if(timeSinceLastEvent > this.too_old_mills) {
                logger.debug("No Events from host since {} evicting", timeSinceLastEvent);
                it.remove();
            }
        }

    }

    @Override
    public void onWorkerEvent(LifecycleEventsProto.WorkerStatusEvent workerEvent) {
        if(logger.isTraceEnabled()) { logger.trace("onWorkerEvent " +  workerEvent + " is error state " + WorkerState.isErrorState(workerEvent.getWorkerState())); }
        if(workerEvent.getHostName().isPresent()  && WorkerState.isErrorState(workerEvent.getWorkerState())) {

            String hostName = workerEvent.getHostName().get();
            logger.info("Registering worker error on host {}", hostName);

            HostErrors hostErrors = hostErrorMap.computeIfAbsent(hostName, (hName) -> new HostErrors(hName,slaveEnabler,this.error_check_window_millis,this.error_check_window_count));
            if(hostErrors.addAndGetIsTooManyErrors(workerEvent)) {
                logger.warn("Host {} has too many errors in a short duration, disabling..", hostName);
                this.slaveDisabler.call(hostName);
            }

        }

    }

    @Override
    public void onHostErrorMapRequest() {
        ActorRef sender = getSender();
        sender.tell(new HostErrorMapResponse(Collections.unmodifiableMap(this.hostErrorMap)), getSelf());
    }



    public static class InitializeAgentsErrorMonitor {
        private final MantisScheduler scheduler;

        public InitializeAgentsErrorMonitor(final MantisScheduler scheduler) {
            Preconditions.checkNotNull(scheduler, "MantisScheduler cannot be null");
            this.scheduler = scheduler;
        }

        public MantisScheduler getScheduler() {
            return this.scheduler;
        }


    }
    static class CheckHostHealthMessage {
        long now = -1;
        public CheckHostHealthMessage() {

        }

        public CheckHostHealthMessage(long now) {
            this.now = now;
        }

        public long getCurrentTime() {
            if(now == -1) {
                return System.currentTimeMillis();
            } else {
                return this.now;
            }
        }

    }

    static class HostErrorMapRequest {

    }

    static class HostErrorMapResponse {

        private final Map<String, HostErrors> errorMap;
        public HostErrorMapResponse(final Map<String, HostErrors> hostErrorsMap) {
            this.errorMap = hostErrorsMap;
        }

        public Map<String,HostErrors> getMap() {
            return this.errorMap;
        }

    }

    static class HostErrors {
        private static final Logger logger = LoggerFactory.getLogger(HostErrors.class);
        private final String hostname;
        private final List<Long> errors;
        private long lastActivityAt = System.currentTimeMillis();
        private final Action1<String> slaveEnabler;
        private final long error_check_window_millis;
        private final int windowCount;
        HostErrors(String hostname, Action1<String> slaveEnabler, long error_check_window_millis, int windowCount) {
            this.hostname = hostname;
            this.errors = new ArrayList<>();
            this.slaveEnabler = slaveEnabler;
            this.error_check_window_millis = error_check_window_millis;
            this.windowCount = windowCount;
        }
        long getLastActivityAt() {
            return lastActivityAt;
        }
        boolean addAndGetIsTooManyErrors(LifecycleEventsProto.WorkerStatusEvent status) {
            logger.info("InaddGetisTooManyErrors for host {}", hostname);
            lastActivityAt = status.getTimestamp();
            if(WorkerState.isErrorState(status.getWorkerState())) {
                errors.add(lastActivityAt);
                logger.info("Registering error {}", errors);
            } else if(status.getWorkerState() == WorkerState.Started) {
                // saw a successfull worker start and error list is not empty clear it and reenable host
                if(!errors.isEmpty()) {
                    errors.clear();
                    logger.info("{} cleared of errors, reenabling host ", hostname);
                    slaveEnabler.call(hostname);

                }
            }
            final Iterator<Long> iterator = errors.iterator();
            while(iterator.hasNext()) {
                final long next = iterator.next();
                // purge old events (rolling window)
                if((lastActivityAt - next) >  error_check_window_millis)
                    iterator.remove();
            }
            logger.info("No of errors in window is {} ", errors.size());
            return errors.size() > windowCount;
        }

        List<Long> getErrorTimestampList() {
            return Collections.unmodifiableList(errors);
        }
    }

}
