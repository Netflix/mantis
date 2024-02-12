/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.runtime.loader;

import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * SubscriptionStateHandler that kills the job if the sink has been unsubscribed for subscriptionTimeout seconds.
 */
@Slf4j
class SubscriptionStateHandlerImpl extends AbstractScheduledService implements SinkSubscriptionStateHandler {

    private final String jobId;
    private final MantisMasterGateway masterClientApi;
    private final Duration subscriptionTimeout;
    private final Duration minRuntime;
    private final AtomicReference<SubscriptionState> currentState = new AtomicReference<>();
    private final Clock clock;

    SubscriptionStateHandlerImpl(String jobId, MantisMasterGateway masterClientApi, long subscriptionTimeoutSecs, long minRuntimeSecs, Clock clock) {
        Preconditions.checkArgument(subscriptionTimeoutSecs > 0, "subscriptionTimeoutSecs should be > 0");
        this.jobId = jobId;
        this.masterClientApi = masterClientApi;
        this.subscriptionTimeout = Duration.ofSeconds(subscriptionTimeoutSecs);
        this.minRuntime = Duration.ofSeconds(minRuntimeSecs);
        this.clock = clock;
    }

    @Override
    public void startUp() {
        currentState.compareAndSet(null, SubscriptionState.of(clock));
        log.info("SubscriptionStateHandlerImpl service started.");
    }

    @Override
    protected Scheduler scheduler() {
        // scheduler that runs every second to check if the subscriber is unsubscribed for subscriptionTimeoutSecs.
        return Scheduler.newFixedDelaySchedule(1, 1, TimeUnit.SECONDS);
    }

    @Override
    protected void runOneIteration() {
        SubscriptionState state = currentState.get();
        if (state.isUnsubscribedFor(subscriptionTimeout)) {
            if (state.hasRunFor(minRuntime)) {
                try {
                    log.info("Calling master to kill due to subscription timeout");
                    masterClientApi.killJob(jobId, "MantisWorker", "No subscriptions for " +
                            subscriptionTimeout.getSeconds() + " secs")
                        .single()
                        .toBlocking()
                        .first();
                } catch (Exception e) {
                    log.error("Failed to kill job {} due to no subscribers for {} seconds", jobId, state.getUnsubscribedDuration().getSeconds());
                }
            } else {
                log.info("Not killing job {} as it has not run for minRuntimeSecs {}", jobId, minRuntime.getSeconds());
            }
        }
    }

    @Override
    public void onSinkUnsubscribed() {
        if (currentState.get() == null) {
            log.error("currentState in SubscriptionStateHandlerImpl is not set onSinkUnsubscribed");
            return;
        }
        log.info("Sink unsubscribed");
        currentState.updateAndGet(SubscriptionState::onSinkUnsubscribed);
    }

    @Override
    public void onSinkSubscribed() {
        log.info("Sink subscribed");
        Preconditions.checkNotNull(currentState.get(), "currentState is not intialized onSinkSubscribed.");
        currentState.updateAndGet(SubscriptionState::onSinkSubscribed);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Value
    static class SubscriptionState {
        boolean subscribed;
        Instant startedAt;
        Instant lastUnsubscriptionTime;
        Clock clock;

        static SubscriptionState of(Clock clock) {
            return new SubscriptionState(false, clock.instant(), clock.instant(), clock);
        }

        public SubscriptionState onSinkSubscribed() {
            if (isSubscribed()) {
                return this;
            } else {
                return new SubscriptionState(true, startedAt, lastUnsubscriptionTime, clock);
            }
        }

        public SubscriptionState onSinkUnsubscribed() {
            if (isSubscribed()) {
                return new SubscriptionState(false, startedAt, clock.instant(), clock);
            } else {
                return this;
            }
        }

        public boolean hasRunFor(Duration duration) {
            return Duration.between(startedAt, clock.instant()).compareTo(duration) >= 0;
        }

        public boolean isUnsubscribedFor(Duration duration) {
            return !isSubscribed() && Duration.between(lastUnsubscriptionTime, clock.instant()).compareTo(duration) >= 0;
        }

        public Duration getUnsubscribedDuration() {
            Preconditions.checkState(!isSubscribed());
            return Duration.between(lastUnsubscriptionTime, clock.instant());
        }
    }
}
