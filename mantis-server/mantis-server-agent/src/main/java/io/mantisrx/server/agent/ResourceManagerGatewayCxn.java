/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.retry.event.RetryOnRetryEvent;
import io.mantisrx.common.Ack;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.properties.LongDynamicProperty;
import io.mantisrx.server.agent.utils.DurableBooleanState;
import io.mantisrx.server.agent.utils.ExponentialBackoffAbstractScheduledService;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of = "gateway")
class ResourceManagerGatewayCxn extends ExponentialBackoffAbstractScheduledService {

    private final int idx;
    private final TaskExecutorRegistration taskExecutorRegistration;
    @Getter
    @Setter
    private volatile ResourceClusterGateway gateway;

    private final LongDynamicProperty heartBeatIntervalDp;
    private final LongDynamicProperty heartBeatTimeoutDp;

    private final long registrationRetryInitialDelayMillis;
    private final double registrationRetryMultiplier;
    private final double registrationRetryRandomizationFactor;
    private final int registrationRetryMaxAttempts;
    private final int tolerableConsecutiveHeartbeatFailures;
    private final TaskExecutor taskExecutor;
    // flag representing if the task executor has been registered with the resource manager
    @Getter
    private volatile boolean registered = false;

    private boolean hasRan = false;
    private final Counter heartbeatTimeoutCounter;
    private final Counter heartbeatFailureCounter;
    private final Counter taskExecutorRegistrationFailureCounter;
    private final Counter taskExecutorDisconnectionFailureCounter;
    private final Counter taskExecutorRegistrationCounter;
    private final Counter taskExecutorDisconnectionCounter;
    private final DurableBooleanState alreadyRegistered;

    ResourceManagerGatewayCxn(
        int idx,
        TaskExecutorRegistration taskExecutorRegistration,
        ResourceClusterGateway gateway,
        LongDynamicProperty heartBeatIntervalDp,
        LongDynamicProperty heartBeatTimeoutDp,
        TaskExecutor taskExecutor,
        int tolerableConsecutiveHeartbeatFailures,
        long heartbeatRetryInitialDelayMillis,
        long heartbeatRetryMaxDelayMillis,
        long registrationRetryInitialDelayMillis,
        double registrationRetryMultiplier,
        double registrationRetryRandomizationFactor,
        int registrationRetryMaxAttempts,
        DurableBooleanState alreadyRegistered) {
        super(heartbeatRetryInitialDelayMillis, heartbeatRetryMaxDelayMillis);
        this.tolerableConsecutiveHeartbeatFailures = tolerableConsecutiveHeartbeatFailures;
        this.idx = idx;
        this.taskExecutorRegistration = taskExecutorRegistration;
        this.gateway = gateway;
        this.heartBeatIntervalDp = heartBeatIntervalDp;
        this.heartBeatTimeoutDp = heartBeatTimeoutDp;
        this.taskExecutor = taskExecutor;
        this.registrationRetryInitialDelayMillis = registrationRetryInitialDelayMillis;
        this.registrationRetryMultiplier = registrationRetryMultiplier;
        this.registrationRetryRandomizationFactor = registrationRetryRandomizationFactor;
        this.registrationRetryMaxAttempts = registrationRetryMaxAttempts;
        this.alreadyRegistered = alreadyRegistered;

        final MetricGroupId teGroupId = new MetricGroupId("TaskExecutor");
        Metrics m = new Metrics.Builder()
                .id(teGroupId)
                .addCounter("heartbeatTimeout")
                .addCounter("heartbeatFailure")
                .addCounter("taskExecutorRegistrationFailure")
                .addCounter("taskExecutorDisconnectionFailure")
                .addCounter("taskExecutorRegistration")
                .addCounter("taskExecutorDisconnection")
                .build();
        this.heartbeatTimeoutCounter = m.getCounter("heartbeatTimeout");
        this.heartbeatFailureCounter = m.getCounter("heartbeatFailure");
        this.taskExecutorRegistrationFailureCounter = m.getCounter("taskExecutorRegistrationFailure");
        this.taskExecutorDisconnectionFailureCounter = m.getCounter("taskExecutorDisconnectionFailure");
        this.taskExecutorRegistrationCounter = m.getCounter("taskExecutorRegistration");
        this.taskExecutorDisconnectionCounter = m.getCounter("taskExecutorDisconnection");
    }

    @Override
    protected String serviceName() {
        return "ResourceManagerGatewayCxn-" + idx;
    }

    @Override
    protected Scheduler scheduler() {
        return new CustomScheduler() {
            @Override
            protected Schedule getNextSchedule() {
                // no delay on first run.
                return new Schedule(
                    hasRan ? ResourceManagerGatewayCxn.this.heartBeatIntervalDp.getValue() : 0,
                    TimeUnit.MILLISECONDS);
            }
        };
    }

    @Override
    public void startUp() throws Exception {
        try {
            if (!alreadyRegistered.getState()) {
                log.info("Trying to register with resource manager {}", gateway);
                registerTaskExecutorWithRetry();
            } else {
                log.info("Registered with resource manager {} already", gateway);
                // sending heartbeat instead
                runIteration();
            }

            registered = true;
            alreadyRegistered.setState(true);
        } catch (Exception e) {
            // the registration may or may not have succeeded. Since we don't know let's just
            // do the disconnection just to be safe.
            log.error("Registration to gateway {} has failed; Disconnecting now to be safe", gateway,
                    e);
            try {
                disconnectTaskExecutor();
            } catch (Exception ignored) {}
            throw e;
        }
    }

    private void onRegistrationRetry(RetryOnRetryEvent event) {
        log.info("Retrying task executor registration. {}", event);
        taskExecutorRegistrationFailureCounter.increment();
    }

    private void registerTaskExecutorWithRetry() throws Exception {
        final IntervalFunction intervalFn = IntervalFunction.ofExponentialRandomBackoff(registrationRetryInitialDelayMillis, registrationRetryMultiplier, registrationRetryRandomizationFactor);
        final RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(registrationRetryMaxAttempts)
                .intervalFunction(intervalFn)
                .build();
        final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        final Retry retry = retryRegistry.retry("ResourceManagerGatewayCxn:registerTaskExecutor", retryConfig);
        retry.getEventPublisher().onRetry(this::onRegistrationRetry);

        try {
            Retry.decorateCheckedSupplier(retry, this::registerTaskExecutor).apply();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private Ack registerTaskExecutor() throws ExecutionException, InterruptedException, TimeoutException {
        taskExecutorRegistrationCounter.increment();
        return gateway
                .registerTaskExecutor(taskExecutorRegistration)
                .get(heartBeatTimeoutDp.getValue(), TimeUnit.MILLISECONDS);
    }

    private void disconnectTaskExecutor() throws ExecutionException, InterruptedException, TimeoutException {
        taskExecutorDisconnectionCounter.increment();
        try {
            gateway.disconnectTaskExecutor(
                            new TaskExecutorDisconnection(taskExecutorRegistration.getTaskExecutorID(),
                                    taskExecutorRegistration.getClusterID()))
                    .get(2 * heartBeatTimeoutDp.getValue(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Disconnection has failed", e);
            taskExecutorDisconnectionFailureCounter.increment();
            throw e;
        }
    }

    protected void runIteration() throws Exception {
        try {
            if (!hasRan) {
                hasRan = true;
            }
            taskExecutor.getCurrentReport()
                    .thenComposeAsync(report -> {
                        log.debug("Sending heartbeat to resource manager {} with report {}", gateway, report);
                        return gateway.heartBeatFromTaskExecutor(new TaskExecutorHeartbeat(taskExecutorRegistration.getTaskExecutorID(), taskExecutorRegistration.getClusterID(), report));
                    })
                    .get(heartBeatTimeoutDp.getValue(), TimeUnit.MILLISECONDS);

            // the heartbeat was successful, let's reset the counter and set the registered flag
            registered = true;
        } catch (TimeoutException e) {
            heartbeatTimeoutCounter.increment();
            handleHeartbeatFailure(e);
            throw e;
        } catch (Exception e) {
            heartbeatFailureCounter.increment();
            handleHeartbeatFailure(e);
            throw e;
        }
    }

    private void handleHeartbeatFailure(Exception e) throws Exception {
        log.error("Failed to send heartbeat to gateway {}", gateway, e);
       // if there are no more retries then clear the registered flag
        if (getRetryCount() >= tolerableConsecutiveHeartbeatFailures) {
            registered = false;
        } else {
            log.info("Ignoring heartbeat failure to gateway {} due to failed heartbeats {} <= {}",
                    gateway, getRetryCount(), tolerableConsecutiveHeartbeatFailures);
        }
    }

    @Override
    public void shutDown() throws Exception {
        registered = false;
        disconnectTaskExecutor();
    }
}
