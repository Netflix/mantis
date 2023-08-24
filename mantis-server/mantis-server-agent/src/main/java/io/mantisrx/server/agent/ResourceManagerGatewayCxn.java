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
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.server.agent.utils.ExponentialBackoffAbstractScheduledService;
import io.mantisrx.server.master.resourcecluster.ResourceClusterGateway;
import io.mantisrx.server.master.resourcecluster.TaskExecutorDisconnection;
import io.mantisrx.server.master.resourcecluster.TaskExecutorHeartbeat;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import io.mantisrx.server.master.resourcecluster.TaskExecutorReport;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.time.Time;

@Slf4j
@ToString(of = "gateway")
class ResourceManagerGatewayCxn extends ExponentialBackoffAbstractScheduledService {

    private final int idx;
    private final TaskExecutorRegistration taskExecutorRegistration;
    @Getter
    private final ResourceClusterGateway gateway;
    private final Time heartBeatInterval;
    private final Time heartBeatTimeout;
    private final Time timeout = Time.of(1000, TimeUnit.MILLISECONDS);
    private final long registrationRetryInitialDelayMillis;
    private final double registrationRetryMultiplier;
    private final double registrationRetryRandomizationFactor;
    private final int registrationRetryMaxAttempts;
    private final Function<Time, CompletableFuture<TaskExecutorReport>> currentReportSupplier;
    // flag representing if the task executor has been registered with the resource manager
    @Getter
    private volatile boolean registered = false;

    private final Counter heartbeatTimeoutCounter;
    private final Counter heartbeatFailureCounter;
    private final Counter taskExecutorRegistrationFailureCounter;
    private final Counter taskExecutorDisconnectionFailureCounter;

    ResourceManagerGatewayCxn(int idx, TaskExecutorRegistration taskExecutorRegistration, ResourceClusterGateway gateway, Time heartBeatInterval, Time heartBeatTimeout, Function<Time, CompletableFuture<TaskExecutorReport>> currentReportSupplier, int tolerableConsecutiveHeartbeatFailures, long heartbeatRetryInitialDelayMillis, long heartbeatRetryMaxDelayMillis, long registrationRetryInitialDelayMillis, double registrationRetryMultiplier, double registrationRetryRandomizationFactor, int registrationRetryMaxAttempts) {
        super(tolerableConsecutiveHeartbeatFailures, heartbeatRetryInitialDelayMillis, heartbeatRetryMaxDelayMillis);
        this.idx = idx;
        this.taskExecutorRegistration = taskExecutorRegistration;
        this.gateway = gateway;
        this.heartBeatInterval = heartBeatInterval;
        this.heartBeatTimeout = heartBeatTimeout;
        this.currentReportSupplier = currentReportSupplier;
        this.registrationRetryInitialDelayMillis = registrationRetryInitialDelayMillis;
        this.registrationRetryMultiplier = registrationRetryMultiplier;
        this.registrationRetryRandomizationFactor = registrationRetryRandomizationFactor;
        this.registrationRetryMaxAttempts = registrationRetryMaxAttempts;

        final MetricGroupId teGroupId = new MetricGroupId("TaskExecutor");
        Metrics m = new Metrics.Builder()
                .id(teGroupId)
                .addCounter("heartbeatTimeout")
                .addCounter("heartbeatFailure")
                .addCounter("taskExecutorRegistrationFailure")
                .addCounter("taskExecutorDisconnectionFailure")
                .build();
        this.heartbeatTimeoutCounter = m.getCounter("heartbeatTimeout");
        this.heartbeatFailureCounter = m.getCounter("heartbeatFailure");
        this.taskExecutorRegistrationFailureCounter = m.getCounter("taskExecutorRegistrationFailure");
        this.taskExecutorDisconnectionFailureCounter = m.getCounter("taskExecutorDisconnectionFailure");

    }

    @Override
    protected String serviceName() {
        return "ResourceManagerGatewayCxn-" + idx;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(
                0,
                heartBeatInterval.getSize(),
                heartBeatInterval.getUnit());
    }

    @Override
    public void startUp() throws Exception {
        log.info("Trying to register with resource manager {}", gateway);
        try {
            registerTaskExecutorWithRetry();
            registered = true;
        } catch (Exception e) {
            // the registration may or may not have succeeded. Since we don't know let's just
            // do the disconnection just to be safe.
            log.error("Registration to gateway {} has failed; Disconnecting now to be safe", gateway,
                    e);
            try {
                gateway.disconnectTaskExecutor(
                                new TaskExecutorDisconnection(taskExecutorRegistration.getTaskExecutorID(),
                                        taskExecutorRegistration.getClusterID()))
                        .get(2 * heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());
            } catch (Exception inner) {
                log.error("Disconnection has also failed", inner);
                taskExecutorDisconnectionFailureCounter.increment();
            }
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
            Retry.decorateCheckedSupplier(retry, () ->
                    gateway
                            .registerTaskExecutor(taskExecutorRegistration)
                            .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit())).apply();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void runIteration() throws Exception {
        try {
            currentReportSupplier.apply(timeout)
                    .thenComposeAsync(report -> {
                        log.debug("Sending heartbeat to resource manager {} with report {}", gateway, report);
                        return gateway.heartBeatFromTaskExecutor(new TaskExecutorHeartbeat(taskExecutorRegistration.getTaskExecutorID(), taskExecutorRegistration.getClusterID(), report));
                    })
                    .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());

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
        if (noMoreRetryLeft()) {
            registered = false;
        } else {
            log.info("Ignoring heartbeat failure to gateway {} due to failed heartbeats {} <= {}",
                    gateway, getRetryCount(), getMaxRetryCount());
        }
    }

    @Override
    public void shutDown() throws Exception {
        registered = false;
        gateway
                .disconnectTaskExecutor(
                        new TaskExecutorDisconnection(taskExecutorRegistration.getTaskExecutorID(),
                                taskExecutorRegistration.getClusterID()))
                .get(heartBeatTimeout.getSize(), heartBeatTimeout.getUnit());
    }
}
