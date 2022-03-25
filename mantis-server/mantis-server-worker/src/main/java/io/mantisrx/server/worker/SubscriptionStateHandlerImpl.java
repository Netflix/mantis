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

package io.mantisrx.server.worker;

import io.mantisrx.server.master.client.MantisMasterGateway;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

class SubscriptionStateHandlerImpl implements SinkSubscriptionStateHandler {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionStateHandlerImpl.class);
    private final AtomicReference<ScheduledFuture> timedOutExitFutureRef = new AtomicReference<>();
    private final String jobId;
    private final MantisMasterGateway masterClientApi;
    private ScheduledThreadPoolExecutor executor;
    private final long subscriptionTimeoutSecs;
    private final long minRuntimeSecs;
    private long startedAt = System.currentTimeMillis();

    SubscriptionStateHandlerImpl(String jobId, MantisMasterGateway masterClientApi, long subscriptionTimeoutSecs, long minRuntimeSecs) {
        this.jobId = jobId;
        this.masterClientApi = masterClientApi;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.minRuntimeSecs = minRuntimeSecs;
        executor = this.subscriptionTimeoutSecs > 0L ? new ScheduledThreadPoolExecutor(1) : null;
    }

    @Override
    public synchronized void start() {
        startedAt = System.currentTimeMillis();
        onSinkUnsubscribed(); // start off as unsubscribed
    }

    private long evalSubscriberTimeoutSecs() {
        return Math.max(
                minRuntimeSecs - ((System.currentTimeMillis() - startedAt) / 1000L),
                subscriptionTimeoutSecs
        );
    }

    @Override
    public synchronized void onSinkUnsubscribed() {
        if (executor == null)
            return;
        if (timedOutExitFutureRef.get() == null) {
            timedOutExitFutureRef.set(
                    executor.schedule(
                            () -> {
                                final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
                                while (true) {
                                    logger.info("Calling master to kill due to subscription timeout");
                                    final Subscription subscription = masterClientApi.killJob(jobId, "MantisWorker", "No subscriptions for " +
                                            subscriptionTimeoutSecs + " secs")
                                            .subscribe();
                                    // wait for kill to happen, we won't be running if it succeeds
                                    try {Thread.sleep(60000);} // arbitrary sleep before we retry the kill
                                    catch (InterruptedException ie) {
                                        logger.info("Interrupted while waiting to kill job upon timeout, cancelling");
                                        return;
                                    }
                                    subscription.unsubscribe(); // unsubscribe from previous one before retrying
                                }
                            },
                            evalSubscriberTimeoutSecs(), TimeUnit.SECONDS
                    )
            );
            logger.info("Setup future job kill (in " + subscriptionTimeoutSecs +
                    " secs) upon no subscribers for ephemeral job " + jobId);
        }
    }

    @Override
    public synchronized void onSinkSubscribed() {
        if (executor == null) {
            return;
        }
        final ScheduledFuture prevFutureKill = timedOutExitFutureRef.getAndSet(null);
        if (prevFutureKill != null && !prevFutureKill.isCancelled() && !prevFutureKill.isDone()) {
            logger.info("Cancelled future kill upon active subscriptions of ephemeral job " + jobId);
            prevFutureKill.cancel(true);
        }
    }

    @Override
    public synchronized void shutdown() {
        // we need to clean up the resources. onSinkSubscribed works in the same fashion.
        // let's call that instead.
        onSinkSubscribed();
        // let's shutdown the executor and reset it.
        executor.shutdown();
        executor = null;
    }

    @Override
    public void enterActiveMode() {

    }
}
