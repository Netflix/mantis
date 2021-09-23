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
///*
// * Copyright 2019 Netflix, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.mantisrx.server.master.heartbeathandlers;
//
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//
//import io.mantisrx.common.metrics.Counter;
//import io.mantisrx.common.metrics.Metrics;
//import io.mantisrx.common.metrics.MetricsRegistry;
//import io.mantisrx.runtime.MantisJobDurationType;
//import io.mantisrx.server.master.MantisJobMgr;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import rx.Observer;
//
//
//class SubscriptionStateHandler implements PayloadExecutor {
//
//    private static final Logger logger = LoggerFactory.getLogger(SubscriptionStateHandler.class);
//    private final AtomicReference<ScheduledFuture> timedOutExitFutureRef = new AtomicReference<>();
//    private final Counter numEphemeralJobTerminated;
//    private final String jobId;
//
//    SubscriptionStateHandler(String jobId, MantisJobMgr jobMgr) {
//        this.jobId = jobId;
//        Metrics m = new Metrics.Builder()
//                .name(SubscriptionStateHandler.class.getName())
//                .addCounter("numEphemeralJobTerminated")
//                .build();
//        m = MetricsRegistry.getInstance().registerAndGet(m);
//        numEphemeralJobTerminated = m.getCounter("numEphemeralJobTerminated");
//    }
//
//    public Observer<HeartbeatPayloadHandler.Data> call() {
//        return new Observer<HeartbeatPayloadHandler.Data>() {
//            @Override
//            public void onCompleted() {
//                ScheduledFuture prevFutureKill = timedOutExitFutureRef.getAndSet(null);
//                if (prevFutureKill != null && !prevFutureKill.isCancelled() && !prevFutureKill.isDone()) {
//                    logger.info("Cancelled future kill upon active completion of ephemeral job " + jobId);
//                    prevFutureKill.cancel(false);
//                }
//            }
//
//            @Override
//            public void onError(Throwable e) {
//                logger.error("Unexpected error " + e.getMessage(), e);
//            }
//
//            @Override
//            public void onNext(final HeartbeatPayloadHandler.Data data) {
//                if (data.getJobMgr() == null || data.getJobMgr().getJobMetadata() == null ||
//                        data.getJobMgr().getJobMetadata().getSla().getDurationType() != MantisJobDurationType.Transient)
//                    return;
//                Boolean subscribed = Boolean.valueOf(data.getPayload().getData());
//                logger.info(jobId + ": Handling ephemeral job subscriptions, subscribed=" + subscribed);
//                if (!subscribed) {
//                    // ephemeral job with no subscribers, consider for reaping
//                    final long subscriberTimeoutSecs = data.getJobMgr().evalSubscriberTimeoutSecs();
//                    if (timedOutExitFutureRef.get() == null) {
//                        ScheduledFuture<?> futureKill = new ScheduledThreadPoolExecutor(1).schedule(new Runnable() {
//                            @Override
//                            public void run() {
//                                numEphemeralJobTerminated.increment();
//                                timedOutExitFutureRef.set(null);
//                                data.getJobMgr().handleSubscriberTimeout();
//                            }
//                        }, subscriberTimeoutSecs, TimeUnit.SECONDS);
//                        if (!timedOutExitFutureRef.compareAndSet(null, futureKill))
//                            futureKill.cancel(false);
//                        else
//                            logger.info("Setup future job kill (in " + subscriberTimeoutSecs +
//                                    " secs) upon no subscribers for ephemeral job " + jobId);
//                    }
//                } else {
//                    ScheduledFuture prevFutureKill = timedOutExitFutureRef.getAndSet(null);
//                    if (prevFutureKill != null && !prevFutureKill.isCancelled() && !prevFutureKill.isDone()) {
//                        logger.info("Cancelled future kill upon active subscriptions of ephemeral job " + jobId);
//                        prevFutureKill.cancel(false);
//                    }
//                }
//            }
//        };
//    }
//
//}
