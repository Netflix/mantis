/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Util;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class JobDiscoveryService {

    public enum LookupType {
        JOB_CLUSTER,
        JOB_ID
    }

    public class JobDiscoveryLookupKey {

        private final LookupType lookupType;
        private final String id;

        public JobDiscoveryLookupKey(final LookupType lookupType, final String id) {
            this.lookupType = lookupType;
            this.id = id;
        }

        public LookupType getLookupType() {
            return lookupType;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final JobDiscoveryLookupKey that = (JobDiscoveryLookupKey) o;
            return lookupType == that.lookupType &&
                    Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {

            return Objects.hash(lookupType, id);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("JobDiscoveryLookupKey{");
            sb.append("lookupType=").append(lookupType);
            sb.append(", id='").append(id).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * The Purpose of this class is to dedup multiple schedulingChanges streams for the same JobId.
     * The first subscriber will cause a BehaviorSubject to be setup with data obtained from mantisClient.getSchedulingChanges
     * Future subscribers will simply connect to the same Subject
     * When the no. of subscribers falls to zero the Observable is unsubscribed and a cleanup callback is invoked.
     */

    public class JobSchedulingInfoSubjectHolder implements AutoCloseable {

        private Subscription subscription;
        private final AtomicInteger subscriberCount = new AtomicInteger();
        private final String jobId;
        private final MantisClient mantisClient;
        private AtomicBoolean inited = new AtomicBoolean(false);
        private CountDownLatch initComplete = new CountDownLatch(1);
        private final Action1 doOnZeroConnections;
        private final Subject<JobSchedulingInfo, JobSchedulingInfo> schedulingInfoBehaviorSubjectingSubject = BehaviorSubject.create();
        private final Registry registry;
        private final Scheduler scheduler;

        private final Counter cleanupCounter;
        private final AtomicLong subscriberCountGauge;

        public JobSchedulingInfoSubjectHolder(MantisClient mantisClient, String jobId, Action1 onZeroConnections, Registry registry, Scheduler scheduler) {
            this(mantisClient, jobId, onZeroConnections, 5, registry, scheduler);
        }

        /**
         * Ctor only no subscriptions happen as part of the ctor
         *
         * @param mantisClient      - Used to get the schedulingInfo Observable
         * @param jobId             - JobId of job to get schedulingInfo
         * @param onZeroConnections - Call back when there are no more subscriptions for this observable
         * @param retryCount        - No. of retires in case of error connecting to schedulingInfo
         */
        JobSchedulingInfoSubjectHolder(MantisClient mantisClient,
                                       String jobId,
                                       Action1 onZeroConnections,
                                       int retryCount,
                                       Registry registry,
                                       Scheduler scheduler) {
            Preconditions.checkNotNull(mantisClient, "Mantis Client cannot be null");
            Preconditions.checkNotNull(jobId, "JobId cannot be null");
            Preconditions.checkArgument(!jobId.isEmpty(), "JobId cannot be empty");
            Preconditions.checkNotNull(onZeroConnections, "on Zero Connections callback cannot be null");
            Preconditions.checkArgument(retryCount >= 0, "Retry count cannot be less than 0");
            this.jobId = jobId;
            this.mantisClient = mantisClient;
            this.doOnZeroConnections = onZeroConnections;
            this.registry = registry;
            this.scheduler = scheduler;


            cleanupCounter = SpectatorUtils.newCounter("mantisapi.schedulingChanges.cleanupCount", "", "jobId", jobId);
            subscriberCountGauge = SpectatorUtils.newGauge("mantisapi.schedulingChanges.subscriberCount", "",
                    new AtomicLong(0l), "jobId", jobId);
        }

        /**
         * If invoked the first time it will subscribe to the schedulingInfo Observable via mantisClient and onNext
         * the results to the schedulinginfoSubject
         * If 2 or more threads concurrently invoke this only 1 will do the initialization while others wait.
         */
        private void init() {
            if (!inited.getAndSet(true)) {
                subscription = mantisClient.getSchedulingChanges(jobId)
                        .retryWhen(Util.getRetryFunc(log, "job scheduling information for " + jobId))
                        .doOnError((t) -> {
                            schedulingInfoBehaviorSubjectingSubject.toSerialized().onError(t);
                            doOnZeroConnections.call(jobId);
                        })
                        .doOnCompleted(() -> {
                            schedulingInfoBehaviorSubjectingSubject.toSerialized().onCompleted();
                            doOnZeroConnections.call(jobId);
                        })
                        .subscribeOn(scheduler)
                        .subscribe((schedInfo) -> schedulingInfoBehaviorSubjectingSubject.onNext(schedInfo));
                initComplete.countDown();

            } else {
                try {
                    initComplete.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        }


        /**
         * For testing
         *
         * @return current subscription count
         */
        int getSubscriptionCount() {
            return subscriberCount.get();
        }

        /**
         * If a subject holding schedulingInfo for the job exists return it as an Observable
         * if not then invoke mantisClient to get an Observable of scheduling changes, write them to a Subject
         * and return it as an observable
         * Also keep track of subscription Count, When the subscription count falls to 0 unsubscribe from schedulingInfo Observable
         *
         * @return Observable of scheduling changes
         */
        public Observable<JobSchedulingInfo> getSchedulingChanges() {
            init();
            return schedulingInfoBehaviorSubjectingSubject

                    .doOnSubscribe(() -> {
                        if (log.isDebugEnabled()) { log.debug("Subscribed"); }
                        subscriberCount.incrementAndGet();
                        subscriberCountGauge.set(subscriberCount.get());
                        if (log.isDebugEnabled()) { log.debug("Subscriber count " + subscriberCount.get()); }
                    })
                    .doOnUnsubscribe(() -> {
                        if (log.isDebugEnabled()) {log.debug("UnSubscribed"); }
                        int subscriberCnt = subscriberCount.decrementAndGet();
                        subscriberCountGauge.set(subscriberCount.get());
                        if (log.isDebugEnabled()) { log.debug("Subscriber count " + subscriberCnt); }
                        if (0 == subscriberCount.get()) {
                            if (log.isDebugEnabled()) { log.debug("Shutting down"); }
                            close();

                        }
                    })
                    .doOnError((t) -> close())
                    ;
        }

        /**
         * Invoked If schedulingInfo Observable Completes or throws an onError of if the subscription count falls to 0
         * Unsubscribes from the schedulingInfoObservable and invokes doOnZeroConnection callback
         */

        @Override
        public void close() {
            if (log.isDebugEnabled()) { log.debug("In Close Unsubscribing...." + subscription.isUnsubscribed()); }
            if (inited.get() && subscription != null && !subscription.isUnsubscribed()) {
                if (log.isDebugEnabled()) { log.debug("Unsubscribing...."); }
                subscription.unsubscribe();
                inited.set(false);
                initComplete = new CountDownLatch(1);
            }
            cleanupCounter.increment();
            this.doOnZeroConnections.call(this.jobId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobSchedulingInfoSubjectHolder that = (JobSchedulingInfoSubjectHolder) o;
            return Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {

            return Objects.hash(jobId);
        }
    }


    /**
     * The Purpose of this class is to dedup multiple job discovery info streams for the same JobId.
     * The first subscriber will cause a BehaviorSubject to be setup with data obtained from mantisClient.jobDiscoveryInfoStream
     * Future subscribers will connect to the same Subject
     * When the no. of subscribers falls to zero the Observable is un-subscribed and a cleanup callback is invoked.
     */

    public class JobDiscoveryInfoSubjectHolder implements AutoCloseable {

        private Subscription subscription;
        private final AtomicInteger subscriberCount = new AtomicInteger();
        private final JobDiscoveryLookupKey lookupKey;
        private final MantisClient mantisClient;
        private AtomicBoolean inited = new AtomicBoolean(false);
        private CountDownLatch initComplete = new CountDownLatch(1);
        private final Action1 doOnZeroConnections;
        private final Subject<JobSchedulingInfo, JobSchedulingInfo> discoveryInfoBehaviorSubject = BehaviorSubject.create();
        private final Scheduler scheduler;

        private final Counter cleanupCounter;
        private final AtomicLong subscriberCountGauge;

        public JobDiscoveryInfoSubjectHolder(MantisClient mantisClient, JobDiscoveryLookupKey lookupKey, Action1 onZeroConnections, Scheduler scheduler) {
            this(mantisClient, lookupKey, onZeroConnections, 5, scheduler);
        }

        /**
         * Ctor only no subscriptions happen as part of the ctor
         *
         * @param mantisClient      - Used to get the schedulingInfo Observable
         * @param lookupKey         - JobId or JobCluster to get schedulingInfo
         * @param onZeroConnections - Call back when there are no more subscriptions for this observable
         * @param retryCount        - No. of retires in case of error connecting to schedulingInfo
         */
        JobDiscoveryInfoSubjectHolder(MantisClient mantisClient,
                                      JobDiscoveryLookupKey lookupKey,
                                      Action1 onZeroConnections,
                                      int retryCount,
                                      Scheduler scheduler) {
            Preconditions.checkNotNull(mantisClient, "Mantis Client cannot be null");
            Preconditions.checkNotNull(lookupKey, "lookup key cannot be null");
            Preconditions.checkArgument(lookupKey.getId() != null && !lookupKey.getId().isEmpty(), "lookup key cannot be empty or null");
            Preconditions.checkNotNull(onZeroConnections, "on Zero Connections callback cannot be null");
            Preconditions.checkArgument(retryCount >= 0, "Retry count cannot be less than 0");
            this.lookupKey = lookupKey;
            this.mantisClient = mantisClient;
            this.doOnZeroConnections = onZeroConnections;
            this.scheduler = scheduler;

            cleanupCounter = SpectatorUtils.newCounter("mantisapi.discoveryinfo.cleanupCount", "", "lookupKey", lookupKey.getId());
            subscriberCountGauge = SpectatorUtils.newGauge("mantisapi.discoveryinfo.subscriberCount", "",
                    new AtomicLong(0l),
                    "lookupKey", lookupKey.getId());
        }

        /**
         * If invoked the first time it will subscribe to the schedulingInfo Observable via mantisClient and onNext
         * the results to the schedulinginfoSubject
         * If 2 or more threads concurrently invoke this only 1 will do the initialization while others wait.
         */
        private void init() {
            if (!inited.getAndSet(true)) {
                Observable<JobSchedulingInfo> jobSchedulingInfoObs;
                switch (lookupKey.getLookupType()) {
                    case JOB_ID:
                        jobSchedulingInfoObs = mantisClient.getSchedulingChanges(lookupKey.getId());
                        break;
                    case JOB_CLUSTER:
                        jobSchedulingInfoObs = mantisClient.jobClusterDiscoveryInfoStream(lookupKey.getId());
                        break;
                    default:
                        throw new IllegalArgumentException("lookup key type is not supported " + lookupKey.getLookupType());
                }
                subscription = jobSchedulingInfoObs
                        .retryWhen(Util.getRetryFunc(log, "job scheduling info for (" + lookupKey.getLookupType() + ") " + lookupKey.id))
                        .doOnError((t) -> {
                            log.info("cleanup jobDiscoveryInfo onError for {}", lookupKey);
                            discoveryInfoBehaviorSubject.toSerialized().onError(t);
                            doOnZeroConnections.call(lookupKey);
                        })
                        .doOnCompleted(() -> {
                            log.info("cleanup jobDiscoveryInfo onCompleted for {}", lookupKey);
                            discoveryInfoBehaviorSubject.toSerialized().onCompleted();
                            doOnZeroConnections.call(lookupKey);
                        })
                        .subscribeOn(scheduler)
                        .subscribe((schedInfo) -> discoveryInfoBehaviorSubject.onNext(schedInfo));
                initComplete.countDown();

            } else {
                try {
                    initComplete.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        }


        /**
         * For testing
         *
         * @return current subscription count
         */
        int getSubscriptionCount() {
            return subscriberCount.get();
        }

        /**
         * If a subject holding schedulingInfo for the job exists return it as an Observable
         * if not then invoke mantisClient to get an Observable of scheduling changes, write them to a Subject
         * and return it as an observable
         * Also keep track of subscription Count, When the subscription count falls to 0 unsubscribe from schedulingInfo Observable
         *
         * @return Observable of scheduling changes
         */
        public Observable<JobSchedulingInfo> jobDiscoveryInfoStream() {
            init();
            return discoveryInfoBehaviorSubject
                    .doOnSubscribe(() -> {
                        if (log.isDebugEnabled()) { log.debug("Subscribed"); }
                        subscriberCount.incrementAndGet();
                        subscriberCountGauge.set(subscriberCount.get());
                        if (log.isDebugEnabled()) { log.debug("Subscriber count " + subscriberCount.get()); }
                    })
                    .doOnUnsubscribe(() -> {
                        if (log.isDebugEnabled()) {log.debug("UnSubscribed"); }
                        int subscriberCnt = subscriberCount.decrementAndGet();
                        subscriberCountGauge.set(subscriberCount.get());
                        if (log.isDebugEnabled()) { log.debug("Subscriber count " + subscriberCnt); }
                        if (0 == subscriberCount.get()) {
                            if (log.isDebugEnabled()) { log.debug("Shutting down"); }
                            close();

                        }
                    })
                    .doOnError((t) -> close())
                    ;
        }

        /**
         * Invoked If schedulingInfo Observable Completes or throws an onError of if the subscription count falls to 0
         * Unsubscribes from the schedulingInfoObservable and invokes doOnZeroConnection callback
         */

        @Override
        public void close() {
            if (log.isDebugEnabled()) { log.debug("In Close un-subscribing...." + subscription.isUnsubscribed()); }
            if (inited.get() && subscription != null && !subscription.isUnsubscribed()) {
                if (log.isDebugEnabled()) { log.debug("Unsubscribing...."); }
                subscription.unsubscribe();
                inited.set(false);
                initComplete = new CountDownLatch(1);
            }
            cleanupCounter.increment();
            log.info("jobDiscoveryInfo close for {}", lookupKey);
            this.doOnZeroConnections.call(this.lookupKey);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobDiscoveryInfoSubjectHolder that = (JobDiscoveryInfoSubjectHolder) o;
            return Objects.equals(lookupKey, that.lookupKey);
        }

        @Override
        public int hashCode() {

            return Objects.hash(lookupKey);
        }
    }


    private final MantisClient mantisClient;
    private final Scheduler scheduler;
    private final AtomicDouble subjectMapSizeGauge;


    private int retryCount = 5;
    private static JobDiscoveryService INSTANCE = null;

    public static synchronized JobDiscoveryService getInstance(MantisClient mantisClient, Scheduler scheduler) {
        if (INSTANCE == null) {
            INSTANCE = new JobDiscoveryService(mantisClient, scheduler);
        }
        return INSTANCE;
    }

    private JobDiscoveryService(final MantisClient mClient, Scheduler scheduler) {
        Preconditions.checkNotNull(mClient, "mantisClient cannot be null");
        this.mantisClient = mClient;
        this.subjectMapSizeGauge = SpectatorUtils.newGauge("mantisapi.discoveryInfo.subjectMapSize", "mantisapi.discoveryInfo.subjectMapSize", new AtomicDouble(0.0));
        this.scheduler = scheduler;
    }

    /**
     * For testing purposes
     *
     * @param cnt No of retries
     */
    @VisibleForTesting
    void setRetryCount(int cnt) {
        this.retryCount = cnt;
    }

    private final ConcurrentMap<JobDiscoveryLookupKey, JobDiscoveryInfoSubjectHolder> subjectMap = new ConcurrentHashMap<>();

    /**
     * Invoked by the subjectHolders when the subscription count goes to 0 (or if there is an error)
     */
    private final Action1<JobDiscoveryLookupKey> removeSubjectAction = key -> {
        if (log.isDebugEnabled()) { log.info("Removing subject for key {}", key.toString()); }
        removeSchedulingInfoSubject(key);
    };

    /**
     * Atomically inserts a JobDiscoveryInfoSubjectHolder if absent and returns an Observable of JobSchedulingInfo to the caller
     *
     * @param lookupKey - Job cluster name or JobID
     *
     * @return
     */

    public Observable<JobSchedulingInfo> jobDiscoveryInfoStream(JobDiscoveryLookupKey lookupKey) {
        Preconditions.checkNotNull(lookupKey, "lookup key cannot be null for fetching job discovery info");
        Preconditions.checkArgument(lookupKey.getId() != null && !lookupKey.getId().isEmpty(), "Lookup ID cannot be null or empty" + lookupKey);
        subjectMapSizeGauge.set(subjectMap.size());
        return subjectMap.computeIfAbsent(lookupKey, (jc) -> new JobDiscoveryInfoSubjectHolder(mantisClient, jc, removeSubjectAction, this.retryCount, scheduler)).jobDiscoveryInfoStream();
    }

    /**
     * Intended to be called via a callback when subscriber count falls to 0
     *
     * @param lookupKey JobId whose entry needs to be removed
     */
    private void removeSchedulingInfoSubject(JobDiscoveryLookupKey lookupKey) {
        subjectMap.remove(lookupKey);
        subjectMapSizeGauge.set(subjectMap.size());
    }

    /**
     * For testing purposes
     *
     * @return No. of entries in the subject
     */
    int getSubjectMapSize() {
        return subjectMap.size();
    }

    /**
     * For testing purposes
     */
    void clearMap() {
        subjectMap.clear();
    }

    public JobDiscoveryLookupKey key(LookupType lookupType, String jobCluster) {
        return new JobDiscoveryLookupKey(lookupType, jobCluster);
    }

    public static final Cache<String, String> jobDiscoveryInfoCache = CacheBuilder.newBuilder()
            .expireAfterWrite(250, TimeUnit.MILLISECONDS)
            .maximumSize(500)
            .build();
}
