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

package io.mantisrx.publish.netty.transmitters;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.netty.pipeline.HttpEventChannel;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;


/**
 * Maintains a set of active, usable {@link MantisWorker}s represented by a key-value mapping of
 * {@code key: MantisWorker, value: error count}.
 * <p>
 * Capacity:
 * <p>
 * This pool maintains a working set of pool up to a configurable capacity. Upon {@link #refresh(List, boolean)},
 * it will update its working set.
 * <p>
 * Blacklist:
 * <p>
 * Callers can use this class to record worker failures for an action.
 * Once a worker has reached a configurable number of failures, it will be blacklisted.
 * The caller can then decide whether or not to continue using that worker.
 * <p>
 * Workers will be removed from the blacklist by having their error counts reset after a configurable amount of time.
 * This is to prevent a worker from indefinitely being blacklisted.
 */
class ChoiceOfTwoWorkerPool {

    private final Registry registry;

    private final AtomicDouble workerPoolGauge;
    private final AtomicDouble blacklistedWorkersGauge;

    private final int capacity;
    private final int errorQuota;
    private final int errorTimeoutSec;
    private final int refreshIntervalSec;
    private final ConcurrentMap<MantisWorker, Integer> pool;
    /**
     * Backed by {@link ConcurrentHashMap.KeySetView}.
     */
    private final Set<MantisWorker> blacklist;
    private final EventChannel eventChannel;

    private AtomicLong lastFetchMs;
    private AtomicLong lastBlacklistRefreshMs;

    ChoiceOfTwoWorkerPool(MrePublishConfiguration config, Registry registry, EventChannel eventChannel) {
        this.registry = registry;

        this.workerPoolGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "workerPool", "channel", HttpEventChannel.CHANNEL_TYPE);
        this.blacklistedWorkersGauge = SpectatorUtils.buildAndRegisterGauge(
                registry, "blacklistedWorkers", "channel", HttpEventChannel.CHANNEL_TYPE);

        this.capacity = config.getWorkerPoolCapacity();
        this.errorQuota = config.getWorkerPoolWorkerErrorQuota();
        this.errorTimeoutSec = config.getWorkerPoolWorkerErrorTimeoutSec();
        this.refreshIntervalSec = config.getWorkerPoolRefreshIntervalSec();
        this.pool = new ConcurrentHashMap<>(config.getWorkerPoolCapacity());
        this.blacklist = ConcurrentHashMap.newKeySet();
        this.eventChannel = eventChannel;

        this.lastFetchMs = new AtomicLong(0);
        this.lastBlacklistRefreshMs = new AtomicLong(0);
    }

    /**
     * Refreshes this pool of {@link MantisWorker}s by checking for staleness and if workers should be
     * removed from the blacklist after a configurable amount of time.
     * <p>
     * Worker Replacement:
     * <p>
     * 1. If the fresh set of workers is the same or has new workers, then do nothing.
     * 2. If a worker in the pool doesn't exist in the fresh set of workers, then replace it.
     * 3. If a worker in the pool is blacklisted, then replace it.
     * <p>
     * If {@code force = false}, this method will try to refresh the pool even if the pool is full.
     * It does this in case a worker in the pool no longer exists according to the fresh set of workers.
     * This may happen in horizontal scaling situations.
     * <p>
     * Blacklist and Worker Error Timeout:
     * <p>
     * If workers error more than the allowed quota, they will be placed into the blacklist. Blacklisted workers will
     * remain blacklisted for a timeout period, after which they may be again considered for selection.
     *
     * @param freshWorkers a list of pool to potentially refresh the pool.
     * @param force        if {@code true}, clear out the entire pool and add new pool up to the capacity;
     *                     if {@code false}, replace pool using the worker replacement strategy.
     */
    void refresh(List<MantisWorker> freshWorkers, boolean force) {
        if (!shouldRefresh(lastFetchMs.get(), refreshIntervalSec * 1000)) {
            return;
        }

        if (force) {
            pool.clear();
            workerPoolGauge.set((double) pool.size());
        }

        if (shouldRefresh(lastBlacklistRefreshMs.get(), errorTimeoutSec * 1000)) {
            blacklist.clear();
            blacklistedWorkersGauge.set((double) blacklist.size());
            lastBlacklistRefreshMs.set(registry.clock().wallTime());
        }

        Set<MantisWorker> staleWorkers = new HashSet<>(pool.keySet());
        Set<MantisWorker> diff = new HashSet<>(staleWorkers);

        // Keep stale workers that exist in the fresh set.
        staleWorkers.retainAll(freshWorkers);
        diff.removeAll(staleWorkers);
        // Remove worker from the pool. No need to explicitly close the underlying Netty channel because
        // it would have already been closed by the attached CloseFutureListener in the HttpEventChannelManager.
        diff.forEach(pool::remove);

        // Exclude stale workers from candidate consideration.
        freshWorkers.removeAll(diff);
        Iterator<MantisWorker> candidates = freshWorkers.iterator();
        while (candidates.hasNext() && pool.size() < capacity) {
            MantisWorker candidate = candidates.next();
            if (!blacklist.contains(candidate)) {
                pool.put(candidate, 0);
                workerPoolGauge.set((double) pool.size());
            }
        }

        lastFetchMs.set(registry.clock().wallTime());
    }

    /**
     * Refreshes this pool of {@link MantisWorker}s without a full replacement.
     *
     * @param workers a list of pool to potentially refresh the pool.
     */
    void refresh(List<MantisWorker> workers) {
        refresh(workers, false);
    }

    private boolean shouldRefresh(long timestamp, long interval) {
        return registry.clock().wallTime() - timestamp > interval;
    }

    /**
     * Runs the {@link BiFunction}, checks for failures, and increments error count for a {@link MantisWorker}.
     */
    CompletableFuture<Void> record(
            Event event,
            BiFunction<MantisWorker, Event, CompletableFuture<Void>> function)
            throws NonRetryableException {
        MantisWorker worker = getRandomWorker();

        if (worker == null) {
            throw new NonRetryableException("no available workers in pool");
        }

        CompletableFuture<Void> future = function.apply(worker, event);
        // RetryableException and NonRetryableException are generally what would be thrown.
        future.whenCompleteAsync((v, t) -> {
            if (t != null) {
                // Increment error count for this specific worker.
                pool.put(worker, pool.get(worker) + 1);
                if (shouldBlacklist(worker)) {
                    // Immediately close Netty channel and remove blacklisted worker from pool.
                    eventChannel.close(worker);
                    pool.remove(worker);
                    workerPoolGauge.set((double) pool.size());
                    blacklist.add(worker);
                    blacklistedWorkersGauge.set((double) blacklist.size());
                }
            }
        });

        return future;
    }

    /**
     * Determines whether or not a worker should be blacklisted.
     */
    private boolean shouldBlacklist(MantisWorker worker) {
        return pool.getOrDefault(worker, 0) > errorQuota;
    }

    /**
     * Determines whether or not a worker in the pool is over the pool's configured per-worker error quota.
     */
    boolean isBlacklisted(MantisWorker worker) {
        return blacklist.contains(worker);
    }

    /**
     * Returns a random choice-of-two {@link MantisWorker} from the pool.
     */
    MantisWorker getRandomWorker() {
        int poolSize = pool.size();

        if (poolSize == 0) {
            return null;
        } else if (poolSize == 1) {
            return (MantisWorker) pool.keySet().toArray()[0];
        } else {
            List<MantisWorker> candidates = new ArrayList<>(pool.keySet());

            int randomIndex1 = ThreadLocalRandom.current().nextInt(pool.size());
            int randomIndex2 = ThreadLocalRandom.current().nextInt(pool.size());
            MantisWorker candidate1 = candidates.get(randomIndex1);
            MantisWorker candidate2 = candidates.get(randomIndex2);
            double candidate1Score = getWorkerScore(candidate1);
            double candidate2Score = getWorkerScore(candidate2);

            return candidate1Score <= candidate2Score ? candidate1 : candidate2;
        }
    }

    /**
     * Returns the number of errors for a given worker in the pool.
     */
    int getWorkerErrors(MantisWorker worker) {
        return pool.getOrDefault(worker, 0);
    }

    /**
     * Calculate a worker score. Lower is better.
     * <p>
     * Currently, the only factor is the {@link Channel}'s buffer size.
     */
    private double getWorkerScore(MantisWorker worker) {
        return eventChannel.bufferSize(worker);
    }

    /**
     * Returns the number of {@link MantisWorker}s currently in the pool.
     */
    int size() {
        return pool.size();
    }

    /**
     * Returns the total number of {@link MantisWorker} that can be cached by this pool.
     */
    int capacity() {
        return capacity;
    }
}
