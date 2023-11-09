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

package io.mantisrx.server.agent.utils;

import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractScheduledService;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ExponentialBackoffAbstractScheduledService extends AbstractScheduledService {

    private final long initialDelayMillis;
    private final long maxDelayMillis;

    private int retryCount = 0;
    private long nextRunTime = 0;

    protected ExponentialBackoffAbstractScheduledService(long initialDelayMillis,
        long maxDelayMillis) {
        this.initialDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    protected abstract void runIteration() throws Exception;

    @Override
    protected void runOneIteration() {
        if (!isTimeForNextRun()) {
            log.debug("Skipping runIteration due to retry delay. Next run after: {}", nextRunTime);
            return;
        }
        runNow();
    }

    private void runNow() {
        try {
            runIteration();
            resetRetryCount();
        } catch (Exception e) {
            onFailure();
        }
    }

    private boolean isTimeForNextRun() {
        return System.currentTimeMillis() >= nextRunTime;
    }

    private void onFailure() {
        // Reschedule task with backoff
        retryCount++;
        long delay = (long) Math.min(initialDelayMillis * Math.pow(2, Math.max(20, retryCount)),
            maxDelayMillis);
        long jitter = ThreadLocalRandom.current().nextLong(delay / 2);
        nextRunTime = System.currentTimeMillis() + delay + jitter;
    }

    private void resetRetryCount() {
        retryCount = 0;
        nextRunTime = 0;
    }

    protected int getRetryCount() {
        return retryCount;
    }
}
