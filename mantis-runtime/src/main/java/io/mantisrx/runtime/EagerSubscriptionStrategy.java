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

package io.mantisrx.runtime;

/**
 * Defines the strategy for when perpetual jobs should start eager subscription
 * to begin data processing.
 */
public enum EagerSubscriptionStrategy {
    /**
     * Start eager subscription immediately when job starts.
     * This is the default behavior and maintains backward compatibility.
     * Data processing begins immediately regardless of whether clients are connected.
     */
    IMMEDIATE,

    /**
     * Wait for the first client connection before starting eager subscription.
     * Job will remain idle (no data processing) until the first SSE client connects.
     * Once activated, job becomes perpetual and survives client disconnections.
     */
    ON_FIRST_CLIENT,

    /**
     * Wait for the first client connection OR a timeout (whichever comes first).
     * Uses the existing subscriptionTimeoutSecs parameter for timeout duration.
     * If no client connects within the timeout, eager subscription starts anyway.
     * Provides a balance between client responsiveness and guaranteed job progress.
     */
    TIMEOUT_BASED
}