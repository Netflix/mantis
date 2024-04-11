/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.server.master;

/**
 * Failover status client interface to get the failover status of current region
 */
public interface FailoverStatusClient {

    FailoverStatusClient DEFAULT = new NoopFailoverStatusClient();

    /**
     * Get the failover status of current region
     */
    FailoverStatus getFailoverStatus();

    enum FailoverStatus {
        /**
         * Normal region
         */
        NORMAL,
        /**
         * Region is evacuating (failed over)
         */
        REGION_EVACUEE,
        /**
         * Region is savior (taking over traffic for failed over region)
         */
        REGION_SAVIOR;
    }

    /**
     * Noop implementation of {@link FailoverStatusClient} that always returns {@link FailoverStatus#NORMAL}
     */
    class NoopFailoverStatusClient implements FailoverStatusClient {

        @Override
        public FailoverStatus getFailoverStatus() {
            return FailoverStatus.NORMAL;
        }
    }
}
