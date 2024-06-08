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

package io.mantisrx.server.worker.jobmaster;

import java.util.Properties;

/**
 * Failover status client interface to get the failover status of current region
 */
public interface JobAutoscalerManager {

    JobAutoscalerManager DEFAULT = new NoopJobAutoscalerManager();

    /**
     * Knobs to toggle autoscaling scale ups
     */
    default boolean isScaleUpEnabled() {
        return true;
    }

    /**
     * Knobs to toggle autoscaling scale downs
     */
    default boolean isScaleDownEnabled() {
        return true;
    }

    default double getCurrentValue() {
        return -1.0;
    }

    /**
     * Noop implementation of {@link JobAutoscalerManager} that always returns true
     * for isScaleUpEnabled, isScaleDownEnabled
     */
    class NoopJobAutoscalerManager implements JobAutoscalerManager {

        private NoopJobAutoscalerManager() {
        }

        @SuppressWarnings("unused")
        public static JobAutoscalerManager valueOf(Properties properties) {
            return DEFAULT;
        }
    }
}
