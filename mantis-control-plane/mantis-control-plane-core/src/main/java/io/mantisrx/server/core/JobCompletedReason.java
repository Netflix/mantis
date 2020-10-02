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

package io.mantisrx.server.core;

/**
 * Enum for the various types of Job Completions.
 */
public enum JobCompletedReason {

    /**
     * Job completed normally due to SLA enforcement or runtime limit.
     */
    Normal,
    /**
     * Job completed due to an abnormal condition.
     */
    Error,
    /**
     * Does not apply to Jobs.
     */
    Lost,
    /**
     * Job was explicitly killed.
     */
    Killed,
    /**
     * Unused.
     */
    Relaunched
}
