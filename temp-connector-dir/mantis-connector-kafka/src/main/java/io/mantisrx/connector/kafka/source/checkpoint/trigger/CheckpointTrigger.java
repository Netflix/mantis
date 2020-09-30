/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.kafka.source.checkpoint.trigger;

public interface CheckpointTrigger {

    /**
     * true indicates checkpoint now, else don't.
     *
     * @return
     */
    boolean shouldCheckpoint();

    /**
     * update internal state based on provided count (typically current message size).
     *
     * @param count
     */
    void update(int count);

    /**
     * hook to reset all internal state after a checkpoint is persisted.
     */
    void reset();

    /**
     * true indicates the trigger is in active and valid state.
     */
    boolean isActive();

    /**
     * cleanup resources.
     */
    void shutdown();
}
