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

package io.mantisrx.connector.kafka.source.checkpoint.strategy;

import org.apache.kafka.common.TopicPartition;

import io.mantisrx.runtime.Context;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface CheckpointStrategy<S> {

    /**
     * initialization when creating the strategy.
     */
    void init(Context context);

    /**
     * initialization when creating the strategy.
     */
    void init(Map<String, String> initParams);

    /**
     * persist checkpoint state by TopicPartition.
     *
     * @param checkpoint
     * @return true on persist success, false otherwise
     */
    boolean persistCheckpoint(Map<TopicPartition, S> checkpoint);

    /**
     * return the persisted checkpoint state for topic-partition (if exists).
     *
     * @param tp topic-partition
     *
     * @return CheckpointState if persisted, else empty Optional
     */
    Optional<S> loadCheckpoint(TopicPartition tp);


    /**
     * Bulk API to Load checkpoints.
     *
     * @param tpList list of TopicPartitions to load checkpointState
     * @return
     */
    Map<TopicPartition, Optional<S>> loadCheckpoints(List<TopicPartition> tpList);

    /**
     * Get checkpoint strategy type, one of {@link CheckpointStrategyOptions}
     * @return {@link CheckpointStrategyOptions checkpointStrategy} implemented
     */
    String type();
}
