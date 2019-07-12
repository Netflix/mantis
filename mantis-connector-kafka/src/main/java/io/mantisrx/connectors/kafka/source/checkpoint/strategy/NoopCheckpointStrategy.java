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

package io.mantisrx.connectors.kafka.source.checkpoint.strategy;

import org.apache.kafka.common.TopicPartition;

import io.mantisrx.runtime.Context;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class NoopCheckpointStrategy implements CheckpointStrategy<Void> {

    @Override
    public void init(Map<String, String> properties) {
    }

    @Override
    public boolean persistCheckpoint(Map<TopicPartition, Void> checkpoint) {
        return true;
    }

    @Override
    public Optional<Void> loadCheckpoint(TopicPartition tp) {
        return Optional.empty();
    }

    @Override
    public void init(Context context) {
        // no-op
    }

    @Override
    public Map<TopicPartition, Optional<Void>> loadCheckpoints(
        List<TopicPartition> tpList) {
        return Collections.emptyMap();
    }

    @Override
    public String type() {
        return CheckpointStrategyOptions.NONE;
    }
}
