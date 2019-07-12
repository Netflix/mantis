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

package io.mantisrx.connectors.kafka.source.checkpoint.trigger;

import io.mantisrx.connectors.kafka.source.MantisKafkaSourceConfig;
import io.mantisrx.connectors.kafka.source.checkpoint.strategy.CheckpointStrategyOptions;


public final class CheckpointTriggerFactory {

    private CheckpointTriggerFactory() { }

    /**
     * Factory method to create instance of {@link CheckpointTrigger}
     * @param kafkaSourceConfig mantis kafka source configuration
     * @return {@link CheckpointTrigger} instance based on config
     */
    public static CheckpointTrigger getNewInstance(final MantisKafkaSourceConfig kafkaSourceConfig) {
        switch (kafkaSourceConfig.getCheckpointStrategy()) {
            case CheckpointStrategyOptions.OFFSETS_ONLY_DEFAULT:
            case CheckpointStrategyOptions.FILE_BASED_OFFSET_CHECKPOINTING:
                return new CountingCheckpointTrigger(kafkaSourceConfig.getMaxBytesInProcessing(), kafkaSourceConfig.getCheckpointIntervalMs());

            case CheckpointStrategyOptions.NONE:
            default:
                return new CheckpointingDisabledTrigger();
        }
    }
}
