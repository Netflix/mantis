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

import io.mantisrx.connectors.kafka.source.metrics.ConsumerMetrics;
import io.mantisrx.runtime.Context;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class CheckpointStrategyFactory {

    private CheckpointStrategyFactory() { }

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointStrategyFactory.class);

    /**
     * Factory method to create instance of {@link CheckpointStrategy}
     * @param context Mantis runtime context
     * @param consumer Kafka consumer
     * @param strategy checkpoint strategy string
     * @param metrics consumer metrics
     * @return instance of {@link CheckpointStrategy}
     */
    public static CheckpointStrategy<?> getNewInstance(final Context context,
                                                       final KafkaConsumer<?, ?> consumer,
                                                       final String strategy,
                                                       final ConsumerMetrics metrics) {
        switch (strategy) {
            case CheckpointStrategyOptions.OFFSETS_ONLY_DEFAULT:
                final Kafka22OffsetCheckpointStrategy cs = new Kafka22OffsetCheckpointStrategy(consumer, metrics);
                cs.init(context);
                return cs;

            case CheckpointStrategyOptions.FILE_BASED_OFFSET_CHECKPOINTING:
                final FileBasedOffsetCheckpointStrategy fcs = new FileBasedOffsetCheckpointStrategy();
                LOGGER.info("initializing file checkpoint strategy");
                fcs.init(context);
                return fcs;

            case CheckpointStrategyOptions.NONE:
            default:
                return new NoopCheckpointStrategy();

        }
    }
}
