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

public final class CheckpointStrategyOptions {

    /**
     * Leverages Kafka for committing offsets.
     */
    public static final String OFFSETS_ONLY_DEFAULT = "offsetsOnlyDefaultKafka";

    /**
     * Sample strategy for storing Offsets outside Kafka to a File based storage, this is only used for Unit testing.
     */
    public static final String FILE_BASED_OFFSET_CHECKPOINTING = "fileBasedOffsetCheckpointing";

    /**
     * Default CheckpointStrategy to disable committing offsets, note this would disable atleast once semantics as
     * offsets are no longer committed to resume from after a worker/process failure.
     */
    public static final String NONE = "disableCheckpointing";

    private CheckpointStrategyOptions() {
    }

    public static String values() {
        return OFFSETS_ONLY_DEFAULT + ", " + FILE_BASED_OFFSET_CHECKPOINTING + ", " + NONE;
    }
}
