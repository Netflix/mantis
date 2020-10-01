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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import io.mantisrx.connector.kafka.source.serde.OffsetAndMetadataDeserializer;
import io.mantisrx.connector.kafka.source.serde.OffsetAndMetadataSerializer;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class FileBasedOffsetCheckpointStrategyTest {

    private FileBasedOffsetCheckpointStrategy strategy = new FileBasedOffsetCheckpointStrategy();

    @Test
    public void testSaveAndLoadCheckpoint() {
        final TopicPartition topicPartition = new TopicPartition("test-topic", 1);
        strategy.init(Collections.singletonMap(FileBasedOffsetCheckpointStrategy.CHECKPOINT_DIR_PROP, FileBasedOffsetCheckpointStrategy.DEFAULT_CHECKPOINT_DIR));
        final OffsetAndMetadata oam = new OffsetAndMetadata(100, Date.from(Instant.now()).toString());
        strategy.persistCheckpoint(Collections.singletonMap(topicPartition, oam));
        final Optional<OffsetAndMetadata> actual = strategy.loadCheckpoint(topicPartition);

        assertEquals(true, actual.isPresent());
        assertEquals(oam, actual.get());
    }

    @Test
    public void testOffsetAndMetadataSerialization() {
        OffsetAndMetadata expected = new OffsetAndMetadata(100, "tempmeta");
        final SimpleModule module = new SimpleModule().addSerializer(OffsetAndMetadata.class, new OffsetAndMetadataSerializer())
            .addDeserializer(OffsetAndMetadata.class, new OffsetAndMetadataDeserializer());
        final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(module);

        try {
            final String s = mapper.writeValueAsString(expected);
            final OffsetAndMetadata actual = mapper.readValue(s, OffsetAndMetadata.class);
            assertEquals(expected, actual);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
