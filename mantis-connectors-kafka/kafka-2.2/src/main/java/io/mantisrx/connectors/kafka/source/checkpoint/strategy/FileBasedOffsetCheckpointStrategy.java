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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;

import io.mantisrx.connectors.kafka.source.serde.OffsetAndMetadataDeserializer;
import io.mantisrx.connectors.kafka.source.serde.OffsetAndMetadataSerializer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mantisrx.runtime.Context;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;


/**
 * DO NOT USE IN PRODUCTION. This strategy is created only for unit test purposes and demonstrates using an alternative
 * storage backend for committing topic partition offsets.
 */
public class FileBasedOffsetCheckpointStrategy implements CheckpointStrategy<OffsetAndMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedOffsetCheckpointStrategy.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        MAPPER.registerModule(new Jdk8Module());
        SimpleModule offsetAndMetadataModule = new SimpleModule();
        offsetAndMetadataModule.addSerializer(OffsetAndMetadata.class, new OffsetAndMetadataSerializer());
        offsetAndMetadataModule.addDeserializer(OffsetAndMetadata.class, new OffsetAndMetadataDeserializer());
        MAPPER.registerModule(offsetAndMetadataModule);
    }

    public static final String DEFAULT_CHECKPOINT_DIR = "/tmp/FileBasedOffsetCheckpointStrategy";
    public static final String CHECKPOINT_DIR_PROP = "checkpointDirectory";
    private final AtomicReference<String> checkpointDir = new AtomicReference<>(null);

    private String filePath(final TopicPartition tp) {
        return checkpointDir.get() + "/" + tp.topic().concat("-").concat(String.valueOf(tp.partition()));
    }

    @Override
    public void init(final Context context) {
        String checkptDir = (String) context.getParameters().get(CHECKPOINT_DIR_PROP);

        checkpointDir.compareAndSet(null, checkptDir);

        createDirectoryIfDoesNotExist(checkpointDir.get());
    }

    @Override
    public boolean persistCheckpoint(Map<TopicPartition, OffsetAndMetadata> checkpoint) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : checkpoint.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final Path filePath = Paths.get(filePath(tp));
            try {
                if (Files.notExists(filePath)) {
                    LOGGER.info("file {} does not exist, creating one", filePath);
                    Files.createFile(filePath);
                }
                Files.write(filePath, Collections.singletonList(MAPPER.writeValueAsString(entry.getValue())));
            } catch (IOException e) {
                LOGGER.error("error writing checkpoint {} to file {}", entry.getValue(), filePath, e);
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    @Override
    public Optional<OffsetAndMetadata> loadCheckpoint(TopicPartition tp) {
        try {
            final List<String> lines = Files.readAllLines(Paths.get(filePath(tp)));
            if (!lines.isEmpty()) {
                final String checkpointString = lines.get(0);
                LOGGER.info("read from file {}", checkpointString);
                return Optional.ofNullable(MAPPER.readValue(checkpointString, OffsetAndMetadata.class));
            }
        } catch (IOException e) {
            LOGGER.error("error loading checkpoint from file {}", filePath(tp), e);
        }
        return Optional.empty();
    }

    @Override
    public void init(Map<String, String> properties) {
        if (!properties.containsKey(CHECKPOINT_DIR_PROP) || Strings.isNullOrEmpty(properties.get(CHECKPOINT_DIR_PROP))) {
            throw new IllegalArgumentException("missing required property " + CHECKPOINT_DIR_PROP);
        }

        String checkptDir = properties.get(CHECKPOINT_DIR_PROP);

        checkpointDir.compareAndSet(null, checkptDir);
        createDirectoryIfDoesNotExist(checkpointDir.get());
    }

    private void createDirectoryIfDoesNotExist(String dir) {
        if (Files.notExists(Paths.get(dir))) {
            LOGGER.info("file {} does not exist, creating one", dir);
            try {
                Files.createDirectory(Paths.get(dir));
            } catch (IOException e) {
                LOGGER.error("failed to create checkpoint directory {}", dir);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Map<TopicPartition, Optional<OffsetAndMetadata>> loadCheckpoints(
        List<TopicPartition> tpList) {
        Map<TopicPartition, Optional<OffsetAndMetadata>> tpChkMap = new HashMap<>();
        for (TopicPartition tp : tpList) {
            tpChkMap.put(tp, loadCheckpoint(tp));
        }
        return tpChkMap;
    }

    @Override
    public String type() {
        return CheckpointStrategyOptions.FILE_BASED_OFFSET_CHECKPOINTING;
    }
}
