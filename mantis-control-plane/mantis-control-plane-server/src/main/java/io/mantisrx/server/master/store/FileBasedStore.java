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

package io.mantisrx.server.master.store;


import io.mantisrx.server.core.IKeyValueStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple File based storage provider. Intended mainly as a sample implementation for
 * {@link IKeyValueStore} interface. This implementation is complete in its functionality, but, isn't
 * expected to be scalable or performant for production loads.
 * <P>This implementation uses <code>/tmp/MantisSpool/</code> as the spool directory. The directory is created
 * if not present already. It will fail only if either a file with that name exists or if a directory with that
 * name exists but isn't writable.</P>
 */
public class FileBasedStore implements IKeyValueStore {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedStore.class);
    private final File rootDir;
    private final ReentrantLock fileLock = new ReentrantLock();

    public FileBasedStore() {
        this(new File("/tmp/mantis_storage"));
    }

    public FileBasedStore(File rootDir) {
        this.rootDir = rootDir;
        final Path rootDirPath = Paths.get(rootDir.getPath());
        try {
            if (Files.notExists(rootDirPath)) {
                Files.createDirectories(rootDirPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path makePath(String dir, String fileName) throws IOException {
        Files.createDirectories(Paths.get(rootDir.getPath(), dir));
        return Paths.get(rootDir.getPath(), dir, fileName);
    }

    public void reset() {
        try {
            FileUtils.deleteDirectory(Paths.get(rootDir.getPath()).toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    @Override
    public Map<String, Map<String, String>> getAllRows(String tableName) throws IOException {
        return getAllPartitionKeys(tableName).stream().map(partitionKey -> {
            try {
                return Pair.of(partitionKey, getAll(tableName, partitionKey));
            } catch (Exception e) {
                logger.warn("failed to read file for partitionKey {} because", partitionKey, e);
                return null;
            }})
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    @Override
    public List<String> getAllPartitionKeys(String tableName) throws IOException {
        final Path tableRoot = Paths.get(this.rootDir.getPath(), tableName);
        if (Files.notExists(tableRoot)) {
            return Collections.emptyList();
        }
        try (Stream<Path> paths = Files.list(tableRoot)) {
            return paths
                .map(x -> x.getFileName().toString())
                .collect(Collectors.toList());
        }
    }

    @Override
    public String get(String tableName, String partitionKey, String secondaryKey) throws IOException {
        return getAll(tableName, partitionKey).get(secondaryKey);
    }

    @Override
    public Map<String, String> getAll(String tableName, String partitionKey) throws IOException {
        final Path filePath = makePath(tableName, partitionKey);
        if (Files.notExists(filePath)) {
            return new HashMap<>();
        }
        return Files.readAllLines(filePath)
            .stream()
            .map(line -> line.split(",", 2))
            .collect(Collectors.toMap(tokens -> tokens[0], tokens -> tokens[1]));
    }

    @Override
    public boolean upsert(String tableName, String partitionKey, String secondaryKey, String data) throws IOException {
        fileLock.lock();
        try {
            final Map<String, String> items = getAll(tableName, partitionKey);
            items.put(secondaryKey, data);
            upsertAll(tableName, partitionKey, items);
            return true;
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public boolean upsertAll(String tableName, String partitionKey, Map<String, String> all, Duration ttl) throws IOException {
        final Path filePath = makePath(tableName, partitionKey);
        final List<String> lines = all.entrySet().stream()
            .map(e -> e.getKey() + "," + e.getValue())
            .collect(Collectors.toList());
        fileLock.lock();
        try {
            Files.write(filePath, lines);
            return true;
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public boolean delete(String tableName, String partitionKey, String secondaryKey) throws IOException {
        fileLock.lock();
        try {
            final Map<String, String> items = getAll(tableName, partitionKey);
            items.remove(secondaryKey);
            upsertAll(tableName, partitionKey, items);
            return true;
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public boolean deleteAll(String tableName, String partitionKey) throws IOException {
        final Path filePath = makePath(tableName, partitionKey);
        fileLock.lock();
        try {
            return filePath.toFile().delete();
        } finally {
            fileLock.unlock();
        }
    }
}
