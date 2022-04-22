/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.control.plane.resource.cluster.resourceprovider;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.JsonFraming;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import io.mantisrx.control.plane.resource.cluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.control.plane.resource.cluster.writable.RegisteredResourceClustersWritable.RegisteredResourceClustersWritableBuilder;
import io.mantisrx.control.plane.resource.cluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple file-based implementation for {@link ResourceClusterStorageProvider}. Not meant for production usage.
 */
@Slf4j
public class SimpleFileResourceClusterStorageProvider implements ResourceClusterStorageProvider {
    public final static String SPOOL_DIR = "/tmp/MantisSpool";

    private final static String CLUSTER_LIST_FILE_NAME = "mantisResourceClusterRegistrations";

    private final static ObjectMapper mapper = new ObjectMapper();

    private final ActorSystem system;

    public SimpleFileResourceClusterStorageProvider(ActorSystem system) {
        this.system = system;
        new File(SPOOL_DIR).mkdirs();
    }

    @Override
    public CompletionStage<ResourceClusterSpecWritable> registerAndUpdateClusterSpec(
            ResourceClusterSpecWritable clusterSpecWritable) {
        log.info("Starting registerAndUpdateClusterSpec: {}", clusterSpecWritable.getId());
        CompletionStage<ResourceClusterSpecWritable> fut =
                Source
                .single(clusterSpecWritable)
                .mapAsync(1, clusterSpecW -> getRegisteredResourceClustersWritable().thenApplyAsync(rc -> {
                            RegisteredResourceClustersWritable.RegisteredResourceClustersWritableBuilder rcBuilder
                                    = (rc == null) ? RegisteredResourceClustersWritable.builder() : rc.toBuilder();

                            return rcBuilder.cluster(
                                            clusterSpecW.getId(),
                                            RegisteredResourceClustersWritable.ClusterRegistration.builder()
                                                    .clusterId(clusterSpecW.getId())
                                                    .version(clusterSpecW.getVersion())
                                                    .build())
                                    .build();
                        })
                )
                .mapAsync(1, rc -> updateRegisteredClusters(rc))
                .mapAsync(1, rc -> updateClusterSpecImpl(clusterSpecWritable))
                .mapAsync(1, rc -> getResourceClusterSpecWritable(rc.getId()))
                .runWith(Sink.last(), system);
        log.info("Return future on registerAndUpdateClusterSpec: {}", clusterSpecWritable.getId());
        return fut;
    }

    @Override
    public CompletionStage<RegisteredResourceClustersWritable> deregisterCluster(String clusterId) {
        log.info("Starting deregisterCluster: {}", clusterId);
        CompletionStage<RegisteredResourceClustersWritable> fut =
            Source
                .single(clusterId)
                .mapAsync(1, clusterSpecW -> getRegisteredResourceClustersWritable().thenApplyAsync(rc -> {
                        RegisteredResourceClustersWritableBuilder rcBuilder = RegisteredResourceClustersWritable.builder();

                        rc.getClusters().entrySet().stream()
                        .filter(kv -> !Objects.equals(clusterId, kv.getKey()))
                        .forEach(kv -> rcBuilder.cluster(kv.getKey(), kv.getValue()));
                        return rcBuilder.build();
                    })
                )
                .mapAsync(1, rc -> updateRegisteredClusters(rc))
                .mapAsync(1, notUsed -> getRegisteredResourceClustersWritable())
                .runWith(Sink.last(), system);
        log.info("Return future on deregisterCluster: {}", clusterId);
        return fut;
    }

    public CompletionStage<ResourceClusterSpecWritable> updateClusterSpecImpl(ResourceClusterSpecWritable spec) {

        Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toPath(
                getClusterSpecFilePath(SPOOL_DIR, spec.getId()));
        Source<ResourceClusterSpecWritable, NotUsed> textSource = Source.single(spec);

        return textSource
                .map(w -> mapper.writeValueAsString(w))
                .map(ByteString::fromString)
                .runWith(fileSink, system)
                .exceptionally(e -> { throw new RuntimeException("failed to save spec: " + spec.getId(), e); })
                .thenComposeAsync(ioRes -> getResourceClusterSpecWritable(spec.getId())); // IOResult result is always true.
    }

    public CompletionStage<Boolean> updateRegisteredClusters(RegisteredResourceClustersWritable clusters) throws IOException {
        Path listFilePath = getClusterListFilePath();
        if (Files.notExists(listFilePath)) {
            Files.createDirectories(Paths.get(SPOOL_DIR));
            Files.createFile(listFilePath);
        }

        Sink<ByteString, CompletionStage<IOResult>> listFileSink = FileIO.toPath(
                getClusterListFilePath());
        Source<RegisteredResourceClustersWritable, NotUsed> textSource = Source.single(clusters);
        return textSource
                .map(mapper::writeValueAsString)
                .map(ByteString::fromString)
                .runWith(listFileSink, system)
                .exceptionally(e -> { throw new RuntimeException("failed to save cluster list. ", e); })
                .thenApply(ioRes -> true); // IOResult result is always true.
    }

    @Override
    public CompletionStage<RegisteredResourceClustersWritable> getRegisteredResourceClustersWritable() {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectReader readOrder = mapper.readerFor(RegisteredResourceClustersWritable.class);
        final Flow<String, RegisteredResourceClustersWritable, NotUsed> jsonToRegisteredClusters =
                Flow.of(String.class).map(readOrder::<RegisteredResourceClustersWritable>readValue);

        final Source<RegisteredResourceClustersWritable, ?> fromFile = FileIO
                .fromPath(getClusterListFilePath())
                .via(JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String()))
                .via(jsonToRegisteredClusters)
                .recover(NoSuchFileException.class, () -> RegisteredResourceClustersWritable.builder().build());

        return fromFile
                .via(Flow.of(RegisteredResourceClustersWritable.class)
                        .alsoTo(Sink.foreach(c -> log.info("Read cluster: {}", c))))
                .runWith(Sink.seq(), system)
                .exceptionally(e -> {
                    throw new RuntimeException("failed to get registered clusters: " + e.getMessage(), e);
                })
                .thenApplyAsync(list -> list.size() > 0 ? list.get(0) : null);
    }

    @Override
    public CompletionStage<ResourceClusterSpecWritable> getResourceClusterSpecWritable(String clusterId) {
        final Flow<String, ResourceClusterSpecWritable, NotUsed> jsonToRegisteredClusters =
                Flow.of(String.class).map(mapper.readerFor(ResourceClusterSpecWritable.class)::readValue);

        final Source<ResourceClusterSpecWritable, ?> fromFile = FileIO
                .fromPath(getClusterSpecFilePath(SPOOL_DIR, clusterId))
                .via(JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String()))
                .via(jsonToRegisteredClusters);

        return fromFile
                .via(Flow.of(ResourceClusterSpecWritable.class)
                    .alsoTo(Sink.foreach(c -> log.info("Got cluster spec: {}", c))))
                .runWith(Sink.last(), Materializer.createMaterializer(system))
                .exceptionally(e -> { throw new RuntimeException("Failed to retrieve clsuter spec: " + clusterId, e); });
    }

    private Path getClusterSpecFilePath(String dirName, String clusterId) {
        return Paths.get(dirName, "clusterspec-" + clusterId);
    }

    private Path getClusterListFilePath() {
        return Paths.get(SPOOL_DIR, CLUSTER_LIST_FILE_NAME);
    }
}
