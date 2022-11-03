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

package io.mantisrx.master.resourcecluster.resourceprovider;

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
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable.RegisteredResourceClustersWritableBuilder;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectReader;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
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
    private final File rootDir;

    public SimpleFileResourceClusterStorageProvider() {
        this.system = ActorSystem.apply("simple-file-resource-cluster-storage-provider");
        this.rootDir = new File(SPOOL_DIR);
        rootDir.mkdirs();
    }

    public SimpleFileResourceClusterStorageProvider(ActorSystem system, File rootDir) {
        this.system = system;
        this.rootDir = rootDir;
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
                                            clusterSpecW.getId().getResourceID(),
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
                .runWith(Sink.<ResourceClusterSpecWritable>last(), system);
        log.info("Return future on registerAndUpdateClusterSpec: {}", clusterSpecWritable.getId());
        return fut;
    }

    @Override
    public CompletionStage<RegisteredResourceClustersWritable> deregisterCluster(ClusterID clusterId) {
        log.info("Starting deregisterCluster: {}", clusterId);
        CompletionStage<RegisteredResourceClustersWritable> fut =
            Source
                .single(clusterId)
                .mapAsync(1, clusterSpecW -> getRegisteredResourceClustersWritable().thenApplyAsync(rc -> {
                        RegisteredResourceClustersWritableBuilder rcBuilder = RegisteredResourceClustersWritable.builder();

                        rc.getClusters().entrySet().stream()
                        .filter(kv -> !Objects.equals(clusterId.getResourceID(), kv.getKey()))
                        .forEach(kv -> rcBuilder.cluster(kv.getKey(), kv.getValue()));
                        return rcBuilder.build();
                    })
                )
                .mapAsync(1, rc -> updateRegisteredClusters(rc))
                .mapAsync(1, notUsed -> getRegisteredResourceClustersWritable())
                .runWith(Sink.<RegisteredResourceClustersWritable>last(), system);
        log.info("Return future on deregisterCluster: {}", clusterId);
        return fut;
    }

    public CompletionStage<ResourceClusterSpecWritable> updateClusterSpecImpl(ResourceClusterSpecWritable spec) {

        Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toFile(
                getClusterSpecFilePath(spec.getId().getResourceID()));
        Source<ResourceClusterSpecWritable, NotUsed> textSource = Source.single(spec);

        return textSource
                .map(w -> mapper.writeValueAsString(w))
                .map(ByteString::fromString)
                .runWith(fileSink, system)
                .exceptionally(e -> { throw new RuntimeException("failed to save spec: " + spec.getId(), e); })
                .thenComposeAsync(ioRes -> getResourceClusterSpecWritable(spec.getId())); // IOResult result is always true.
    }

    public CompletionStage<Boolean> updateRegisteredClusters(RegisteredResourceClustersWritable clusters) throws IOException {
        File listFilePath = getClusterListFilePath();
        if (!listFilePath.exists()) {
            listFilePath.createNewFile();
        }

        Sink<ByteString, CompletionStage<IOResult>> listFileSink = FileIO.toFile(
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
                .fromFile(getClusterListFilePath())
                .via(JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String()))
                .via(jsonToRegisteredClusters)
                .recover(NoSuchFileException.class, () -> {
                    return RegisteredResourceClustersWritable.builder().build();
                });

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
    public CompletionStage<ResourceClusterSpecWritable> getResourceClusterSpecWritable(ClusterID clusterId) {
        final Flow<String, ResourceClusterSpecWritable, NotUsed> jsonToRegisteredClusters =
                Flow.of(String.class).map(mapper.readerFor(ResourceClusterSpecWritable.class)::readValue);

        final Source<ResourceClusterSpecWritable, ?> fromFile = FileIO
                .fromFile(getClusterSpecFilePath(clusterId.getResourceID()))
                .via(JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String()))
                .via(jsonToRegisteredClusters);

        return fromFile
                .via(Flow.of(ResourceClusterSpecWritable.class)
                    .alsoTo(Sink.foreach(c -> log.info("Got cluster spec: {}", c))))
                .runWith(Sink.last(), Materializer.createMaterializer(system))
                .exceptionally(e -> { throw new RuntimeException("Failed to retrieve cluster spec: " + clusterId, e); });
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> getResourceClusterScaleRules(ClusterID clusterId) {
        final Flow<String, ResourceClusterScaleRulesWritable, NotUsed> jsonToScaleRules =
            Flow.of(String.class).map(mapper.readerFor(ResourceClusterScaleRulesWritable.class)::readValue);

        final Source<ResourceClusterScaleRulesWritable, ?> fromFile = FileIO
            .fromFile(getClusterScaleRuleSpecFilePath(clusterId.getResourceID()))
            .via(JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String()))
            .via(jsonToScaleRules)
            .recover(NoSuchFileException.class, () ->
                ResourceClusterScaleRulesWritable.builder().clusterId(clusterId).build());;

        return fromFile
            .via(Flow.of(ResourceClusterScaleRulesWritable.class)
                .alsoTo(Sink.foreach(c -> log.info("Got cluster scale rule spec: {}", c))))
            .runWith(Sink.last(), Materializer.createMaterializer(system))
            .exceptionally(e -> {
                throw new RuntimeException("Failed to retrieve cluster scaleRule spec: " + clusterId, e);
            });
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(
        ResourceClusterScaleRulesWritable ruleSpec) {
        log.info("Starting registerResourceClusterScaleRule with full spec: {}", ruleSpec);
        CompletionStage<ResourceClusterScaleRulesWritable> fut =
            Source
                .single(ruleSpec)
                .mapAsync(1, this::updateClusterScaleRules)
                .mapAsync(1, rc -> getResourceClusterScaleRules(ruleSpec.getClusterId()))
                .runWith(Sink.<ResourceClusterScaleRulesWritable>last(), system);
        log.info("Return future on registerResourceClusterScaleRule with full spec: {}", ruleSpec.getClusterId());
        return fut;
    }

    @Override
    public CompletionStage<ResourceClusterScaleRulesWritable> registerResourceClusterScaleRule(ResourceClusterScaleSpec rule) {
        log.info("Starting registerResourceClusterScaleRule: {}", rule);
        CompletionStage<ResourceClusterScaleRulesWritable> fut =
            Source
                .single(rule)
                .mapAsync(1, ruleSpec -> getResourceClusterScaleRules(rule.getClusterId())
                    .thenApplyAsync(rc -> {
                        ResourceClusterScaleRulesWritable.ResourceClusterScaleRulesWritableBuilder rcBuilder =
                            (rc == null) ?
                            ResourceClusterScaleRulesWritable.builder().clusterId(rule.getClusterId()) :
                            rc.toBuilder();

                        return rcBuilder.scaleRule(rule.getSkuId().getResourceID(), rule).build();
                    })
                )
                .mapAsync(1, rc -> updateClusterScaleRules(rc))
                .mapAsync(1, rc -> getResourceClusterScaleRules(rule.getClusterId()))
                .runWith(Sink.<ResourceClusterScaleRulesWritable>last(), system);
        log.info("Return future on registerResourceClusterScaleRule: {}", rule.getClusterId());
        return fut;
    }

    public CompletionStage<Boolean> updateClusterScaleRules(ResourceClusterScaleRulesWritable rules) throws IOException {
        File ruleFilePath = getClusterScaleRuleSpecFilePath(rules.getClusterId().getResourceID());
        if (!ruleFilePath.exists()) {
            ruleFilePath.createNewFile();
        }

        Sink<ByteString, CompletionStage<IOResult>> rulesFileSink = FileIO.toFile(
            getClusterScaleRuleSpecFilePath(rules.getClusterId().getResourceID()));
        Source<ResourceClusterScaleRulesWritable, NotUsed> textSource = Source.single(rules);
        return textSource
            .map(mapper::writeValueAsString)
            .map(ByteString::fromString)
            .runWith(rulesFileSink, system)
            .exceptionally(e -> { throw new RuntimeException("failed to save cluster list. ", e); })
            .thenApply(ioRes -> true); // IOResult result is always true.
    }

    @VisibleForTesting
    File getClusterSpecFilePath(String clusterId) {
        return new File(rootDir, "clusterspec-" + clusterId);
    }

    File getClusterScaleRuleSpecFilePath(String clusterId) {
        return new File(rootDir, "clusterscalerulespec-" + clusterId);
    }

    File getClusterListFilePath() {
        return new File(rootDir, CLUSTER_LIST_FILE_NAME);
    }
}
