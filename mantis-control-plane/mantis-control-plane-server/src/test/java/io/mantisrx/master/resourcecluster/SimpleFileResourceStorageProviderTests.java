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

package io.mantisrx.master.resourcecluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.resourceprovider.SimpleFileResourceClusterStorageProvider;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleFileResourceStorageProviderTests {
    static ActorSystem system;

    @BeforeClass
    public static void setup() throws IOException {
        Config config = ConfigFactory.parseString("akka {\n" +
                "  loggers = [\"akka.testkit.TestEventListener\"]\n" +
                "  loglevel = \"INFO\"\n" +
                "  stdout-loglevel = \"INFO\"\n" +
                "  test.single-expect-default = 300000 millis\n" +
                "}\n");
        system = ActorSystem.create("FileResourceStorageProviderTests", config.withFallback(ConfigFactory.load()));
        deleteFiles();
    }

    @Test
    public void testResourceClusterRules() throws ExecutionException, InterruptedException, IOException {
        final String clusterId = "mantisRCMTest2";
        SimpleFileResourceClusterStorageProvider prov = new SimpleFileResourceClusterStorageProvider(system);

        ResourceClusterScaleSpec rule1 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId("small")
            .coolDownSecs(5)
            .maxIdleToKeep(10)
            .minIdleToKeep(5)
            .minSize(1)
            .maxSize(25)
            .build();
        CompletionStage<ResourceClusterScaleRulesWritable> updateFut = prov.registerResourceClusterScaleRule(rule1);
        System.out.println("res: " + updateFut.toCompletableFuture().get());

        CompletionStage<ResourceClusterScaleRulesWritable> rulesFut = prov.getResourceClusterScaleRules(clusterId);
        ResourceClusterScaleRulesWritable rules = rulesFut.toCompletableFuture().get();
        assertEquals(1, rules.getScaleRules().size());
        assertEquals(clusterId, rules.getClusterId());
        assertEquals(rule1, rules.getScaleRules().get("small"));

        ResourceClusterScaleSpec rule2 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId("large")
            .coolDownSecs(9)
            .maxIdleToKeep(99)
            .minIdleToKeep(5)
            .minSize(1)
            .maxSize(25)
            .build();
        CompletionStage<ResourceClusterScaleRulesWritable> updateFut2 = prov.registerResourceClusterScaleRule(rule2);
        updateFut2.toCompletableFuture().get();

        CompletionStage<ResourceClusterScaleRulesWritable> rulesFut2 = prov.getResourceClusterScaleRules(clusterId);
        rules = rulesFut2.toCompletableFuture().get();
        assertEquals(2, rules.getScaleRules().size());
        assertEquals(clusterId, rules.getClusterId());
        assertEquals(rule1, rules.getScaleRules().get("small"));
        assertEquals(rule2, rules.getScaleRules().get("large"));

        ResourceClusterScaleSpec rule3 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId("small")
            .coolDownSecs(999)
            .maxIdleToKeep(123)
            .minIdleToKeep(2)
            .minSize(1)
            .maxSize(25)
            .build();
        CompletionStage<ResourceClusterScaleRulesWritable> updateFut3 = prov.registerResourceClusterScaleRule(rule3);
        updateFut3.toCompletableFuture().get();

        CompletionStage<ResourceClusterScaleRulesWritable> rulesFut3 = prov.getResourceClusterScaleRules(clusterId);
        rules = rulesFut3.toCompletableFuture().get();
        assertEquals(2, rules.getScaleRules().size());
        assertEquals(clusterId, rules.getClusterId());
        assertEquals(rule3, rules.getScaleRules().get("small"));
        assertEquals(rule2, rules.getScaleRules().get("large"));
    }

    @Test
    public void testUpdateClusterSpec() throws ExecutionException, InterruptedException, IOException {
        final String clusterId = "mantisRCMTest2";

        MantisResourceClusterSpec spec = MantisResourceClusterSpec.builder()
                .id(clusterId)
                .name(clusterId)
                .envType(MantisResourceClusterEnvType.Prod)
                .ownerEmail("andyz@netflix.com")
                .ownerName("andyz@netflix.com")
                .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId("small")
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                .skuId("small")
                                .desireSize(2)
                                .maxSize(3)
                                .minSize(1)
                                .build())
                        .cpuCoreCount(5)
                        .memorySizeInBytes(16384)
                        .diskSizeInBytes(81920)
                        .networkMbps(700)
                        .imageId("mantistaskexecutor:main.latest")
                        .skuMetadataField(
                                "skuKey",
                                "us-east-1")
                        .skuMetadataField(
                                "sgKey",
                                "sg-1, sg-2, sg-3, sg-4")
                        .build())
                .build();

        ResourceClusterSpecWritable specWritable = ResourceClusterSpecWritable.builder()
                .clusterSpec(spec)
                .version("v1")
                .id(spec.getId())
                .build();

        SimpleFileResourceClusterStorageProvider prov = new SimpleFileResourceClusterStorageProvider(system);

        CompletionStage<ResourceClusterSpecWritable> updateFut = prov.registerAndUpdateClusterSpec(specWritable);
        System.out.println("res: " + updateFut.toCompletableFuture().get());

        Path clusterFilePath = Paths.get(SimpleFileResourceClusterStorageProvider.SPOOL_DIR, "clusterspec-" + clusterId);
        Path regFilePath = Paths.get(
                SimpleFileResourceClusterStorageProvider.SPOOL_DIR, "mantisResourceClusterRegistrations");
        assertTrue(Files.exists(regFilePath));
        assertTrue(Files.exists(clusterFilePath));

        System.out.println(Files.readAllLines(clusterFilePath).stream().collect(Collectors.joining()));

        assertEquals("{\"clusters\":{\"mantisRCMTest2\":{\"clusterId\":\"mantisRCMTest2\",\"version\":\"v1\"}}}",
                Files.readAllLines(regFilePath).stream().collect(Collectors.joining()));

        assertEquals(
                "{\"version\":\"v1\",\"id\":\"mantisRCMTest2\",\"clusterSpec\":{\"name\":\"mantisRCMTest2\"," +
                        "\"id\":\"mantisRCMTest2\",\"ownerName\":\"andyz@netflix.com\",\"ownerEmail\":\"andyz@netflix.com\"," +
                        "\"envType\":\"Prod\",\"skuSpecs\":[{\"skuId\":\"small\",\"capacity\":{\"skuId\":\"small\"," +
                        "\"minSize\":1,\"maxSize\":3,\"desireSize\":2},\"imageId\":\"mantistaskexecutor:main"
                        + ".latest\"," +
                        "\"cpuCoreCount\":5,\"memorySizeInBytes\":16384,\"networkMbps\":700,\"diskSizeInBytes\":81920," +
                        "\"skuMetadataFields\":{\"skuKey\":\"us-east-1\","
                        + "\"sgKey\":\"sg-1," +
                        " sg-2, sg-3, sg-4\"}}],\"clusterMetadataFields\":{}}}",
                Files.readAllLines(clusterFilePath).stream().collect(Collectors.joining()));

        //// Test register second cluster.
        String clusterId2 = "clusterApp2";
        MantisResourceClusterSpec spec2 = MantisResourceClusterSpec.builder()
                .id(clusterId2)
                .name(clusterId2)
                .envType(MantisResourceClusterEnvType.Prod)
                .ownerEmail("mantisrx@netflix.com")
                .ownerName("mantisrx@netflix.com")
                .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId("large")
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                .skuId("large")
                                .desireSize(3)
                                .maxSize(4)
                                .minSize(1)
                                .build())
                        .cpuCoreCount(19)
                        .memorySizeInBytes(54321)
                        .diskSizeInBytes(998877)
                        .networkMbps(3300)
                        .imageId("dev/mantistaskexecutor:main.2")
                        .skuMetadataField(
                                "skuKey",
                                "us-east-1")
                        .skuMetadataField(
                                "sgKey",
                                "sg-1, sg-2, sg-3, sg-4")
                        .build())
                .build();

        ResourceClusterSpecWritable specWritable2 = ResourceClusterSpecWritable.builder()
                .clusterSpec(spec2)
                .version("v2")
                .id(spec2.getId())
                .build();

        CompletionStage<ResourceClusterSpecWritable> updateFut2 = prov.registerAndUpdateClusterSpec(specWritable2);
        System.out.println("res: " + updateFut2.toCompletableFuture().get());

        Path clusterFilePath2 = Paths.get("/tmp/MantisSpool", "clusterspec-" + clusterId2);
        assertTrue(Files.exists(regFilePath));
        assertTrue(Files.exists(clusterFilePath2));

        assertEquals(
                "{\"clusters\":{\"mantisRCMTest2\":{\"clusterId\":\"mantisRCMTest2\",\"version\":\"v1\"},"
                        + "\"clusterApp2\":{\"clusterId\":\"clusterApp2\",\"version\":\"v2\"}}}",
                Files.readAllLines(regFilePath).stream().collect(Collectors.joining()));

        assertEquals(
                "{\"version\":\"v2\",\"id\":\"clusterApp2\",\"clusterSpec\":{\"name\":\"clusterApp2\","
                        + "\"id\":\"clusterApp2\",\"ownerName\":\"mantisrx@netflix.com\",\"ownerEmail\":"
                        + "\"mantisrx@netflix.com\",\"envType\":\"Prod\",\"skuSpecs\":[{\"skuId\":\"large\","
                        + "\"capacity\":{\"skuId\":\"large\",\"minSize\":1,\"maxSize\":4,\"desireSize\":3},\"imageId\""
                        + ":\"dev/mantistaskexecutor:main.2\",\"cpuCoreCount\":19,\"memorySizeInBytes\":54321,"
                        + "\"networkMbps\":3300,\"diskSizeInBytes\":998877,\"skuMetadataFields\":"
                        + "{\"skuKey\":\"us-east-1\",\"sgKey\":\"sg-1, sg-2, sg-3, sg-4\"}}],"
                        + "\"clusterMetadataFields\":{}}}",
                Files.readAllLines(clusterFilePath2).stream().collect(Collectors.joining()));

        RegisteredResourceClustersWritable clusters = prov.getRegisteredResourceClustersWritable()
                .toCompletableFuture().get();
        assertEquals(2, clusters.getClusters().size());

        assertTrue(clusters.getClusters().containsKey(spec.getId()));
        assertEquals(spec.getId(), clusters.getClusters().get(spec.getId()).getClusterId());
        assertEquals(specWritable.getVersion(), clusters.getClusters().get(spec.getId()).getVersion());

        assertTrue(clusters.getClusters().containsKey(spec2.getId()));
        assertEquals(spec2.getId(), clusters.getClusters().get(spec2.getId()).getClusterId());
        assertEquals(specWritable2.getVersion(), clusters.getClusters().get(spec2.getId()).getVersion());

        CompletionStage<ResourceClusterSpecWritable> clusterSpecFut1 =
                prov.getResourceClusterSpecWritable(spec.getId());
        ResourceClusterSpecWritable specResp1 = clusterSpecFut1.toCompletableFuture().get();

        assertEquals(specWritable, specResp1);

        CompletionStage<ResourceClusterSpecWritable> clusterSpecFut2 =
                prov.getResourceClusterSpecWritable(spec2.getId());
        ResourceClusterSpecWritable specResp2 = clusterSpecFut2.toCompletableFuture().get();

        assertEquals(specWritable2, specResp2);

        CompletionStage<RegisteredResourceClustersWritable> deregisterFut = prov.deregisterCluster(clusterId2);
        RegisteredResourceClustersWritable resClusters = deregisterFut.toCompletableFuture().get();

        assertEquals(1, resClusters.getClusters().size());
        assertTrue(resClusters.getClusters().containsKey(clusterId));
    }

    private static void deleteFiles() throws IOException {
        Path path = Paths.get(SimpleFileResourceClusterStorageProvider.SPOOL_DIR);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}
