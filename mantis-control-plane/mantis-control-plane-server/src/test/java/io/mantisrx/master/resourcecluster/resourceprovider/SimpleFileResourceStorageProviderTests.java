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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterEnvType;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.writable.RegisteredResourceClustersWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterScaleRulesWritable;
import io.mantisrx.master.resourcecluster.writable.ResourceClusterSpecWritable;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
//import org.junit.rules.TemporaryFolder;

@Slf4j
public class SimpleFileResourceStorageProviderTests {
    static ActorSystem system;

//    @Rule
//    public TemporaryFolder storageDirectory = new TemporaryFolder();
    @TempDir
    File storageDirectory, testDirectory;

    @BeforeAll
    public static void setup() throws IOException {
        Config config = ConfigFactory.parseString("akka {\n" +
                "  loggers = [\"akka.testkit.TestEventListener\"]\n" +
                "  loglevel = \"INFO\"\n" +
                "  stdout-loglevel = \"INFO\"\n" +
                "  test.single-expect-default = 300000 millis\n" +
                "}\n");
        system = ActorSystem.create("FileResourceStorageProviderTests", config.withFallback(ConfigFactory.load()));
    }

    @Test
    public void testResourceClusterRules() throws ExecutionException, InterruptedException, IOException {
        final ClusterID clusterId = ClusterID.of("mantisRCMTest2");
        final ContainerSkuID smallSkuId = ContainerSkuID.of("small");
        SimpleFileResourceClusterStorageProvider prov = new SimpleFileResourceClusterStorageProvider(system, storageDirectory);

        ResourceClusterScaleSpec rule1 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId(smallSkuId)
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
        assertEquals(rule1, rules.getScaleRules().get(smallSkuId.getResourceID()));

        ResourceClusterScaleSpec rule2 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId(ContainerSkuID.of("large"))
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
        assertEquals(rule1, rules.getScaleRules().get(smallSkuId.getResourceID()));
        assertEquals(rule2, rules.getScaleRules().get("large"));

        ResourceClusterScaleSpec rule3 = ResourceClusterScaleSpec.builder()
            .clusterId(clusterId)
            .skuId(smallSkuId)
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
        assertEquals(rule3, rules.getScaleRules().get(smallSkuId.getResourceID()));
        assertEquals(rule2, rules.getScaleRules().get("large"));
    }

    @Test
    public void testUpdateClusterSpec() throws ExecutionException, InterruptedException, IOException {
        final String clusterId = "mantisRCMTest2";

        MantisResourceClusterSpec spec = MantisResourceClusterSpec.builder()
                .id(ClusterID.of(clusterId))
                .name(clusterId)
                .envType(MantisResourceClusterEnvType.Prod)
                .ownerEmail("andyz@netflix.com")
                .ownerName("andyz@netflix.com")
                .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("small"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                .skuId(ContainerSkuID.of("small"))
                                .desireSize(2)
                                .maxSize(3)
                                .minSize(1)
                                .build())
                        .cpuCoreCount(5)
                        .memorySizeInMB(16384)
                        .diskSizeInMB(81920)
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

        SimpleFileResourceClusterStorageProvider prov = new SimpleFileResourceClusterStorageProvider(system, testDirectory);

        CompletionStage<ResourceClusterSpecWritable> updateFut = prov.registerAndUpdateClusterSpec(specWritable);
        log.info("res: " + updateFut.toCompletableFuture().get());

        File clusterFilePath = prov.getClusterSpecFilePath(clusterId);
        File regFilePath = prov.getClusterListFilePath();
        assertTrue(regFilePath.exists());
        assertTrue(clusterFilePath.exists());

        System.out.println(Files.readAllLines(clusterFilePath.toPath()).stream().collect(Collectors.joining()));

        assertEquals("{\"clusters\":{\"mantisRCMTest2\":{\"clusterId\":{\"resourceID\":\"mantisRCMTest2\"},"
                + "\"version\":\"v1\"}}}",
                Files.readAllLines(regFilePath.toPath()).stream().collect(Collectors.joining()));

        assertEquals(
                "{\"version\":\"v1\",\"id\":{\"resourceID\":\"mantisRCMTest2\"},\"clusterSpec\":{\"name\":\"mantisRCMTest2\"," +
                        "\"id\":{\"resourceID\":\"mantisRCMTest2\"},\"ownerName\":\"andyz@netflix.com\",\"ownerEmail\":\"andyz@netflix.com\"," +
                        "\"envType\":\"Prod\",\"skuSpecs\":[{\"skuId\":{\"resourceID\":\"small\"},"
                        + "\"capacity\":{\"skuId\":{\"resourceID\":\"small\"},"
                        + "\"minSize\":1,\"maxSize\":3,\"desireSize\":2},\"imageId\":\"mantistaskexecutor:main"
                        + ".latest\",\"cpuCoreCount\":5,\"networkMbps\":700,"
                        + "\"skuMetadataFields\":{\"skuKey\":\"us-east-1\","
                        + "\"sgKey\":\"sg-1,"
                        + " sg-2, sg-3, sg-4\"},\"memorySizeInMB\":16384,\"diskSizeInMB\":81920}],"
                        + "\"clusterMetadataFields\":{}}}",
                Files.readAllLines(clusterFilePath.toPath()).stream().collect(Collectors.joining()));

        //// Test register second cluster.
        String clusterId2 = "clusterApp2";
        MantisResourceClusterSpec spec2 = MantisResourceClusterSpec.builder()
                .id(ClusterID.of(clusterId2))
                .name(clusterId2)
                .envType(MantisResourceClusterEnvType.Prod)
                .ownerEmail("mantisrx@netflix.com")
                .ownerName("mantisrx@netflix.com")
                .skuSpec(MantisResourceClusterSpec.SkuTypeSpec.builder()
                        .skuId(ContainerSkuID.of("large"))
                        .capacity(MantisResourceClusterSpec.SkuCapacity.builder()
                                .skuId(ContainerSkuID.of("large"))
                                .desireSize(3)
                                .maxSize(4)
                                .minSize(1)
                                .build())
                        .cpuCoreCount(19)
                        .memorySizeInMB(54321)
                        .diskSizeInMB(998877)
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
        log.info("res: " + updateFut2.toCompletableFuture().get());

        File clusterFilePath2 = prov.getClusterSpecFilePath(clusterId2);
        assertTrue(regFilePath.exists());
        assertTrue(clusterFilePath2.exists());

        assertEquals(
                "{\"clusters\":{\"mantisRCMTest2\":{\"clusterId\":{\"resourceID\":\"mantisRCMTest2\"},\"version\":\"v1\"},"
                        + "\"clusterApp2\":{\"clusterId\":{\"resourceID\":\"clusterApp2\"},\"version\":\"v2\"}}}",
                Files.readAllLines(regFilePath.toPath()).stream().collect(Collectors.joining()));

        assertEquals(
                "{\"version\":\"v2\",\"id\":{\"resourceID\":\"clusterApp2\"},\"clusterSpec\":{\"name\":\"clusterApp2\","
                        + "\"id\":{\"resourceID\":\"clusterApp2\"},\"ownerName\":\"mantisrx@netflix.com\",\"ownerEmail\":"
                        + "\"mantisrx@netflix.com\",\"envType\":\"Prod\","
                        + "\"skuSpecs\":[{\"skuId\":{\"resourceID\":\"large\"},"
                        + "\"capacity\":{\"skuId\":{\"resourceID\":\"large\"},\"minSize\":1,\"maxSize\":4,"
                        + "\"desireSize\":3},\"imageId\""
                        + ":\"dev/mantistaskexecutor:main.2\",\"cpuCoreCount\":19,"
                        + "\"networkMbps\":3300,\"skuMetadataFields\":"
                        + "{\"skuKey\":\"us-east-1\",\"sgKey\":\"sg-1, sg-2, sg-3, sg-4\"},"
                        + "\"memorySizeInMB\":54321,\"diskSizeInMB\":998877}],"
                        + "\"clusterMetadataFields\":{}}}",
                Files.readAllLines(clusterFilePath2.toPath()).stream().collect(Collectors.joining()));

        RegisteredResourceClustersWritable clusters = prov.getRegisteredResourceClustersWritable()
                .toCompletableFuture().get();
        assertEquals(2, clusters.getClusters().size());

        assertTrue(clusters.getClusters().containsKey(spec.getId().getResourceID()));
        assertEquals(spec.getId(), clusters.getClusters().get(spec.getId().getResourceID()).getClusterId());
        assertEquals(specWritable.getVersion(), clusters.getClusters().get(spec.getId().getResourceID()).getVersion());

        assertTrue(clusters.getClusters().containsKey(spec2.getId().getResourceID()));
        assertEquals(spec2.getId(), clusters.getClusters().get(spec2.getId().getResourceID()).getClusterId());
        assertEquals(specWritable2.getVersion(), clusters.getClusters().get(spec2.getId().getResourceID()).getVersion());

        CompletionStage<ResourceClusterSpecWritable> clusterSpecFut1 =
                prov.getResourceClusterSpecWritable(spec.getId());
        ResourceClusterSpecWritable specResp1 = clusterSpecFut1.toCompletableFuture().get();

        assertEquals(specWritable, specResp1);

        CompletionStage<ResourceClusterSpecWritable> clusterSpecFut2 =
                prov.getResourceClusterSpecWritable(spec2.getId());
        ResourceClusterSpecWritable specResp2 = clusterSpecFut2.toCompletableFuture().get();

        assertEquals(specWritable2, specResp2);

        CompletionStage<RegisteredResourceClustersWritable> deregisterFut =
            prov.deregisterCluster(ClusterID.of(clusterId2));
        RegisteredResourceClustersWritable resClusters = deregisterFut.toCompletableFuture().get();

        assertEquals(1, resClusters.getClusters().size());
        assertTrue(resClusters.getClusters().containsKey(clusterId));
    }
}
