/*
 * Copyright 2024 Netflix, Inc.
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

import io.mantisrx.master.resourcecluster.proto.SkuSizeSpec;
import io.mantisrx.master.resourcecluster.proto.SkuTypeSpec;
import io.mantisrx.runtime.AllocationConstraints;
import io.mantisrx.server.master.persistence.IMantisPersistenceProvider;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import io.mantisrx.server.master.resourcecluster.ContainerSkuID;
import io.mantisrx.server.master.resourcecluster.SkuSizeID;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TaskExecutorAllocationHelper {
    private final ClusterID clusterID;
    private final Map<String, String> allocationAttributesWithDefaults;
    private final IMantisPersistenceProvider storageProvider;

    private Map<ContainerSkuID, SkuTypeSpec> skus = new ConcurrentHashMap<>();
    private Map<SkuSizeID, SkuSizeSpec> skuSizes = new ConcurrentHashMap<>();


    public TaskExecutorAllocationHelper(ClusterID clusterID, IMantisPersistenceProvider storageProvider, String allocationAttributesWithDefaults) {
        this.clusterID = clusterID;
        this.storageProvider = storageProvider;
        this.allocationAttributesWithDefaults = Arrays.stream(allocationAttributesWithDefaults.split(","))
            .filter(s -> !s.trim().isEmpty() && s.contains(":"))
            .map(s -> s.split(":"))
            .collect(Collectors.toMap(a -> a[0].trim(), a -> a[1].trim()));
    }

    public Optional<ContainerSkuID> findBestFitSkuID(AllocationConstraints allocationConstraints) {
        ContainerSkuID bestFitSkuID = null;

        for (SkuTypeSpec sku : skus.values()) {
            if (canFit(sku.getSkuId(), allocationConstraints)) {
                if (bestFitSkuID != null) {
                    bestFitSkuID = findBestFit(bestFitSkuID, sku.getSkuId());
                } else {
                    bestFitSkuID = sku.getSkuId();
                }
            }
        }
        return bestFitSkuID == null ? Optional.empty() : Optional.of(bestFitSkuID);
    }

    public void addSkuIdIfAbsent(ContainerSkuID skuID) {
        if (!skus.containsKey(skuID)) {
            refreshMetadata(skuID);
        }
    }

    public void removeSkuIdIfPresent(ContainerSkuID skuID) {
        skus.remove(skuID);
    }

    private boolean canFit(ContainerSkuID skuID, AllocationConstraints constraints) {
        return canFitResources(skuID, constraints) && areAllocationConstraintsSatisfied(skuID, constraints);
    }

    private boolean canFitResources(ContainerSkuID skuID, AllocationConstraints constraints) {
        // TODO: shall we also check disk & other dims?
        final SkuSizeSpec skuSize = skuSizes.get(getSkuSizeIdFrom(skus.get(skuID)));
         return skuSize.getCpuCoreCount() >= constraints.getMachineDefinition().getCpuCores() &&
             skuSize.getMemorySizeInMB() >= constraints.getMachineDefinition().getMemoryMB();
    }

    private boolean areAllocationConstraintsSatisfied(ContainerSkuID skuID, AllocationConstraints constraints) {
        for (Entry<String, String> e : allocationAttributesWithDefaults.entrySet()) {
            final String skuAttrValue = skus.get(skuID).getSkuMetadataFields().getOrDefault(e.getKey(), e.getValue());
            final String constraintValue = constraints.getAssignmentAttributes().getOrDefault(e.getKey(), e.getValue());
            if (!skuAttrValue.equalsIgnoreCase(constraintValue)) {
                return false;
            }
        }
        return true;
    }

    private ContainerSkuID findBestFit(ContainerSkuID sku0, ContainerSkuID sku1) {
        final SkuSizeSpec size0 = skuSizes.get(getSkuSizeIdFrom(skus.get(sku0)));
        final SkuSizeSpec size1 = skuSizes.get(getSkuSizeIdFrom(skus.get(sku1)));
        if (size0.getCpuCoreCount() < size1.getCpuCoreCount() || (
            size0.getCpuCoreCount() == size1.getCpuCoreCount() && size0.getMemorySizeInMB() < size1.getMemorySizeInMB()
        )) {
            return sku0;
        }
        return sku1;
    }

    private void refreshMetadata(ContainerSkuID skuID) {
        try {
            Objects.requireNonNull(this.storageProvider.getResourceClusterSpecWritable(clusterID))
                .getClusterSpec()
                .getSkuSpecs()
                .stream()
                .filter(s -> s.getSkuId().equals(skuID))
                .forEach(skuTypeSpec -> {
                    this.skus.put(skuID, skuTypeSpec);

                    // If skuSizeID hasn't been set yet --> create one on the fly
                    // TODO: remove this when all sku specs are updated with sizeID
                    if (skuTypeSpec.getSizeId() == null) {
                        SkuSizeSpec skuSizeSpec = SkuSizeSpec.builder()
                            .skuSizeID(getSkuSizeIdFrom(skuTypeSpec))
                            .skuSizeName(getSkuSizeIdFrom(skuTypeSpec).getResourceID())
                            .cpuCoreCount(skuTypeSpec.getCpuCoreCount())
                            .memorySizeInMB(skuTypeSpec.getMemorySizeInMB())
                            .diskSizeInMB(skuTypeSpec.getDiskSizeInMB())
                            .networkMbps(skuTypeSpec.getNetworkMbps())
                            .build();
                        this.skuSizes.put(skuSizeSpec.getSkuSizeID(), skuSizeSpec);
                        return;
                    }
                    if (!skuSizes.containsKey(skuTypeSpec.getSizeId())) {
                        try {
                            this.storageProvider.getResourceClusterSkuSizes()
                                .forEach(skuSizeSpec -> this.skuSizes.put(skuSizeSpec.getSkuSizeID(), skuSizeSpec));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SkuSizeID getSkuSizeIdFrom(SkuTypeSpec skuTypeSpec) {
        return skuTypeSpec.getSizeId() == null ? SkuSizeID.of("skuTypeSpec-" + skuTypeSpec.getSkuId().getResourceID()): skuTypeSpec.getSizeId();
    }
}
