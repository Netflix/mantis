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

package io.mantisrx.server.master;

import java.util.Optional;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterFitnessCalculator implements VMTaskFitnessCalculator {

    private static final Logger logger = LoggerFactory.getLogger(ClusterFitnessCalculator.class);

    private final String clusterAttributeName;

    public ClusterFitnessCalculator() {
        clusterAttributeName = ConfigurationProvider.getConfig().getSlaveClusterAttributeName();
    }

    private Optional<String> getAttribute(final VirtualMachineLease lease, final String attributeName) {
        boolean hasValue = lease.getAttributeMap() != null
                && lease.getAttributeMap().get(attributeName) != null
                && lease.getAttributeMap().get(attributeName).getText().hasValue();
        return hasValue ? Optional.of(lease.getAttributeMap().get(attributeName).getText().getValue()) : Optional.empty();
    }

    @Override
    public String getName() {
        return "Mantis Job Cluster Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        final Optional<String> preferredCluster = ((ScheduleRequest) taskRequest)
                .getPreferredCluster();

        if (preferredCluster.isPresent()) {
            // task has a preferred cluster set, check if the preferred cluster matches the targetVM
            final Optional<String> targetVMCluster = getAttribute(targetVM.getCurrAvailableResources(),
                    clusterAttributeName);
            if (!targetVMCluster.isPresent() ||
                    !targetVMCluster.get().equals(preferredCluster.get())) {
                // the target VM cluster is missing or does not match, not an ideal fit for this request
                if (logger.isDebugEnabled()) {
                    logger.debug("preferred cluster {} targetVM cluster {}", preferredCluster.get(), targetVMCluster.orElse("missing"));
                }
                return 0.8;
            }
        }
        // the task request does not have a preference for a particular cluster or the targetVM cluster matches the preferred cluster
        // so this VM is a perfect fit, can defer to other fitness criteria for selection
        return 1.0;
    }
}
