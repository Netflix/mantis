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

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.BinPackingFitnessCalculators;
import io.mantisrx.server.master.config.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AgentFitnessCalculator implements VMTaskFitnessCalculator {

    private static final Logger logger = LoggerFactory.getLogger(AgentFitnessCalculator.class);
    final VMTaskFitnessCalculator binPacker = BinPackingFitnessCalculators.cpuMemNetworkBinPacker;
    final VMTaskFitnessCalculator durationTypeFitnessCalculator = new DurationTypeFitnessCalculator();
    final VMTaskFitnessCalculator clusterFitnessCalculator = new ClusterFitnessCalculator();
    private final double binPackingWeight;
    private final double clusterWeight;
    private final double durationTypeWeight;
    private final double goodEnoughThreshold;
    private final Func1<Double, Boolean> fitnessGoodEnoughFunc;
    public AgentFitnessCalculator() {
        binPackingWeight = ConfigurationProvider.getConfig().getBinPackingFitnessWeight();
        clusterWeight = ConfigurationProvider.getConfig().getPreferredClusterFitnessWeight();
        durationTypeWeight = ConfigurationProvider.getConfig().getDurationTypeFitnessWeight();
        goodEnoughThreshold = ConfigurationProvider.getConfig().getFitnessGoodEnoughThreshold();
        logger.info("clusterWeight {} durationTypeWeight {} binPackingWeight {} goodEnoughThreshold {}", clusterWeight, durationTypeWeight, binPackingWeight, goodEnoughThreshold);
        this.fitnessGoodEnoughFunc = f -> f > goodEnoughThreshold;
    }

    @Override
    public String getName() {
        return "Mantis Agent Task Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        double binPackingValue = binPacker.calculateFitness(taskRequest, targetVM, taskTrackerState);
        double durationTypeFitness = durationTypeFitnessCalculator.calculateFitness(taskRequest, targetVM, taskTrackerState);
        double clusterFitnessValue = clusterFitnessCalculator.calculateFitness(taskRequest, targetVM, taskTrackerState);
        // add others such as stream locality fitness calculator
        if (logger.isDebugEnabled()) {
            logger.debug("cluster {} duration {} binpack score {} total {}", clusterFitnessValue * clusterWeight,
                    durationTypeFitness * durationTypeWeight, binPackingValue * binPackingWeight,
                    (binPackingValue * binPackingWeight + durationTypeFitness * durationTypeWeight + clusterFitnessValue * clusterWeight));
        }
        return (binPackingValue * binPackingWeight + durationTypeFitness * durationTypeWeight + clusterFitnessValue * clusterWeight) /
                (binPackingWeight + durationTypeWeight + clusterWeight);
    }

    public Func1<Double, Boolean> getFitnessGoodEnoughFunc() {
        return fitnessGoodEnoughFunc;
    }
}
