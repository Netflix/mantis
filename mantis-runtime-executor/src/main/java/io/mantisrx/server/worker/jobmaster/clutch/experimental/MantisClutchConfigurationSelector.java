/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.server.worker.jobmaster.clutch.experimental;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.mantisrx.control.clutch.Clutch;
import io.mantisrx.control.clutch.ClutchConfiguration;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MantisClutchConfigurationSelector implements Function1<Map<Clutch.Metric, UpdateDoublesSketch>, ClutchConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(MantisClutchConfigurationSelector.class);

    private final Integer stageNumber;
    private final StageSchedulingInfo stageSchedulingInfo;
    private final AtomicDouble trueCpuMax = new AtomicDouble(0.0);
    private final AtomicDouble trueNetworkMax = new AtomicDouble(0.0);
    private final AtomicDouble trueCpuMin = new AtomicDouble(0.0);
    private final AtomicDouble trueNetworkMin = new AtomicDouble(0.0);
    private final long initializationTime = System.currentTimeMillis();
    private final long ONE_DAY_MILLIS = 1000 * 60 * 60 * 24;
    private final long TEN_MINUTES_MILLIS = 1000 * 60 * 10;

    public MantisClutchConfigurationSelector(Integer stageNumber, StageSchedulingInfo stageSchedulingInfo) {
        this.stageNumber = stageNumber;
        this.stageSchedulingInfo = stageSchedulingInfo;
    }

    /**
     * Determines a suitable set point within some defined bounds.
     * We use a quantile above 0.5 to encourage the function to grow over time if possible.
     * @param sketches The map of sketches for which to compute a set point. Must contain RPS.
     * @return A suitable set point for autoscaling.
     */
    private double getSetpoint(Map<Clutch.Metric, UpdateDoublesSketch> sketches, double numberOfCpuCores) {
        double setPoint = sketches.get(Clutch.Metric.RPS).getQuantile(0.75);
        double minRps = 1000 * numberOfCpuCores;
        double maxRps = 2500 * numberOfCpuCores;

        // Checking for high or low values;

        if (isSetpointHigh(sketches)
            && System.currentTimeMillis() - initializationTime > ONE_DAY_MILLIS - TEN_MINUTES_MILLIS) {
            setPoint *= 0.9;
        } else if (isSetpointLow(sketches)
            && System.currentTimeMillis() - initializationTime > ONE_DAY_MILLIS - TEN_MINUTES_MILLIS) {
            setPoint *= 1.11;
        }

        if (isUnderprovisioined(sketches)
            && System.currentTimeMillis() - initializationTime > ONE_DAY_MILLIS) {
          logger.info("Job is underprovisioned see previous messages to determine metric.");
        }

        // Sanity checking against mins / maxes

        if (setPoint < minRps) {
          logger.info("Setpoint {} was less than minimum {}. Setting to {}.", minRps, minRps);
          setPoint = minRps;
        }
        if (setPoint > maxRps) {
          logger.info("Setpoint {} was greater than maximum {}. Setting to {}.", maxRps, maxRps);
          setPoint = maxRps;
        }

        return setPoint;
    }

    @Override
    public ClutchConfiguration apply(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        updateTrueMaxValues(sketches);

        double numberOfCpuCores = stageSchedulingInfo.getMachineDefinition().getCpuCores();

        // Setpoint
        double setPoint = getSetpoint(sketches, numberOfCpuCores);

        // ROPE
        Tuple2<Double, Double> rope = Tuple.of(setPoint * 0.3, 0.0);

        // Gain
        long deltaT = stageSchedulingInfo.getScalingPolicy().getCoolDownSecs() / 30l;
        //double minMaxMidPoint = stageSchedulingInfo.getScalingPolicy().getMax() - stageSchedulingInfo.getScalingPolicy().getMin();
        double dampeningFactor = 0.33; // 0.4 caused a little oscillation too. We'll try 1/3 across each.

        double kp = 1.0 / setPoint / deltaT * stageSchedulingInfo.getScalingPolicy().getMin(); //minMaxMidPoint * dampeningFactor;
        double ki = 0.0 * dampeningFactor; // We don't want any "state" from integral gain right now.
        double kd = 1.0 / setPoint / deltaT * stageSchedulingInfo.getScalingPolicy().getMin(); // minMaxMidPoint * dampeningFactor;

        // TODO: Do we want to reset sketches, we need at least one day's values
        //resetSketches(sketches);
        return ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(setPoint)
                .kp(kp)
                .ki(ki)
                .kd(kd)
                .minSize(stageSchedulingInfo.getScalingPolicy().getMin())
                .maxSize(stageSchedulingInfo.getScalingPolicy().getMax())
                .rope(rope)
                .cooldownInterval(stageSchedulingInfo.getScalingPolicy().getCoolDownSecs())
                .cooldownUnits(TimeUnit.SECONDS)
                .build();
    }

    private void resetSketches(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        sketches.values().forEach(UpdateDoublesSketch::reset);
    }

    /**
     * Implements the rules for determine if the setpoint is too low, which runs the job too cold.
     * We are currently defining too cold as CPU or Network spending half their time below half of true max.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the setpoint is too low and we are thus running the job too cold.
     */
    private boolean isSetpointLow(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double cpuMedian = sketches.get(Clutch.Metric.CPU).getQuantile(0.5);
        double networkMedian = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.5);

        boolean cpuTooLow = cpuMedian < trueCpuMax.get() * 0.5;
        boolean networkTooLow = networkMedian < trueNetworkMax.get() * 0.5;

        if (cpuTooLow) {
            logger.info("CPU running too cold for stage {} with median {} and max {}. Recommending increase in setPoint.", stageNumber, cpuMedian, trueCpuMax.get());
        }

        if (networkTooLow) {
            logger.info("Network running too cold for stage {} with median {} and max {}. Recommending increase in setPoint.", stageNumber, networkMedian, trueNetworkMax.get());
        }

        return cpuTooLow || networkTooLow;
    }

    /**
     * Implements the rules for determine if the setpoint is too high, which in turn runs the job too hot.
     * We are currently defining a setpoint as too high if CPU or Network spend half their time over 80% of true max.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the setpoint is too high and we are thus running the job too hot.
     */
    private boolean isSetpointHigh(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double cpuMedian = sketches.get(Clutch.Metric.CPU).getQuantile(0.5);
        double networkMedian = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.5);

        // TODO: How do we ensure we're not just always operating in a tight range?
        boolean cpuTooHigh = cpuMedian > trueCpuMax.get() * 0.8
          && cpuMedian > trueCpuMin.get() * 1.2;
        boolean networkTooHigh = networkMedian > trueNetworkMax.get() * 0.8
          && networkMedian > trueNetworkMin.get() * 1.2;

        if (cpuTooHigh) {
            logger.info("CPU running too hot for stage {} with median {} and max {}. Recommending reduction in setPoint.", stageNumber, cpuMedian, trueCpuMax.get());
        }

        if (networkTooHigh) {
            logger.info("Network running too hot for stage {} with median {} and max {}. Recommending reduction in setPoint.", stageNumber, cpuMedian, trueNetworkMax.get());
        }

        return cpuTooHigh || networkTooHigh;
    }

    /**
     * Determines if a job is underprovisioned on cpu or network.
     * We are currently definiing underprovisioned as spending 20% or more of our time above the
     * provisioned resource amount.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the job is underprovisioned.
     */
    private boolean isUnderprovisioined(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double provisionedCpuLimit = stageSchedulingInfo.getMachineDefinition().getCpuCores() * 100.0;
        double provisionedNetworkLimit = stageSchedulingInfo.getMachineDefinition().getNetworkMbps() * 1024.0 * 1024.0;

        double cpu = sketches.get(Clutch.Metric.CPU).getQuantile(0.8);
        double network = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.8);

        // If we spend 20% or more of our time above the CPU provisioned limit
        boolean cpuUnderProvisioned = cpu > provisionedCpuLimit;
        boolean networkUnderProvisioned = network > provisionedNetworkLimit;

        if (cpuUnderProvisioned) {
          logger.error("CPU is underprovisioned! 80% percentile {}% is above provisioned {}%.", cpu, provisionedCpuLimit);
        }

        if (networkUnderProvisioned) {
          logger.error("Network is underprovisioned! 80% percentile {}% is above provisioned {}%.", network, provisionedNetworkLimit);
        }

        return cpuUnderProvisioned || networkUnderProvisioned;
    }

    /**
     * Performs bookkeeping on true maximum values.
     * We need to do this because we reset the sketches constantly so their max is impacted by current setPoint.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     */
    private void updateTrueMaxValues(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
      double cpuMax = sketches.get(Clutch.Metric.CPU).getMaxValue();
      double networkMax = sketches.get(Clutch.Metric.NETWORK).getMaxValue();

      if (cpuMax > trueCpuMax.get()) {
        trueCpuMax.set(cpuMax);
      }
      if (networkMax > trueNetworkMax.get()) {
        trueNetworkMax.set(networkMax);
      }

      double cpuMin = sketches.get(Clutch.Metric.CPU).getMinValue();
      double networkMin = sketches.get(Clutch.Metric.NETWORK).getMinValue();

      if (cpuMin < trueCpuMin.get()) {
        trueCpuMin.set(cpuMin);
      }
      if (networkMin < trueNetworkMin.get()) {
        trueNetworkMin.set(networkMin);
      }
    }
}
