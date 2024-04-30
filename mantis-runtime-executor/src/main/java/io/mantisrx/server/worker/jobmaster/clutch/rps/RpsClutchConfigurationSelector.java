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

package io.mantisrx.server.worker.jobmaster.clutch.rps;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.mantisrx.control.clutch.Clutch;
import io.mantisrx.control.clutch.ClutchConfiguration;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.vavr.Function1;
import io.vavr.Tuple2;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RpsClutchConfigurationSelector implements Function1<Map<Clutch.Metric, UpdateDoublesSketch>, ClutchConfiguration> {
    private static final double DEFAULT_INTEGRAL_DECAY = 0.1;
    private final Integer stageNumber;
    private final StageSchedulingInfo stageSchedulingInfo;
    private final io.mantisrx.server.worker.jobmaster.clutch.ClutchConfiguration customConfig;
    private ClutchConfiguration prevConfig;

    public RpsClutchConfigurationSelector(Integer stageNumber, StageSchedulingInfo stageSchedulingInfo, io.mantisrx.server.worker.jobmaster.clutch.ClutchConfiguration customConfig) {
        this.stageNumber = stageNumber;
        this.stageSchedulingInfo = stageSchedulingInfo;
        this.customConfig = customConfig;
    }

    @Override
    public ClutchConfiguration apply(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double setPoint = getSetpoint(sketches);
        Tuple2<Double, Double> rope = getRope().map(x -> x / 100.0 * setPoint, y -> y / 100.0 * setPoint);

        // Gain - number of ticks within the cooldown period. This is the minimum number of times PID output will accumulate
        // before an action is taken.
        long deltaT = getCooldownSecs() / 30l;

        double kp = 1.0 / Math.max(setPoint, 1.0) / Math.max(getCumulativeIntegralDivisor(getIntegralScaler(), deltaT), 1.0);
        double ki = 0.0;
        double kd = 1.0 / Math.max(setPoint, 1.0) / Math.max(getCumulativeIntegralDivisor(getIntegralScaler(), deltaT), 1.0);

        ClutchConfiguration config = ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(setPoint)
                .kp(kp)
                .ki(ki)
                .kd(kd)
                .integralDecay(getIntegralScaler())
                .minSize(getMinSize())
                .maxSize(getMaxSize())
                .rope(rope)
                .cooldownInterval(getCooldownSecs())
                .cooldownUnits(TimeUnit.SECONDS)
                .build();

        // If config is similar to previous, don't return a new config which would trigger a PID reset.
        if (isSimilarToPreviousConfig(config)) {
            return prevConfig;
        }
        prevConfig = config;
        return config;
    }

    private double getSetpoint(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        UpdateDoublesSketch rpsSketch = sketches.get(Clutch.Metric.RPS);
        double setPoint = rpsSketch.getQuantile(getSetPointPercentile());

        // Check if set point drifted too low due to distribution skewing lower.
        if (rpsSketch.getQuantile(0.99) * getSetPointPercentile() > setPoint) {
            setPoint = setPoint * 1.1;
        }

        return setPoint;
    }

    private double getSetPointPercentile() {
        if (customConfig != null && customConfig.getRpsConfig().isDefined()) {
            return customConfig.getRpsConfig().get().getSetPointPercentile() / 100.0;
        }
        return ClutchRpsPIDConfig.DEFAULT.getSetPointPercentile() / 100.0;
    }

    private Tuple2<Double, Double> getRope() {
        if (customConfig != null && customConfig.getRpsConfig().isDefined()) {
            return customConfig.getRpsConfig().get().getRope();
        }
        return ClutchRpsPIDConfig.DEFAULT.getRope();
    }

    private int getMinSize() {
        if (customConfig != null && customConfig.getMinSize() > 0) {
            return customConfig.getMinSize();
        }
        if (stageSchedulingInfo.getScalingPolicy() != null && stageSchedulingInfo.getScalingPolicy().getMin() > 0) {
            return stageSchedulingInfo.getScalingPolicy().getMin();
        }
        return stageSchedulingInfo.getNumberOfInstances();
    }

    private int getMaxSize() {
        if (customConfig != null && customConfig.getMaxSize() > 0) {
            return customConfig.getMaxSize();
        }
        if (stageSchedulingInfo.getScalingPolicy() != null && stageSchedulingInfo.getScalingPolicy().getMax() > 0) {
            return stageSchedulingInfo.getScalingPolicy().getMax();
        }
        return stageSchedulingInfo.getNumberOfInstances();
    }

    private long getCooldownSecs() {
        if (customConfig != null && customConfig.getCooldownSeconds().isDefined()) {
            return customConfig.getCooldownSeconds().get();
        }
        if (stageSchedulingInfo.getScalingPolicy() != null) {
            return stageSchedulingInfo.getScalingPolicy().getCoolDownSecs();
        }
        return 0;
    }

    private double getIntegralScaler() {
        if (customConfig != null && customConfig.getIntegralDecay().isDefined()) {
            return 1.0 - customConfig.getIntegralDecay().get();
        }
        return 1.0 - DEFAULT_INTEGRAL_DECAY;
    }

    private boolean isSimilarToPreviousConfig(ClutchConfiguration curConfig) {
        if (prevConfig == null) {
            return false;
        }
        double prevSetPoint = prevConfig.getSetPoint();
        double curSetPoint = curConfig.getSetPoint();

        // Consider the config similar if setPoint is within 5%.
        return curSetPoint >= prevSetPoint * 0.95 && curSetPoint <= prevSetPoint * 1.05;
    }

    private double getCumulativeIntegralDivisor(double integralScaler, long count) {
        double result = 0.0;
        for (int i = 0; i < count; i++) {
            result = result * integralScaler + 1.0;
        }
        return result;
    }
}
