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

package io.mantisrx.server.worker.jobmaster.clutch;

import io.mantisrx.server.worker.jobmaster.clutch.rps.ClutchRpsPIDConfig;
import io.vavr.control.Option;


public class ClutchConfiguration {

    public final int minSize;
    public final int maxSize;
    public final double rps;

    public final Option<Long> minSamples;
    public final Option<Long> cooldownSeconds;
    public final Option<Double> panicThresholdSeconds;
    public final Option<Double> maxAdjustment;
    public final Option<Boolean> useExperimental;
    public final Option<Double> integralDecay;

    public final Option<ClutchPIDConfig> cpu;
    public final Option<ClutchPIDConfig> memory;
    public final Option<ClutchPIDConfig> network;
    public final Option<ClutchRpsPIDConfig> rpsConfig;

    @java.beans.ConstructorProperties( {"minSize", "maxSize", "rps", "minSamples", "cooldownSeconds", "panicThresholdSeconds", "maxAdjustment", "cpu", "memory", "network", "rpsConfig", "useExperimental", "integralDecay"})
    public ClutchConfiguration(int minSize, int maxSize, double rps, Option<Long> minSamples, Option<Long> cooldownSeconds,
                               Option<Double> panicThresholdSeconds, Option<Double> maxAdjustment, Option<ClutchPIDConfig> cpu,
                               Option<ClutchPIDConfig> memory, Option<ClutchPIDConfig> network, Option<ClutchRpsPIDConfig> rpsConfig,
                               Option<Boolean> useExperimental, Option<Double> integralDecay) {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.rps = rps;
        this.minSamples = minSamples;
        this.cooldownSeconds = cooldownSeconds;
        this.panicThresholdSeconds = panicThresholdSeconds;
        this.maxAdjustment = maxAdjustment;
        this.cpu = cpu;
        this.memory = memory;
        this.network = network;
        this.rpsConfig = rpsConfig;
        this.useExperimental = useExperimental;
        this.integralDecay = integralDecay;
    }

    public int getMinSize() {
        return this.minSize;
    }

    public int getMaxSize() {
        return this.maxSize;
    }

    public double getRps() {
        return this.rps;
    }

    public Option<Long> getMinSamples() {
        return this.minSamples;
    }

    public Option<Long> getCooldownSeconds() {
        return this.cooldownSeconds;
    }

    public Option<Double> getPanicThresholdSeconds() {
        return this.panicThresholdSeconds;
    }

    public Option<Double> getMaxAdjustment() {
        return this.maxAdjustment;
    }

    public Option<Boolean> getUseExperimental() {
        return this.useExperimental;
    }

    public Option<Double> getIntegralDecay() {
        return this.integralDecay;
    }

    public Option<ClutchPIDConfig> getCpu() {
        return this.cpu;
    }

    public Option<ClutchPIDConfig> getMemory() {
        return this.memory;
    }

    public Option<ClutchPIDConfig> getNetwork() {
        return this.network;
    }

    public Option<ClutchRpsPIDConfig> getRpsConfig() {
        return this.rpsConfig;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof ClutchConfiguration)) return false;
        final ClutchConfiguration other = (ClutchConfiguration) o;
        if (this.getMinSize() != other.getMinSize()) return false;
        if (this.getMaxSize() != other.getMaxSize()) return false;
        if (Double.compare(this.getRps(), other.getRps()) != 0) return false;
        final Object this$minSamples = this.getMinSamples();
        final Object other$minSamples = other.getMinSamples();
        if (this$minSamples == null ? other$minSamples != null : !this$minSamples.equals(other$minSamples))
            return false;
        final Object this$cooldownSeconds = this.getCooldownSeconds();
        final Object other$cooldownSeconds = other.getCooldownSeconds();
        if (this$cooldownSeconds == null ? other$cooldownSeconds != null : !this$cooldownSeconds.equals(other$cooldownSeconds))
            return false;
        final Object this$panicThresholdSeconds = this.getPanicThresholdSeconds();
        final Object other$panicThresholdSeconds = other.getPanicThresholdSeconds();
        if (this$panicThresholdSeconds == null ? other$panicThresholdSeconds != null : !this$panicThresholdSeconds.equals(other$panicThresholdSeconds))
            return false;
        final Object this$maxAdjustment = this.getMaxAdjustment();
        final Object other$maxAdjustment = other.getMaxAdjustment();
        if (this$maxAdjustment == null ? other$maxAdjustment != null : !this$maxAdjustment.equals(other$maxAdjustment))
            return false;
        final Object this$cpu = this.getCpu();
        final Object other$cpu = other.getCpu();
        if (this$cpu == null ? other$cpu != null : !this$cpu.equals(other$cpu)) return false;
        final Object this$memory = this.getMemory();
        final Object other$memory = other.getMemory();
        if (this$memory == null ? other$memory != null : !this$memory.equals(other$memory)) return false;
        final Object this$network = this.getNetwork();
        final Object other$network = other.getNetwork();
        if (this$network == null ? other$network != null : !this$network.equals(other$network)) return false;
        final Object this$rpsConfig = this.getRpsConfig();
        final Object other$rpsConfig = other.getRpsConfig();
        if (this$rpsConfig == null ? other$rpsConfig != null : !this$rpsConfig.equals(other$rpsConfig)) return false;
        final Object this$useExperimental = this.getUseExperimental();
        final Object other$useExperimental = other.getUseExperimental();
        if (this$useExperimental == null ? other$useExperimental != null : !this$useExperimental.equals(other$useExperimental)) return false;
        final Object this$integralDecay = this.getIntegralDecay();
        final Object other$integralDecay = other.getIntegralDecay();
        if (this$integralDecay == null ? other$integralDecay != null : !this$integralDecay.equals(other$integralDecay))
            return false;
        return true;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + this.getMinSize();
        result = result * PRIME + this.getMaxSize();
        final long $rps = Double.doubleToLongBits(this.getRps());
        result = result * PRIME + (int) ($rps >>> 32 ^ $rps);
        final Object $minSamples = this.getMinSamples();
        result = result * PRIME + ($minSamples == null ? 43 : $minSamples.hashCode());
        final Object $cooldownSeconds = this.getCooldownSeconds();
        result = result * PRIME + ($cooldownSeconds == null ? 43 : $cooldownSeconds.hashCode());
        final Object $panicThresholdSeconds = this.getPanicThresholdSeconds();
        result = result * PRIME + ($panicThresholdSeconds == null ? 43 : $panicThresholdSeconds.hashCode());
        final Object $maxAdjustment = this.getMaxAdjustment();
        result = result * PRIME + ($maxAdjustment == null ? 43 : $maxAdjustment.hashCode());
        final Object $cpu = this.getCpu();
        result = result * PRIME + ($cpu == null ? 43 : $cpu.hashCode());
        final Object $memory = this.getMemory();
        result = result * PRIME + ($memory == null ? 43 : $memory.hashCode());
        final Object $network = this.getNetwork();
        result = result * PRIME + ($network == null ? 43 : $network.hashCode());
        final Object $rpsConfig = this.getRpsConfig();
        result = result * PRIME + ($rpsConfig == null ? 43 : $rpsConfig.hashCode());
        final Object $useExperimental = this.getUseExperimental();
        result = result * PRIME + ($useExperimental == null ? 43 : $useExperimental.hashCode());
        final Object $integralDecay = this.getIntegralDecay();
        result = result * PRIME + ($integralDecay == null ? 43 : $integralDecay.hashCode());
        return result;
    }

    public String toString() {
        return "ClutchConfiguration(minSize=" + this.getMinSize() + ", maxSize=" + this.getMaxSize() + ", rps=" + this.getRps() +
                ", minSamples=" + this.getMinSamples() + ", cooldownSeconds=" + this.getCooldownSeconds() +
                ", panicThresholdSeconds=" + this.getPanicThresholdSeconds() + ", maxAdjustment=" + this.getMaxAdjustment() +
                ", cpu=" + this.getCpu() + ", memory=" + this.getMemory() + ", network=" + this.getNetwork() +
                ", rpsConfig=" + this.getRpsConfig() + ", useExperimental=" + this.getUseExperimental() +
                ", integralDecay=" + this.getIntegralDecay() + ")";
    }
}
