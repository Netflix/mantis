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

package io.mantisrx.server.worker.jobmaster.control;

import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;


public class AdaptiveAutoscalerConfig {

    public final StageScalingPolicy.ScalingReason metric;

    public final double setPoint;
    public final boolean invert;
    public final double rope;

    // Gain
    public final double kp;
    public final double ki;
    public final double kd;

    public final double minScale;
    public final double maxScale;

    @java.beans.ConstructorProperties( {"metric", "setPoint", "invert", "rope", "kp", "ki", "kd", "minScale", "maxScale"})
    public AdaptiveAutoscalerConfig(StageScalingPolicy.ScalingReason metric, double setPoint, boolean invert, double rope, double kp, double ki, double kd, double minScale, double maxScale) {
        this.metric = metric;
        this.setPoint = setPoint;
        this.invert = invert;
        this.rope = rope;
        this.kp = kp;
        this.ki = ki;
        this.kd = kd;
        this.minScale = minScale;
        this.maxScale = maxScale;
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String cfg = "{\"metric\": \"KafkaProcessed\", \"setPoint\": 1, \"invert\": true, \"rope\": 0, \"kp\": 0.1, \"ki\": 0.00, \"kd\": 0.00, \"minScale\": 1, \"maxScale\": 5}\n";
        AdaptiveAutoscalerConfig config = objectMapper
                .readValue(cfg,
                        new TypeReference<AdaptiveAutoscalerConfig>() {});
        System.out.println(config.toString());

    }

    public StageScalingPolicy.ScalingReason getMetric() {
        return this.metric;
    }

    public double getSetPoint() {
        return this.setPoint;
    }

    public boolean isInvert() {
        return this.invert;
    }

    public double getRope() {
        return this.rope;
    }

    public double getKp() {
        return this.kp;
    }

    public double getKi() {
        return this.ki;
    }

    public double getKd() {
        return this.kd;
    }

    public double getMinScale() {
        return this.minScale;
    }

    public double getMaxScale() {
        return this.maxScale;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof AdaptiveAutoscalerConfig)) return false;
        final AdaptiveAutoscalerConfig other = (AdaptiveAutoscalerConfig) o;
        final Object this$metric = this.getMetric();
        final Object other$metric = other.getMetric();
        if (this$metric == null ? other$metric != null : !this$metric.equals(other$metric)) return false;
        if (Double.compare(this.getSetPoint(), other.getSetPoint()) != 0) return false;
        if (this.isInvert() != other.isInvert()) return false;
        if (Double.compare(this.getRope(), other.getRope()) != 0) return false;
        if (Double.compare(this.getKp(), other.getKp()) != 0) return false;
        if (Double.compare(this.getKi(), other.getKi()) != 0) return false;
        if (Double.compare(this.getKd(), other.getKd()) != 0) return false;
        if (Double.compare(this.getMinScale(), other.getMinScale()) != 0) return false;
        if (Double.compare(this.getMaxScale(), other.getMaxScale()) != 0) return false;
        return true;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $metric = this.getMetric();
        result = result * PRIME + ($metric == null ? 43 : $metric.hashCode());
        final long $setPoint = Double.doubleToLongBits(this.getSetPoint());
        result = result * PRIME + (int) ($setPoint >>> 32 ^ $setPoint);
        result = result * PRIME + (this.isInvert() ? 79 : 97);
        final long $rope = Double.doubleToLongBits(this.getRope());
        result = result * PRIME + (int) ($rope >>> 32 ^ $rope);
        final long $kp = Double.doubleToLongBits(this.getKp());
        result = result * PRIME + (int) ($kp >>> 32 ^ $kp);
        final long $ki = Double.doubleToLongBits(this.getKi());
        result = result * PRIME + (int) ($ki >>> 32 ^ $ki);
        final long $kd = Double.doubleToLongBits(this.getKd());
        result = result * PRIME + (int) ($kd >>> 32 ^ $kd);
        final long $minScale = Double.doubleToLongBits(this.getMinScale());
        result = result * PRIME + (int) ($minScale >>> 32 ^ $minScale);
        final long $maxScale = Double.doubleToLongBits(this.getMaxScale());
        result = result * PRIME + (int) ($maxScale >>> 32 ^ $maxScale);
        return result;
    }

    public String toString() {
        return "AdaptiveAutoscalerConfig(metric=" + this.getMetric() + ", setPoint=" + this.getSetPoint() + ", invert=" + this.isInvert() + ", rope=" + this.getRope() + ", kp=" + this.getKp() + ", ki=" + this.getKi() + ", kd=" + this.getKd() + ", minScale=" + this.getMinScale() + ", maxScale=" + this.getMaxScale() + ")";
    }
}
