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

import io.mantisrx.server.worker.jobmaster.clutch.ClutchPIDConfig;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;

public class ClutchRpsPIDConfig extends ClutchPIDConfig {

    public static final ClutchRpsPIDConfig DEFAULT = new ClutchRpsPIDConfig(0.0, Tuple.of(30.0, 0.0), 0.0, 0.0,
            Option.of(75.0), Option.of(0.0), Option.of(0.0), Option.of(1.0), Option.of(1.0));

    /**
     * Percentile of RPS data points to use as set point. 99.0 means P99.
     */
    public final double setPointPercentile;

    /**
     * Percentage threshold for scaling up. Use to delay scaling up during until a threshold is reached, effectively
     * reducing the number of scaling activities. 10.0 means 10%.
     */
    public final double scaleUpAbovePct;


    /**
     * Percentage threshold for scaling down. Use to delay scaling down during until a threshold is reached, effectively
     * reducing the number of scaling activities. 10.0 means 10%.
     */
    public final double scaleDownBelowPct;

    /**
     * Scale up multiplier. Use to artificially increase/decrease size of scale up. Can be used to reduce number of
     * scaling activities. However, a large number can cause over provisioning and lead to oscillation.
     */
    public final double scaleUpMultiplier;

    /**
     * Scale down multiplier. Use to artificially increase/decrease size of scale down. Can be used to reduce number of
     * scaling activities. However, a large number can cause under provisioning and lead to oscillation.
     */
    public final double scaleDownMultiplier;

    @java.beans.ConstructorProperties( {"setPoint", "rope", "kp", "kd", "setPointPercentile", "scaleUpAbovePct", "scaleDownBelowPct", "scaleUpMultiplier", "scaleDownMultiplier"})
    public ClutchRpsPIDConfig(double setPoint, Tuple2<Double, Double> rope, double kp, double kd,
                              Option<Double> setPointPercentile,
                              Option<Double> scaleUpAbovePct,
                              Option<Double> scaleDownBelowPct,
                              Option<Double> scaleUpMultiplier,
                              Option<Double> scaleDownMultiplier) {
        super(setPoint, rope == null ? DEFAULT.getRope() : rope, kp, kd);
        this.setPointPercentile = setPointPercentile.getOrElse(() -> DEFAULT.getSetPointPercentile());
        this.scaleUpAbovePct = scaleUpAbovePct.getOrElse(() -> DEFAULT.getScaleUpAbovePct());
        this.scaleDownBelowPct = scaleDownBelowPct.getOrElse(() -> DEFAULT.getScaleDownBelowPct());
        this.scaleUpMultiplier = scaleUpMultiplier.getOrElse(() -> DEFAULT.getScaleUpMultiplier());
        this.scaleDownMultiplier = scaleDownMultiplier.getOrElse(() -> DEFAULT.getScaleDownMultiplier());

    }

    public double getSetPointPercentile() {
        return setPointPercentile;
    }

    public double getScaleUpAbovePct() {
        return scaleUpAbovePct;
    }

    public double getScaleDownBelowPct() {
        return scaleDownBelowPct;
    }

    public double getScaleUpMultiplier() {
        return scaleUpMultiplier;
    }

    public double getScaleDownMultiplier() {
        return scaleDownMultiplier;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof ClutchRpsPIDConfig)) return false;
        final ClutchRpsPIDConfig other = (ClutchRpsPIDConfig) o;

        if (!super.equals(o)) return false;

        if (Double.compare(this.getSetPointPercentile(), other.getSetPointPercentile()) != 0) return false;
        if (Double.compare(this.getScaleUpAbovePct(), other.getScaleUpAbovePct()) != 0) return false;
        if (Double.compare(this.getScaleDownBelowPct(), other.getScaleDownBelowPct()) != 0) return false;
        if (Double.compare(this.getScaleUpMultiplier(), other.getScaleUpMultiplier()) != 0) return false;
        if (Double.compare(this.getScaleDownMultiplier(), other.getScaleDownMultiplier()) != 0) return false;
        return true;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = super.hashCode();
        final long $setPointPercentile = Double.doubleToLongBits(this.getSetPointPercentile());
        result = result * PRIME + (int) ($setPointPercentile >>> 32 ^ $setPointPercentile);
        final long $scaleUpAbovePct = Double.doubleToLongBits(this.getScaleUpAbovePct());
        result = result * PRIME + (int) ($scaleUpAbovePct >>> 32 ^ $scaleUpAbovePct);
        final long $scaleDownBelowPct = Double.doubleToLongBits(this.getScaleDownBelowPct());
        result = result * PRIME + (int) ($scaleDownBelowPct >>> 32 ^ $scaleDownBelowPct);
        final long $scaleUpMultiplier = Double.doubleToLongBits(this.getScaleUpMultiplier());
        result = result * PRIME + (int) ($scaleUpMultiplier >>> 32 ^ $scaleUpMultiplier);
        final long $scaleDownMultiplier = Double.doubleToLongBits(this.getScaleDownMultiplier());
        result = result * PRIME + (int) ($scaleDownMultiplier >>> 32 ^ $scaleDownMultiplier);
        return result;
    }

    public String toString() {
        return "ClutchRPSPIDConfig(setPoint=" + this.getSetPoint() + ", rope=" + this.getRope() + ", kp=" + this.getKp() + ", kd=" + this.getKd() +
                ", setPointPercentile=" + this.getSetPointPercentile() +
                ", scaleUpAbovePct=" + this.getScaleUpAbovePct() + ", scaleDownBelowPct=" + this.getScaleDownBelowPct() +
                ", scaleUpMultiplier=" + this.getScaleUpMultiplier() + ", scaleDownMultiplier=" + this.getScaleDownMultiplier() + ")";
    }
}
