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

import io.vavr.Tuple2;


public class ClutchPIDConfig {

    public final double setPoint;
    public final Tuple2<Double, Double> rope;

    // Gain
    public final double kp;
    public final double kd;

    @java.beans.ConstructorProperties( {"setPoint", "rope", "kp", "kd"})
    public ClutchPIDConfig(double setPoint, Tuple2<Double, Double> rope, double kp, double kd) {
        this.setPoint = setPoint;
        this.rope = rope;
        this.kp = kp;
        this.kd = kd;
    }

    public double getSetPoint() {
        return this.setPoint;
    }

    public Tuple2<Double, Double> getRope() {
        return this.rope;
    }

    public double getKp() {
        return this.kp;
    }

    public double getKd() {
        return this.kd;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof ClutchPIDConfig)) return false;
        final ClutchPIDConfig other = (ClutchPIDConfig) o;
        if (Double.compare(this.getSetPoint(), other.getSetPoint()) != 0) return false;
        final Object this$rope = this.getRope();
        final Object other$rope = other.getRope();
        if (this$rope == null ? other$rope != null : !this$rope.equals(other$rope)) return false;
        if (Double.compare(this.getKp(), other.getKp()) != 0) return false;
        if (Double.compare(this.getKd(), other.getKd()) != 0) return false;
        return true;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final long $setPoint = Double.doubleToLongBits(this.getSetPoint());
        result = result * PRIME + (int) ($setPoint >>> 32 ^ $setPoint);
        final Object $rope = this.getRope();
        result = result * PRIME + ($rope == null ? 43 : $rope.hashCode());
        final long $kp = Double.doubleToLongBits(this.getKp());
        result = result * PRIME + (int) ($kp >>> 32 ^ $kp);
        final long $kd = Double.doubleToLongBits(this.getKd());
        result = result * PRIME + (int) ($kd >>> 32 ^ $kd);
        return result;
    }

    public String toString() {
        return "ClutchPIDConfig(setPoint=" + this.getSetPoint() + ", rope=" + this.getRope() + ", kp=" + this.getKp() + ", kd=" + this.getKd() + ")";
    }
}
