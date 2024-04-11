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

package io.mantisrx.runtime.descriptor;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@Getter
public class StageScalingPolicy implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int stage;
    private final int min;
    private final int max;
    private final boolean enabled;
    private final int increment;
    private final int decrement;
    private final long coolDownSecs;
    private final Map<ScalingReason, Strategy> strategies;
    /**
     * Controls whether scale down operations are allowed when the region is evacuated.
     */
    private final boolean allowScaleDownDuringEvacuated;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageScalingPolicy(@JsonProperty("stage") int stage,
                              @JsonProperty("min") int min, @JsonProperty("max") int max,
                              @JsonProperty("increment") int increment, @JsonProperty("decrement") int decrement,
                              @JsonProperty("coolDownSecs") long coolDownSecs,
                              @JsonProperty("strategies") Map<ScalingReason, Strategy> strategies,
                              @JsonProperty(value = "allowScaleDownDuringEvacuated", defaultValue = "true") Boolean allowScaleDownDuringEvacuated) {
        this.stage = stage;
        this.min = min;
        this.max = Math.max(max, min);
        enabled = min != max && strategies != null && !strategies.isEmpty();
        this.increment = Math.max(increment, 1);
        this.decrement = Math.max(decrement, 1);
        this.coolDownSecs = coolDownSecs;
        this.strategies = strategies == null ? new HashMap<ScalingReason, Strategy>() : new HashMap<>(strategies);
        // `defaultValue` is for documentation purpose only, use `Boolean` to determine if the field is missing on `null`
        this.allowScaleDownDuringEvacuated = allowScaleDownDuringEvacuated == null || allowScaleDownDuringEvacuated;
    }

    public Map<ScalingReason, Strategy> getStrategies() {
        return Collections.unmodifiableMap(strategies);
    }

    @Override
    public String toString() {
        return "StageScalingPolicy{" +
                "stage=" + stage +
                ", min=" + min +
                ", max=" + max +
                ", enabled=" + enabled +
                ", increment=" + increment +
                ", decrement=" + decrement +
                ", coolDownSecs=" + coolDownSecs +
                ", strategies=" + strategies +
                ", allowScaleDownDuringEvacuated=" + allowScaleDownDuringEvacuated +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (coolDownSecs ^ (coolDownSecs >>> 32));
        result = prime * result + decrement;
        result = prime * result + (enabled ? 1231 : 1237);
        result = prime * result + increment;
        result = prime * result + max;
        result = prime * result + min;
        result = prime * result + stage;
        result = prime * result + ((strategies == null) ? 0 : strategies.hashCode());
        result = prime * result + (allowScaleDownDuringEvacuated ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StageScalingPolicy other = (StageScalingPolicy) obj;
        if (coolDownSecs != other.coolDownSecs)
            return false;
        if (decrement != other.decrement)
            return false;
        if (enabled != other.enabled)
            return false;
        if (increment != other.increment)
            return false;
        if (max != other.max)
            return false;
        if (min != other.min)
            return false;
        if (stage != other.stage)
            return false;
        if (strategies == null) {
            return other.strategies == null;
        } else if (!strategies.equals(other.strategies)) {
            return false;
        } else return allowScaleDownDuringEvacuated == other.allowScaleDownDuringEvacuated;
    }

    public enum ScalingReason {
        CPU,
        @Deprecated
        Memory,
        Network,
        DataDrop,
        KafkaLag,
        UserDefined,
        KafkaProcessed,
        Clutch,
        ClutchExperimental,
        ClutchRps,
        RPS,
        JVMMemory,
        SourceJobDrop,
        FailoverAware
    }

    @Getter
    @ToString
    @EqualsAndHashCode
    public static class RollingCount implements Serializable {

        private static final long serialVersionUID = 1L;
        private final int count;
        private final int of;

        @JsonCreator
        public RollingCount(@JsonProperty("count") int count, @JsonProperty("of") int of) {
            this.count = count;
            this.of = of;
        }
    }

    @ToString
    @Getter
    @EqualsAndHashCode
    public static class Strategy implements Serializable {

        private static final long serialVersionUID = 1L;
        private final ScalingReason reason;
        private final double scaleDownBelowPct;
        private final double scaleUpAbovePct;
        private final RollingCount rollingCount;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Strategy(@JsonProperty("reason") ScalingReason reason,
                        @JsonProperty("scaleDownBelowPct") double scaleDownBelowPct,
                        @JsonProperty("scaleUpAbovePct") double scaleUpAbovePct,
                        @JsonProperty("rollingCount") RollingCount rollingCount) {
            this.reason = reason;
            this.scaleDownBelowPct = scaleDownBelowPct;
            this.scaleUpAbovePct = Math.max(scaleDownBelowPct, scaleUpAbovePct);
            this.rollingCount = rollingCount == null ? new RollingCount(1, 1) : rollingCount;
        }
    }
}
