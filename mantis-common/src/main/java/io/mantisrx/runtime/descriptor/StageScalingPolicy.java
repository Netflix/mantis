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
@ToString
@EqualsAndHashCode
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
     * Controls whether AutoScaleManager is enabled or disabled
     */
    private final boolean allowAutoScaleManager;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageScalingPolicy(@JsonProperty("stage") int stage,
                              @JsonProperty("min") int min, @JsonProperty("max") int max,
                              @JsonProperty("increment") int increment, @JsonProperty("decrement") int decrement,
                              @JsonProperty("coolDownSecs") long coolDownSecs,
                              @JsonProperty("strategies") Map<ScalingReason, Strategy> strategies,
                              @JsonProperty(value = "allowAutoScaleManager", defaultValue = "false") Boolean allowAutoScaleManager) {
        this.stage = stage;
        this.min = min;
        this.max = Math.max(max, min);
        enabled = min != max && strategies != null && !strategies.isEmpty();
        this.increment = Math.max(increment, 1);
        this.decrement = Math.max(decrement, 1);
        this.coolDownSecs = coolDownSecs;
        this.strategies = strategies == null ? new HashMap<ScalingReason, Strategy>() : new HashMap<>(strategies);
        // `defaultValue` is for documentation purpose only, use `Boolean` to determine if the field is missing on `null`
        this.allowAutoScaleManager = allowAutoScaleManager == Boolean.TRUE;
    }

    public Map<ScalingReason, Strategy> getStrategies() {
        return Collections.unmodifiableMap(strategies);
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
        AutoscalerManagerEvent
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
