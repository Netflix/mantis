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

package io.mantisrx.runtime.common.descriptor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;


public class StageScalingPolicy {

    private final int stage;
    private final int min;
    private final int max;
    private final boolean enabled;
    private final int increment;
    private final int decrement;
    private final long coolDownSecs;
    private final Map<ScalingReason, Strategy> strategies;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageScalingPolicy(@JsonProperty("stage") int stage,
                              @JsonProperty("min") int min, @JsonProperty("max") int max,
                              @JsonProperty("increment") int increment, @JsonProperty("decrement") int decrement,
                              @JsonProperty("coolDownSecs") long coolDownSecs,
                              @JsonProperty("strategies") Map<ScalingReason, Strategy> strategies) {
        this.stage = stage;
        this.min = min;
        this.max = Math.max(max, min);
        enabled = min != max && strategies != null && !strategies.isEmpty();
        this.increment = Math.max(increment, 1);
        this.decrement = Math.max(decrement, 1);
        this.coolDownSecs = coolDownSecs;
        this.strategies = strategies == null ? new HashMap<ScalingReason, Strategy>() : new HashMap<>(strategies);
    }

    public static void main(String[] args) {
        Map<ScalingReason, Strategy> smap = new HashMap<>();
        smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
        StageScalingPolicy policy = new StageScalingPolicy(1, 1, 2, 1, 1, 60, smap);
        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println(mapper.writeValueAsString(policy));
            String json1 = "{\"stage\":1,\"min\":1,\"max\":2,\"increment\":1,\"decrement\":1,\"strategies\":{},\"enabled\":false}";
            StageScalingPolicy sp = mapper.readValue(json1, StageScalingPolicy.class);
            //System.out.println(mapper.writeValueAsString(sp));
            String json2 = "{\"stage\":1,\"min\":1,\"max\":5,\"increment\":1,\"decrement\":1,\"coolDownSecs\":600,\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":50,\"scaleUpAbovePct\":75}},\"enabled\":true}";
            sp = mapper.readValue(json2, StageScalingPolicy.class);
            System.out.println(mapper.writeValueAsString(sp));
            String json3 = "{\"stage\":1,\"min\":1,\"max\":3,\"increment\":1,\"decrement\":1,\"coolDownSecs\":0,\"strategies\":{\"Memory\":{\"reason\":\"Memory\",\"scaleDownBelowPct\":65,\"scaleUpAbovePct\":80,\"rollingCount\":{\"count\":6,\"of\":10}}},\"enabled\":true}";
            sp = mapper.readValue(json3, StageScalingPolicy.class);
            //System.out.println(mapper.writeValueAsString(sp));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getStage() {
        return stage;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getIncrement() {
        return increment;
    }

    public int getDecrement() {
        return decrement;
    }

    public long getCoolDownSecs() {
        return coolDownSecs;
    }

    public Map<ScalingReason, Strategy> getStrategies() {
        return Collections.unmodifiableMap(strategies);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StageScalingPolicy{");
        sb.append("stage=").append(stage);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", enabled=").append(enabled);
        sb.append(", increment=").append(increment);
        sb.append(", decrement=").append(decrement);
        sb.append(", coolDownSecs=").append(coolDownSecs);
        sb.append(", strategies=").append(strategies);
        sb.append('}');
        return sb.toString();
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
            if (other.strategies != null)
                return false;
        } else if (!strategies.equals(other.strategies))
            return false;
        return true;
    }

    public enum ScalingReason {
        CPU,
        Memory,
        Network,
        DataDrop,
        KafkaLag,
        UserDefined,
        KafkaProcessed,
        Clutch,
        ClutchExperimental,
        RPS,
        JVMMemory
    }

    public static class RollingCount {

        private final int count;
        private final int of;

        @JsonCreator
        public RollingCount(@JsonProperty("count") int count, @JsonProperty("of") int of) {
            this.count = count;
            this.of = of;
        }

        public int getCount() {
            return count;
        }

        public int getOf() {
            return of;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RollingCount{");
            sb.append("count=").append(count);
            sb.append(", of=").append(of);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class Strategy {

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

        public ScalingReason getReason() {
            return reason;
        }

        public double getScaleDownBelowPct() {
            return scaleDownBelowPct;
        }

        public double getScaleUpAbovePct() {
            return scaleUpAbovePct;
        }

        public RollingCount getRollingCount() {
            return rollingCount;
        }

        @Override
        public String toString() {
            return "Strategy{" +
                    "reason=" + reason +
                    ", scaleDownBelowPct=" + scaleDownBelowPct +
                    ", scaleUpAbovePct=" + scaleUpAbovePct +
                    ", rollingCount=" + rollingCount +
                    '}';
        }
    }
}
