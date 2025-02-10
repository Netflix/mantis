package io.mantisrx.runtime.descriptor;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Map;

@Builder
@Value
public class StageScalingRule implements Serializable {
    private static final long serialVersionUID = 1L;

    String ruleId;
    ScalerConfig scalerConfig;
    TriggerConfig triggerConfig;
    Map<String, String> metadata;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public StageScalingRule(
        @JsonProperty("ruleId") String ruleId,
        @JsonProperty("scalerConfig") ScalerConfig scalerConfig,
        @JsonProperty("triggerConfig") TriggerConfig triggerConfig,
        @JsonProperty("metadata") Map<String, String> metadata) {
        this.ruleId = ruleId;
        this.scalerConfig = scalerConfig;
        this.triggerConfig = triggerConfig;
        this.metadata = metadata;
    }

    @Builder
    @Value
    public static class ScalerConfig {
        String type; // only support standard scaling policy for now
        StageScalingPolicy scalingPolicy;

        /**
         * Desired size when this config is triggered.
         */
        int desireSize;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public ScalerConfig(
            @JsonProperty("type") String type,
            @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
            @JsonProperty("desireSize") int desireSize) {
            this.type = type;
            this.scalingPolicy = scalingPolicy;
            this.desireSize = desireSize;
        }

    }

    @Builder
    @Value
    public static class TriggerConfig {
        String triggerType;
        String scheduleCron;
        String scheduleDuration;
        String customTrigger;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public TriggerConfig(
            @JsonProperty("type") String triggerType,
            @JsonProperty("scalingPolicy") String scheduleCron,
            @JsonProperty("scalingPolicy") String scheduleDuration,
            @JsonProperty("desireSize") String customTrigger) {
            this.triggerType = triggerType;
            this.scheduleCron = scheduleCron;
            this.scheduleDuration = scheduleDuration;
            this.customTrigger = customTrigger;
        }
    }
}
