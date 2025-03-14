package io.mantisrx.runtime.descriptor;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Builder
@Value
public class JobScalingRule implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String TRIGGER_TYPE_SCHEDULE = "schedule";
    public static final String TRIGGER_TYPE_PERPETUAL = "perpetual";
    public static final String TRIGGER_TYPE_CUSTOM = "custom";

    /**
     * Unique identifier for this rule. By default, this is an int value starting from 0.
     * -1 is reserved for default rule.
     */
    String ruleId;
    ScalerConfig scalerConfig;
    TriggerConfig triggerConfig;
    Map<String, String> metadata;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobScalingRule(
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
        /**
         * Only support standard scaling policy for now
         */
        String type;

        /**
         * Stage specific scaling policy. The key should be stage num in int.
         */
        @Builder.Default
        Map<String, StageScalerConfig> stageConfigMap = Collections.emptyMap();

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public ScalerConfig(
            @JsonProperty("type") String type,
            @JsonProperty("stageConfigMap") Map<String, StageScalerConfig> stageConfigMap) {
            this.type = type;
            this.stageConfigMap = stageConfigMap;
        }

        @JsonIgnore
        public Optional<StageScalerConfig> getScalerConfigByStageNum(int stageNum) {
            return Optional.ofNullable(stageConfigMap.getOrDefault(String.valueOf(stageNum), null));
        }
    }

    @Builder
    @Value
    public static class StageScalerConfig {
        @Nullable
        StageScalingPolicy scalingPolicy;

        /**
         * Desired size when this config is triggered.
         * If no desire size is needed, set it to null.
         */
        @Nullable
        Integer desireSize;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public StageScalerConfig(
            @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
            @JsonProperty("desireSize") Integer desireSize) {
            this.scalingPolicy = scalingPolicy;
            this.desireSize = desireSize;
        }
    }

    @Builder
    @Value
    public static class TriggerConfig {
        /**
         * Supported values are "schedule", "perpetual", "custom" defined in JobScalingRule constants.
         */
        String triggerType;
        String scheduleCron;
        String scheduleDuration;
        String customTrigger;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public TriggerConfig(
            @JsonProperty("triggerType") String triggerType,
            @JsonProperty("scheduleCron") String scheduleCron,
            @JsonProperty("scheduleDuration") String scheduleDuration,
            @JsonProperty("customTrigger") String customTrigger) {
            this.triggerType = triggerType;
            this.scheduleCron = scheduleCron;
            this.scheduleDuration = scheduleDuration;
            this.customTrigger = customTrigger;
        }
    }
}
