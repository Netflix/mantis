package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Builder
@Value
public class JobClusterScalerRule {
    String jobClusterName;
    String ruleId;
    ScalerConfig scalerConfig;
    TriggerConfig triggerConfig;
    Map<String, String> metadata;

    // todo add json creator and json property annotations on ctor

    @Builder
    @Value
    public static class ScalerConfig {
        String type; // only support standard scaling policy for now
        StageScalingPolicy scalingPolicy;

        /**
         * Desired size when this config is triggered.
         */
        int desireSize;
    }

    @Builder
    @Value
    public static class TriggerConfig {
        String triggerType;
        String scheduleCron;
        String scheduleDuration;
        String customTrigger;
    }

    public static JobClusterScalerRule fromProto(
        JobClusterScalerRuleProto.CreateScalerRuleRequest proto,
        long lastRuleIdNumber) {
        return JobClusterScalerRule.builder()
            .jobClusterName(proto.getJobClusterName())
            .ruleId(String.valueOf(lastRuleIdNumber + 1))
            .scalerConfig(ScalerConfig.builder()
                .type(proto.getScalerConfig().getType())
                .desireSize(proto.getScalerConfig().getDesireSize())
                .scalingPolicy(proto.getScalerConfig().getScalingPolicy())
                .build())
            .triggerConfig(TriggerConfig.builder()
                .triggerType(proto.getTriggerConfig().getTriggerType())
                .scheduleCron(proto.getTriggerConfig().getScheduleCron())
                .scheduleDuration(proto.getTriggerConfig().getScheduleDuration())
                .customTrigger(proto.getTriggerConfig().getCustomTrigger())
                .build())
            .metadata(proto.getMetadata())
            .build();
    }

    public JobClusterScalerRuleProto.ScalerRule toProto() {
        return JobClusterScalerRuleProto.ScalerRule.builder()
            .ruleId(this.getRuleId())
            .scalerConfig(JobClusterScalerRuleProto.ScalerConfig.builder()
                .type(this.getScalerConfig().getType())
                .desireSize(this.getScalerConfig().getDesireSize())
                .scalingPolicy(this.getScalerConfig().getScalingPolicy())
                .build())
            .triggerConfig(JobClusterScalerRuleProto.TriggerConfig.builder()
                .triggerType(this.getTriggerConfig().getTriggerType())
                .scheduleCron(this.getTriggerConfig().getScheduleCron())
                .scheduleDuration(this.getTriggerConfig().getScheduleDuration())
                .customTrigger(this.getTriggerConfig().getCustomTrigger())
                .build())
            .metadata(this.getMetadata())
            .build();
    }
}
