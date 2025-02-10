package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageScalingRule;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Builder
@Value
public class JobClusterScalerRule {
    String jobClusterName;
    StageScalingRule rule;

    public static JobClusterScalerRule fromProto(
        JobClusterScalerRuleProto.CreateScalerRuleRequest proto,
        long lastRuleIdNumber) {
        return JobClusterScalerRule.builder()
            .jobClusterName(proto.getJobClusterName())
            .rule(StageScalingRule.builder()
                .ruleId(String.valueOf(lastRuleIdNumber + 1))
                .scalerConfig(StageScalingRule.ScalerConfig.builder()
                    .type(proto.getScalerConfig().getType())
                    .desireSize(proto.getScalerConfig().getDesireSize())
                    .scalingPolicy(proto.getScalerConfig().getScalingPolicy())
                    .build())
                .triggerConfig(StageScalingRule.TriggerConfig.builder()
                    .triggerType(proto.getTriggerConfig().getTriggerType())
                    .scheduleCron(proto.getTriggerConfig().getScheduleCron())
                    .scheduleDuration(proto.getTriggerConfig().getScheduleDuration())
                    .customTrigger(proto.getTriggerConfig().getCustomTrigger())
                    .build())
                .metadata(proto.getMetadata())
                .build())
            .build();
    }

    public StageScalingRule toProto() {
        return this.rule;
    }
}
