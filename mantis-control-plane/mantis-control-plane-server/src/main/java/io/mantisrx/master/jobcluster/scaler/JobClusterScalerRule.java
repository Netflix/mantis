package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class JobClusterScalerRule {
    String jobClusterName;
    JobScalingRule rule;

    public static JobClusterScalerRule fromProto(
        JobClusterScalerRuleProto.CreateScalerRuleRequest proto,
        long lastRuleIdNumber) {
        return JobClusterScalerRule.builder()
            .jobClusterName(proto.getJobClusterName())
            .rule(JobScalingRule.builder()
                .ruleId(String.valueOf(lastRuleIdNumber + 1))
                .scalerConfig(JobScalingRule.ScalerConfig.builder()
                    .type(proto.getScalerConfig().getType())
                    .stageDesireSize(proto.getScalerConfig().getStageDesireSize())
                    .scalingPolicies(proto.getScalerConfig().getScalingPolicies())
                    .build())
                .triggerConfig(JobScalingRule.TriggerConfig.builder()
                    .triggerType(proto.getTriggerConfig().getTriggerType())
                    .scheduleCron(proto.getTriggerConfig().getScheduleCron())
                    .scheduleDuration(proto.getTriggerConfig().getScheduleDuration())
                    .customTrigger(proto.getTriggerConfig().getCustomTrigger())
                    .build())
                .metadata(proto.getMetadata())
                .build())
            .build();
    }

    public JobScalingRule toProto() {
        return this.rule;
    }
}
