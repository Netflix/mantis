package io.mantisrx.server.worker.jobmaster.rules;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class RuleUtils {
    public static JobScalingRule getDefaultScalingRule(SchedulingInfo schedulingInfo) {
        if (schedulingInfo == null ||
            schedulingInfo.getStages().entrySet().stream()
                .noneMatch(kv -> kv.getValue().getScalingPolicy() != null)) {
            return null;
        }

        return JobScalingRule.builder()
            .ruleId(String.valueOf(Integer.MIN_VALUE)) // set default rule id to Integer.MIN_VALUE
            .scalerConfig(JobScalingRule.ScalerConfig.builder()
                .scalingPolicies(schedulingInfo.getStages().values().stream()
                    .map(StageSchedulingInfo::getScalingPolicy)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()))
                .stageDesireSize(schedulingInfo.getStages().entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getNumberOfInstances())))
                .build())
            .build();
    }

    public static boolean isPerpetualRule(JobScalingRule rule) {
        return rule.getTriggerConfig() == null ||
            rule.getTriggerConfig().getTriggerType() == null ||
            rule.getTriggerConfig().getTriggerType().equals(JobScalingRule.TRIGGER_TYPE_PERPETUAL);
    }
}
