package io.mantisrx.server.worker.jobmaster.rules;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RuleUtils {
    public static JobScalingRule getDefaultScalingRule(SchedulingInfo schedulingInfo) {
        if (schedulingInfo == null ||
            schedulingInfo.getStages().entrySet().stream()
                .noneMatch(kv -> kv.getValue().getScalingPolicy() != null)) {
            return null;
        }

        return JobScalingRule.builder()
            .ruleId(String.valueOf(-1)) // set default rule id to -1
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

    public static Comparator<String> defaultIntValueRuleIdComparator() {
        return Comparator.comparingInt(Integer::parseInt);
    }

    public static Func1<Observable<? extends Throwable>, Observable<?>> LimitTenRetryLogic =
        attempts -> attempts
        .zipWith(Observable.range(1, 10), (Func2<Throwable, Integer, Integer>) (t1, integer) -> integer)
        .flatMap((Func1<Integer, Observable<?>>) integer -> {
            long delay = 2L * (integer > 5 ? 10 : integer);
            return Observable.timer(delay, TimeUnit.SECONDS);
        });
}
