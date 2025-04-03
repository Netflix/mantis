package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRuleUtils {
    public static JobScalingRule createPerpetualRule(String ruleId, String jobId) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("standard")
            .stageConfigMap(ImmutableMap.of(
                "1",
                JobScalingRule.StageScalerConfig.builder()
                    .scalingPolicy(createDefaultStageScalingPolicy())
                    .build()))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_PERPETUAL)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createPerpetualRuleWithDesireSize(String ruleId, String jobId) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("standard")
            .stageConfigMap(ImmutableMap.of(
                "1",
                JobScalingRule.StageScalerConfig.builder()
                    .scalingPolicy(createDefaultStageScalingPolicy())
                    .desireSize(10)
                    .build()))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_PERPETUAL)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createPerpetualRuleWithDesireSizeOnly(String ruleId, String jobId) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("standard")
            .stageConfigMap(ImmutableMap.of(
                "1",
                JobScalingRule.StageScalerConfig.builder()
                    .desireSize(10)
                    .build()))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_PERPETUAL)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createScheduleRule(String ruleId, String cronSpec, String duration) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("schedule")
            .stageConfigMap(ImmutableMap.of(
                "1",
                JobScalingRule.StageScalerConfig.builder()
                    .scalingPolicy(createDefaultStageScalingPolicy())
                    .build()))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_SCHEDULE)
            .scheduleCron(cronSpec)
            .scheduleDuration(duration)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createCustomTestRule(String ruleId, String customeRuleClassName) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("custom")
            .stageConfigMap(ImmutableMap.of(
                "1",
                JobScalingRule.StageScalerConfig.builder()
                    .scalingPolicy(createDefaultStageScalingPolicy())
                    .build()))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_CUSTOM)
            .customTrigger(customeRuleClassName)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static StageScalingPolicy createDefaultStageScalingPolicy(int type) {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> strategies = new HashMap<>();

        StageScalingPolicy.ScalingReason reason = StageScalingPolicy.ScalingReason.values()[type];
        strategies.put(reason, new StageScalingPolicy.Strategy(
            reason,
            0.3,
            0.7,
            new StageScalingPolicy.RollingCount(3, 5)
        ));

        return new StageScalingPolicy(
            1, // stage
            1, // min
            10, // max
            1, // increment
            1, // decrement
            300, // coolDownSecs
            strategies,
            false // allowAutoScaleManager
        );
    }

    public static StageScalingPolicy createDefaultStageScalingPolicy() {
        return createDefaultStageScalingPolicy(0);
    }

    @Slf4j
    public static class TestParentActor extends AbstractActor {
        public final JobScalerContext jobScalerContext;
        public final JobScalingRule rule;
        public ActorRef childRuleActor;
        public AtomicBoolean ruleActivated = new AtomicBoolean(false);
        public AtomicInteger ruleActivateCnt = new AtomicInteger(0);
        public AtomicInteger ruleDeactivateCnt = new AtomicInteger(0);

        public static Props Props(JobScalerContext context, JobScalingRule rule) {
            return Props.create(TestParentActor.class, context, rule);
        }

        public TestParentActor(JobScalerContext context, JobScalingRule rule) {
            this.jobScalerContext = context;
            this.rule = rule;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(GetStateRequest.class, req -> {
                    getSender().tell(
                        GetStateResponse.builder()
                            .childRuleActorRef(childRuleActor)
                            .ruleActivated(ruleActivated)
                            .ruleActivateCnt(ruleActivateCnt)
                            .ruleDeactivateCnt(ruleDeactivateCnt)
                            .build(),
                        getSelf());
                })
                .match(CoordinatorActor.ActivateRuleRequest.class,
                    req -> {
                    log.info("TestParentActor: ActivateRuleRequest");
                    this.ruleActivated.set(true);
                    this.ruleActivateCnt.incrementAndGet();
                })
                .match(CoordinatorActor.DeactivateRuleRequest.class,
                    req -> {
                        log.info("TestParentActor: DeactivateRuleRequest");
                        this.ruleActivated.set(false);
                        this.ruleDeactivateCnt.incrementAndGet();
                    })
                .match(KillChildActorRequest.class, req -> {
                    log.info("TestParentActor: KillChildActorRequest");
                    getContext().stop(childRuleActor);
                })
                .matchAny(this::unhandled)
                .build();
        }

        @Override
        public void preStart() {
            switch (this.rule.getTriggerConfig().getTriggerType()) {
                case JobScalingRule.TRIGGER_TYPE_PERPETUAL:
                    this.childRuleActor = getContext().actorOf(
                        PerpetualRuleActor.Props(jobScalerContext, rule), "testRuleActor");
                    break;
                case JobScalingRule.TRIGGER_TYPE_SCHEDULE:
                    this.childRuleActor = getContext().actorOf(
                        ScheduleRuleActor.Props(jobScalerContext, rule), "testRuleActor");
                    break;
                case JobScalingRule.TRIGGER_TYPE_CUSTOM:
                    this.childRuleActor = getContext().actorOf(
                        CustomRuleActor.Props(jobScalerContext, rule), "testRuleActor");
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unknown rule type: " + this.rule.getTriggerConfig().getTriggerType());
            }
        }

        @Value
        public static class GetStateRequest {
        }

        @Value
        public static class KillChildActorRequest {
        }

        @Builder
        @Value
        public static class GetStateResponse {
            ActorRef childRuleActorRef;
            AtomicBoolean ruleActivated;
            AtomicInteger ruleActivateCnt;
            AtomicInteger ruleDeactivateCnt;
        }

    }
}
