package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@Slf4j
public class ScalerControllerActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    JobScalingRule activeRule;

    public static Props Props(JobScalerContext context) {
        return Props.create(ScalerControllerActor.class, context);
    }

    public ScalerControllerActor(JobScalerContext context) {
        this.jobScalerContext = context;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(CoordinatorActor.ActivateRuleRequest.class, this::activateScaler)
            .match(CoordinatorActor.DeactivateRuleRequest.class, this::deactivateScaler)
            .match(GetActiveRuleRequest.class,
                req -> getSender().tell(GetActiveRuleResponse.of(activeRule), getSelf()))
            .matchAny(any -> log.warn("Unknown message: {}", any))
            .build();
    }

    @Override
    public void postStop() {
        log.info("ScalerControllerActor stopped");
    }

    @Override
    public void preStart() {
        log.info("[preStart] {} Controller actor started", getSelf());
    }

    private void activateScaler(CoordinatorActor.ActivateRuleRequest activateScalerRequest) {
        // create rx scaler
        // handle desire size
        // check rule id ranking
        log.info("Activating scaler for rule: {}", activateScalerRequest.getRule());
        this.activeRule = activateScalerRequest.getRule();
        log.info("Activated scaler rule: {}", activateScalerRequest.getRule().getRuleId());
    }

    private void deactivateScaler(CoordinatorActor.DeactivateRuleRequest deactivateRequest) {
        // deactivate rx scaler
        // todo send coordinator to refresh rules
        if (this.activeRule != null && deactivateRequest.getRuleId().equals(this.activeRule.getRuleId())) {
            log.info("Deactivating scaler rule: {}", deactivateRequest.getRuleId());
            this.activeRule = null;
        }

    }

    @Value
    public static class GetActiveRuleRequest {
    }

    @Value
    public static class GetActiveRuleResponse {
        @Nullable
        JobScalingRule rule;

        public static GetActiveRuleResponse of(JobScalingRule rule) {
            return new GetActiveRuleResponse(rule);
        }
    }
}
