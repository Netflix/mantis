package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PerpetualRuleActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    final JobScalingRule rule;

    public static Props Props(JobScalerContext context, JobScalingRule rule) {
        return Props.create(PerpetualRuleActor.class, context, rule);
    }

    public PerpetualRuleActor(JobScalerContext context, JobScalingRule rule) {
        this.jobScalerContext = context;
        this.rule = rule;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(CoordinatorActor.ActivateRuleRequest.class, this::activateScalerRule)
            .matchAny(any -> log.warn("Unknown message: {}", any))
            .build();
    }

    private void activateScalerRule(CoordinatorActor.ActivateRuleRequest activateRuleRequest) {
        // trigger message to coordinator to activate rule
        getContext().getParent().tell(activateRuleRequest, getSelf());
    }
}
