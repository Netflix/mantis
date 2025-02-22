package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomRuleActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    final JobScalingRule rule;

    public static Props Props(JobScalerContext context, JobScalingRule rule) {
        return Props.create(CustomRuleActor.class, context, rule);
    }

    public CustomRuleActor(JobScalerContext context, JobScalingRule rule) {
        this.jobScalerContext = context;
        this.rule = rule;
    }

    @Override
    public void preStart() {
        log.info("CustomRuleActor started");
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
