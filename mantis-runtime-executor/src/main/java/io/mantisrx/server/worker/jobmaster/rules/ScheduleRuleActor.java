package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;

public class ScheduleRuleActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    final JobScalingRule rule;

    public static Props Props(JobScalerContext context, JobScalingRule rule) {
        return Props.create(ScheduleRuleActor.class, context, rule);
    }

    public ScheduleRuleActor(JobScalerContext context, JobScalingRule rule) {
        this.jobScalerContext = context;
        this.rule = rule;
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
