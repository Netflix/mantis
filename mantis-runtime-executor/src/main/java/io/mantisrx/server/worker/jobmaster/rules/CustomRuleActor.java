package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.base.Strings;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static akka.dispatch.Futures.future;

/**
 * Actor that implements a custom scaling rule.
 * Use cases:
 * - Trigger scaling rule change based on external signals (e.g. external rest endpoint for failover signal).
 * - Trigger scaling based on job metrics to have fine-grained control over scaling steps and tuning.
 */
@Slf4j
public class CustomRuleActor extends AbstractActor {
    // Custom rule trigger executor is used to separate custom logic from rule system dispatcher.
    private final ExecutorService customRuleExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("CustomRuleExecutorPool-%d")
            .build()
    );
    private final ExecutionContext customExecutionContext =
        ExecutionContext.fromExecutorService(customRuleExecutorService);

    final JobScalerContext jobScalerContext;
    final JobScalingRule rule;
    JobScalingRuleCustomTrigger customTrigger;

    public static Props Props(JobScalerContext context, JobScalingRule rule) {
        return Props.create(CustomRuleActor.class, context, rule);
    }

    public CustomRuleActor(JobScalerContext context, JobScalingRule rule) {
        this.jobScalerContext = context;
        this.rule = rule;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("CustomRuleActor started");
        // initialize custom trigger based on rule configuration
        String customTriggerClassName = rule.getTriggerConfig().getCustomTrigger();
        if (Strings.isNullOrEmpty(customTriggerClassName)) {
            log.error("Custom trigger class name is not set in rule configuration");
            return;
        }

        // first try service locator
        try {
            this.customTrigger = this.jobScalerContext.getContext().getServiceLocator()
                .service(customTriggerClassName, JobScalingRuleCustomTrigger.class);
            log.info("Loaded custom trigger class from service locator: {}", customTriggerClassName);
        } catch (Exception e) {
            log.warn("Failed to load custom trigger class from service locator: {}", customTriggerClassName, e);
        }

        // if not found, try loading directly
        try {
            if (this.customTrigger == null) {
                log.info("Try loading custom trigger class directly: {}", customTriggerClassName);
                this.customTrigger =
                    (JobScalingRuleCustomTrigger) Class.forName(customTriggerClassName).newInstance();
                log.info("Loaded custom trigger class directly: {}", customTriggerClassName);
            }
        } catch (Exception e) {
            log.error("Failed to load custom trigger class directly: {}, no custom trigger available",
                customTriggerClassName, e);
            return;
        }

        // initialize custom trigger
        if (this.customTrigger == null) {
            log.error("[{}] Custom trigger is not available: {}, Ignore custom rule: {}.",
                this.jobScalerContext.getJobId(), customTriggerClassName, this.rule);
            return;
        }

        // capture needed pointers for callbacks
        ActorRef coordinatorActor = getContext().getParent();
        ActorRef self = getSelf();
        ExecutionContextExecutor dispatcher = getContext().getDispatcher();
        CustomRuleTriggerHandler triggerHandler = CustomRuleTriggerHandler.builder()
            .callBackExecutor(dispatcher)
            .activateCallback(rule -> coordinatorActor.tell(
                CoordinatorActor.ActivateRuleRequest.of(this.jobScalerContext.getJobId(), rule), self))
            .deactivateCallback(ruleId -> coordinatorActor.tell(
                CoordinatorActor.DeactivateRuleRequest.of(this.jobScalerContext.getJobId(), ruleId), self))
            .build();

        Future<Void> startAndRunCustomRuleFuture = future(() -> {
            try {
                this.customTrigger.init(
                    this.jobScalerContext,
                    this.rule,
                    triggerHandler
                );

                log.info("Starting custom trigger: {}", customTriggerClassName);
                this.customTrigger.run();
            } catch (Exception e) {
                log.error("Custom trigger run failed with: ", e);
                throw new RuntimeException(e);
            }
            return null;
        }, customExecutionContext);

        startAndRunCustomRuleFuture.onComplete(result -> {
            if (result.isSuccess()) {
                log.info("Custom trigger started successfully");
            } else {
                log.error("failed to run custom rule: {}, restart rule actor",
                    this.rule, result.failed().get());
                throw new RuntimeException("CustomTrigger run failed, restart rule actor", result.failed().get());
            }
            return null;
        }, getContext().dispatcher());
    }

    @Override
    public void postStop() throws Exception {
        log.info("CustomRuleActor stopped for {}, {}", this.jobScalerContext.getJobId(), this.rule.getRuleId());
        if (this.customTrigger != null) {
            future(() -> {
                try {
                    this.customTrigger.shutdown();
                } catch (Exception e) {
                    log.error("Failed to shutdown custom trigger: ", e);
                }
                return null;
            }, customExecutionContext);
        }

        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchAny(any -> log.warn("Unknown message: {}", any))
            .build();
    }
}
