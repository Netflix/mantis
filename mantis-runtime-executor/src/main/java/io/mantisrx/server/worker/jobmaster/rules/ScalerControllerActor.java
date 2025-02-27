package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobAutoScalerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static akka.dispatch.Futures.future;

@Slf4j
public class ScalerControllerActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    volatile JobScalingRule activeRule;
    volatile JobAutoScalerService activeJobAutoScalerService;

    private final ExecutorService executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("JobAutoScalerServiceExecutorPool-%d")
            .build()
    );
    private final ExecutionContext executionContext = ExecutionContext.fromExecutorService(executorService);

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

    private void activateScaler(CoordinatorActor.ActivateRuleRequest activateScalerRequest) {
        log.info("Activating scaler for rule: {}", activateScalerRequest.getRule());
        if (activateScalerRequest.getRule() == null) {
            log.error("ActivateRuleRequest rule is null: {}, ignore.", activateScalerRequest.getJobId());
            return;
        }

        int compareRes = this.jobScalerContext.getRuleIdComparator().compare(
            activateScalerRequest.getRule().getRuleId(),
            Optional.ofNullable(this.activeRule)
                .map(JobScalingRule::getRuleId).orElse(String.valueOf(Integer.MIN_VALUE)));
        if (compareRes > 0) {
            startScalerService(activateScalerRequest);
        } else {
            log.warn("ActivateRuleRequest has lower ranking, ignore: {}, current rule: {}",
                activateScalerRequest.getRule().getRuleId(), this.activeRule.getRuleId());
        }
    }

    private void deactivateScaler(CoordinatorActor.DeactivateRuleRequest deactivateRequest) {
        if (this.activeRule != null && deactivateRequest.getRuleId().equals(this.activeRule.getRuleId())) {
            log.info("[EVENT DEACTIVATE RULE] scaler rule: {}", deactivateRequest.getRuleId());
            stopScalerService();
            getContext().getParent().tell(
                CoordinatorActor.RefreshRuleRequest.of(this.jobScalerContext.getJobId()), self());
        } else {
            log.warn("DeactivateRuleRequest rule is not active: {}, current: {}",
                deactivateRequest.getRuleId(), this.activeRule);
        }
    }

    private void stopScalerService() {
        try {
            log.info("Stopping Job Auto Scaler service for rule: {}", this.activeRule);
            if (this.activeJobAutoScalerService != null) {
                JobAutoScalerService currentService = this.activeJobAutoScalerService;
                JobScalingRule currentRule = this.activeRule;
                Future<Void> stopServiceFuture = future(() -> {
                    currentService.shutdown();
                    return null;
                }, executionContext);

                stopServiceFuture.onComplete(result -> {
                    if (result.isSuccess()) {
                        log.info("[Job Auto Scaler Shutdown] for rule {} successfully", currentRule.getRuleId());
                    } else {
                        log.error("failed to shutdown job auto scaler service in rule: {}, reset and request refresh",
                            currentRule, result.failed().get());
                    }
                    return null;
                }, getContext().dispatcher());
            }
        } catch (Exception ex) {
            log.error("failed to stop job auto scaler service", ex);
        } finally {
            this.activeRule = null;
            this.activeJobAutoScalerService = null;
        }
    }

    private void startScalerService(CoordinatorActor.ActivateRuleRequest activateScalerRequest) {
        log.info("[EVENT ACTIVATE RULE] req is higher ranking from current: {}, activating rule: {}",
            Optional.ofNullable(this.activeRule).map(JobScalingRule::getRuleId).orElse("null"),
            activateScalerRequest.getRule().getRuleId());
        final JobAutoScalerService currentService = this.activeJobAutoScalerService;
        final JobScalingRule currentRule = this.activeRule;

        this.activeRule = activateScalerRequest.getRule();
        this.activeJobAutoScalerService = this.jobScalerContext.getJobAutoScalerServiceFactory()
            .apply(this.jobScalerContext, activateScalerRequest.getRule());

        final JobAutoScalerService newService = this.activeJobAutoScalerService;
        final JobScalingRule newRule = this.activeRule;

        // Run start service in scaler executor,
        // DO NOT mutate actor state in scaler executor!
        Future<String> startServiceFuture = future(() -> {
            // shutdown current service if exists, ignore shutdown error.
            if (currentService != null) {
                log.info("Stopping current Job Auto Scaler service for rule: {}", currentRule.getRuleId());
                try {
                    currentService.shutdown();
                } catch (Exception ex) {
                    log.error("failed to stop current job auto scaler service", ex);
                }
            }

            // first handle stage desire size
            for (Map.Entry<Integer, Integer> kv : newRule.getScalerConfig().getStageDesireSize().entrySet()) {
                log.info("Start scaling stage {} to desire size {}", kv.getKey(), kv.getValue());
                this.jobScalerContext.getMasterClientApi().scaleJobStage(
                        this.jobScalerContext.getJobId(),
                        kv.getKey(),
                        kv.getValue(),
                        "Desire size from scaling ruleID: " + newRule.getRuleId())
                    .retryWhen(RuleUtils.LimitTenRetryLogic)
                    .doOnCompleted(() -> log.info("Scaled stage {} to desire size {}", kv.getKey(), kv.getValue()))
                    .onErrorResumeNext(throwable -> {
                        log.error("{} Failed to scale stage {} to desire size {}",
                            this.jobScalerContext.getJobId(), kv.getKey(), kv.getKey());
                        return Observable.empty();
                    })
                    .toBlocking()
                    .first();
                log.info("Finish scaling stage {} to desire size {}", kv.getKey(), kv.getValue());
            }

            log.info("start activeJobAutoScalerService for {}", this.activeRule.getRuleId());
            newService.start();
            return newRule.getRuleId();
        }, executionContext);

        // handle scaler service error back in dispatcher thread.
        startServiceFuture.onComplete(result -> {
            if (result.isSuccess()) {
                log.info("Job Auto Scaler started successfully for ruleID: {}", result.get());
            } else {
                log.error("failed to setup job auto scaler service in rule: {}",
                    newRule, result.failed().get());
                // only reset if the failed rule service is the current active one back in dispatcher thread.
                if (this.activeRule.getRuleId().equals(newRule.getRuleId())) {
                    log.error("reset controller actor due to failed rule: {}", this.activeRule.getRuleId());
                    this.activeRule = null;
                    this.activeJobAutoScalerService = null;
                    getContext().getParent().tell(CoordinatorActor.RefreshRuleRequest.of(this.jobScalerContext.getJobId()), self());
                } else {
                    log.warn("Ignore non-active rule service start failure: {}, current rule: {}",
                        newRule.getRuleId(), this.activeRule.getRuleId());
                }
            }
            return null;
        }, getContext().dispatcher());

        log.info("Activated scaler rule: {}", activateScalerRequest.getRule().getRuleId());
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
