package io.mantisrx.server.worker.jobmaster.rules;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.runtime.Context;
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

import static org.apache.pekko.dispatch.Futures.future;

@Slf4j
public class ScalerControllerActor extends AbstractActor {
    final JobScalerContext jobScalerContext;
    volatile JobScalingRule activeRule;
    volatile JobAutoScalerService activeJobAutoScalerService;

    private final Gauge activeRuleGauge;
    private final Counter activateCount;
    private final Counter deactivateCount;

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

        // setup metrics
        Metrics m =
            new Metrics.Builder()
                .name("JobScalerRule")
                .addCounter("activateCount")
                .addCounter("deactivateCount")
                .addGauge("activeRule")
                .build();
        m = Optional.ofNullable(this.jobScalerContext.getContext()).map(Context::getMetricsRegistry)
            .orElse(MetricsRegistry.getInstance())
            .registerAndGet(m);
        // default is -1 and -2 is no active rule.
        this.activeRuleGauge = m.getGauge("activeRule");
        this.activateCount = m.getCounter("activateCount");
        this.deactivateCount = m.getCounter("deactivateCount");
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
            try {
                startScalerService(activateScalerRequest);
            } catch (Exception e) {
                log.error("Failed to start scaler service: {}", activateScalerRequest.getRule(), e);
                throw new RuntimeException("failed to start job scaler service", e);
            }
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
            this.activeRuleGauge.set(-2L);
            this.deactivateCount.increment();
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

        if (this.activeRule.getScalerConfig().getStageConfigMap().entrySet().stream()
            .anyMatch(entry ->
                entry.getValue() != null && entry.getValue().getScalingPolicy() != null)) {
            log.info("Creating Job Auto Scaler service for rule: {}", activateScalerRequest.getRule().getRuleId());
            this.activeJobAutoScalerService = this.jobScalerContext.getJobAutoScalerServiceFactory()
                .apply(this.jobScalerContext, activateScalerRequest.getRule());
        } else {
            // the rule only requested desire size but no scaling policy, no need to create service.
            log.info("No Job Auto Scaler service required for rule: {}", activateScalerRequest.getRule().getRuleId());
            this.activeJobAutoScalerService = null;
        }

        final JobAutoScalerService newService = this.activeJobAutoScalerService;
        final JobScalingRule newRule = this.activeRule;

        log.info("closing current service {} for rule: {}, starting new service {} for rule {}",
            currentService,
            Optional.ofNullable(currentRule).map(JobScalingRule::getRuleId),
            newService,
            newRule.getRuleId());
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
                    throw new RuntimeException(ex);
                }
            }

            // first handle stage desire size
            for (Map.Entry<String, JobScalingRule.StageScalerConfig> kv :
                newRule.getScalerConfig().getStageConfigMap().entrySet()) {
                if (kv.getValue() == null || kv.getValue().getDesireSize() == null || kv.getValue().getDesireSize() < 0) {
                    log.info("No valid desire size for stage: {}, ignore", kv.getKey());
                    continue;
                }

                log.info("Start scaling stage {} to desire size {}", kv.getKey(), kv.getValue());
                this.jobScalerContext.getMasterClientApi().scaleJobStage(
                        this.jobScalerContext.getJobId(),
                        Integer.parseInt(kv.getKey()),
                        kv.getValue().getDesireSize(),
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

            if (newService == null) {
                log.info("[No Scaler Required] Job Auto Scaler service is null for rule: {}", newRule.getRuleId());
            } else {
                log.info("start activeJobAutoScalerService for {}", this.activeRule.getRuleId());
                newService.start();
            }
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

                    throw new RuntimeException("failed to start job scaler", result.failed().get());
                    // getContext().getParent().tell(CoordinatorActor.RefreshRuleRequest.of(this.jobScalerContext.getJobId()), self());
                } else {
                    log.warn("Ignore non-active rule service start failure: {}, current rule: {}",
                        newRule.getRuleId(), this.activeRule.getRuleId());
                }
            }
            return null;
        }, getContext().dispatcher());

        this.activateCount.increment();
        try {
            this.activeRuleGauge.set(Long.parseLong(activateScalerRequest.getRule().getRuleId()));
        } catch (NumberFormatException e) {
            log.error("Unexpected non-number rule id: {}", activateScalerRequest.getRule().getRuleId(), e);
        }
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
