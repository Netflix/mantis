package io.mantisrx.server.worker.jobmaster.akka.rules;


import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import rx.Subscription;
import rx.schedulers.Schedulers;
import scala.concurrent.ExecutionContextExecutor;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CoordinatorActor extends AbstractActor {
    private final JobScalerContext jobScalerContext;
    private final ExecutionContextExecutor ec = getContext().getSystem().dispatcher();
    private Subscription subscription;
    private JobScalerRuleInfo currentRuleInfo;
    private JobScalingRule defaultRule;
    private ActorRef controllerActor;
    protected final Map<String, ActorRef> ruleActors = new HashMap<>();

    /**
     * Latest activation requested by each trigger driven rule (custom / schedule), keyed by rule id.
     * <p>
     * Whether such a rule is currently in effect is only known to its rule actor: a custom trigger decides from
     * its own signals and a schedule rule from its cron window, so {@link #currentRuleInfo} cannot answer it.
     * The rule to activate is the one the trigger sent rather than the declared one, because triggers compute
     * the effective desire size and scaling steps at activation time. Recording the request here lets
     * {@link #onRuleRefresh} re-elect a rule that is still standing once a higher ranking rule goes away,
     * instead of dropping to the default rule.
     */
    protected final Map<String, JobScalingRule> standingRuleActivations = new HashMap<>();

    public static Props Props(JobScalerContext context) {
        return Props.create(CoordinatorActor.class, context);
    }

    public CoordinatorActor(JobScalerContext context) {
        this.jobScalerContext = context;

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            // onRuleChange: update local rule state, create new actor if needed
            .match(JobScalerRuleInfo.class, this::onRuleChange)
            // onRuleRefresh: trigger highest ranking rule in effect to controller
            .match(RefreshRuleRequest.class, this::onRuleRefresh)
            .match(ActivateRuleRequest.class, this::onActivateRuleRequest)
            .match(DeactivateRuleRequest.class, this::onDeactivateRuleRequest)
            .match(Terminated.class, this::onTerminated)
            // [for testing only] dump state
            .match(GetStateRequest.class, this::onGetStateRequest)
            .matchAny(any -> log.warn("Unknown message: {}", any))
            .build();
    }

    private void onGetStateRequest(GetStateRequest getStateRequest) {
        log.info("[Use In Testing Only] Received get state request: {}", getStateRequest);
        getSender().tell(
            GetStateResponse.builder()
                .currentRuleInfo(this.currentRuleInfo)
                .defaultRule(this.defaultRule)
                .controllerActor(this.controllerActor)
                .ruleActors(ImmutableMap.copyOf(this.ruleActors))
                .standingRuleActivations(ImmutableMap.copyOf(this.standingRuleActivations))
                .build(),
            self());
    }

    @Override
    public void preStart() throws Exception {
        // rely on default strategy to restart actor on error
        super.preStart();
        log.info("[preStart] {} Coordinator Actor started", getSelf());
        try {
            // startup sequence
            // 1. process rule actor using default config
            // 2. subscribe to scalerRule stream. listen for changes and tell self onRuleChange.
            // 3. create and watch scalerControllerActor
            // 4. tell onRuleRefresh to scalerControllerActor

            initState();
        } catch (Exception ex) {
            log.error("CoordinatorActor failed to start", ex);
            throw ex;
        }
    }

    @Override
    public void postStop() throws Exception {
        log.info("[postStop] {} Actor stopped", getSelf());
        if (this.subscription != null && !this.subscription.isUnsubscribed()) {
            this.subscription.unsubscribe();
        }

        super.postStop();
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        log.error("[preRestart] Actor restarting due to exception: ", reason);
        super.preRestart(reason, message);
    }

    private void onRuleChange(JobScalerRuleInfo scalerRuleInfo) {
        log.info("Received rule change: {}", scalerRuleInfo);
        if (scalerRuleInfo == null || !this.jobScalerContext.getJobId().equals(scalerRuleInfo.getJobId())) {
            log.warn("Received invalid rules: {}", scalerRuleInfo);
            return;
        }
        this.currentRuleInfo = scalerRuleInfo;

        // removed deleted rules
        Set<String> newRuleIds = Optional.ofNullable(this.currentRuleInfo.getRules()).orElse(ImmutableList.of())
            .stream()
            .map(JobScalingRule::getRuleId)
            .collect(Collectors.toSet());

        Set<String> removedRuleIds = this.ruleActors.keySet().stream()
            .filter(ruleId -> !newRuleIds.contains(ruleId) &&
                (defaultRule == null || !ruleId.equals(defaultRule.getRuleId())))
            .collect(Collectors.toSet());

        // remove rule actors no longer present. Ignore default rule.
        for (String ruleId : removedRuleIds) {
            ActorRef ruleActor = this.ruleActors.remove(ruleId);
            log.info("Stopping rule actor: {}", ruleActor);
            getContext().stop(ruleActor);

            // a deleted rule is no longer in effect, drop it before the refresh below can re-elect it
            this.standingRuleActivations.remove(ruleId);

            // notify controller to deactivate rule if active
            this.controllerActor.tell(DeactivateRuleRequest.of(this.jobScalerContext.getJobId(), ruleId), self());
        }

        // create new rule actors
        for (JobScalingRule rule : this.currentRuleInfo.getRules()) {
            if (!this.ruleActors.containsKey(rule.getRuleId())) {
                log.info("Creating rule actor: {}", rule);
                createRuleActor(rule);
            }
        }

        // trigger rule refresh
        self().tell(
            RefreshRuleRequest.of(this.jobScalerContext.getJobId()), self());
    }

    private void onRuleRefresh(RefreshRuleRequest refreshRuleRequest) {
        log.info("Refreshing current rule: {}", refreshRuleRequest);
        if (!refreshRuleRequest.getJobId().equals(this.jobScalerContext.getJobId())) {
            log.error("Invalid job id from request: {}, current job id {}",
                refreshRuleRequest, this.jobScalerContext.getJobId());
            return;
        }

        Comparator<JobScalingRule> byRuleId = Comparator.comparing(rule -> Long.valueOf(rule.getRuleId()));

        Optional<JobScalingRule> perpetualRule =
            Optional.ofNullable(this.currentRuleInfo)
                .map(JobScalerRuleInfo::getRules)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .filter(RuleUtils::isPerpetualRule)
                .max(byRuleId);

        // Trigger driven rules never satisfy isPerpetualRule, so they can only be re-elected from the
        // activations they sent. Without this a custom or schedule rule that is still in effect is dropped
        // to the default rule as soon as a higher ranking rule it lost to is removed.
        Optional<JobScalingRule> standingRule = this.standingRuleActivations.values().stream().max(byRuleId);

        Optional<JobScalingRule> activeRule =
            Stream.of(perpetualRule, standingRule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(byRuleId);

        // If no rule is in effect, fall back to defaultRule if not null
        JobScalingRule finalRule = activeRule.orElse(defaultRule);
        if (finalRule == null) {
            log.warn("No active rule found {}", getSelf());
            return;
        }

        ActivateRuleRequest activateRequest =
            ActivateRuleRequest.of(this.jobScalerContext.getJobId(), finalRule);

        if (this.standingRuleActivations.containsKey(finalRule.getRuleId())) {
            // Cannot route through the rule actor: it owns a trigger and does not accept activations, and the
            // rule to activate is the trigger computed one rather than the declared one. The controller ranking
            // check makes this a no-op when the rule is already the active one.
            log.info("Re-electing standing rule {}", finalRule.getRuleId());
            this.controllerActor.tell(activateRequest, self());
            return;
        }

        ActorRef ruleActor = this.ruleActors.get(finalRule.getRuleId());
        if (ruleActor == null) {
            log.error("No rule actor for rule {}, cannot activate: {}", finalRule.getRuleId(), finalRule);
            return;
        }
        ruleActor.tell(activateRequest, self());
    }

    private void onActivateRuleRequest(ActivateRuleRequest activateRuleRequest) {
        JobScalingRule rule = activateRuleRequest.getRule();
        if (rule != null && isTriggerDrivenRule(rule.getRuleId())) {
            this.standingRuleActivations.put(rule.getRuleId(), rule);
        }
        this.controllerActor.tell(activateRuleRequest, self());
    }

    private void onDeactivateRuleRequest(DeactivateRuleRequest deactivateRuleRequest) {
        this.standingRuleActivations.remove(deactivateRuleRequest.getRuleId());
        this.controllerActor.tell(deactivateRuleRequest, self());
    }

    private void onTerminated(Terminated terminated) {
        log.info("Actor {} terminated.", terminated.actor());
        // a rule whose actor is gone has no trigger behind it any more, never re-elect it
        this.ruleActors.entrySet().stream()
            .filter(kv -> kv.getValue().equals(terminated.actor()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet())
            .forEach(ruleId -> {
                log.warn("Rule actor for {} terminated unexpectedly, dropping it from rule state", ruleId);
                this.ruleActors.remove(ruleId);
                this.standingRuleActivations.remove(ruleId);
            });
    }

    /**
     * @return true when the given rule id belongs to a rule whose activation is decided by a trigger
     * (custom or schedule) rather than by rule ranking alone. Perpetual rules and the default rule return false:
     * they are always reconstructible from {@link #currentRuleInfo} / {@link #defaultRule}.
     */
    private boolean isTriggerDrivenRule(String ruleId) {
        return Optional.ofNullable(this.currentRuleInfo)
            .map(JobScalerRuleInfo::getRules)
            .orElse(ImmutableList.of())
            .stream()
            .anyMatch(rule -> ruleId.equals(rule.getRuleId()) && !RuleUtils.isPerpetualRule(rule));
    }

    private void createRuleActor(JobScalingRule rule) {
        Props newActorProps;
        String actorName;
        if (RuleUtils.isPerpetualRule(rule)) {
            newActorProps = PerpetualRuleActor.Props(this.jobScalerContext, rule);
            actorName = "PerpetualRuleActor-" + rule.getRuleId();
        } else if (JobScalingRule.TRIGGER_TYPE_CUSTOM.equals(rule.getTriggerConfig().getTriggerType())) {
            newActorProps = CustomRuleActor.Props(this.jobScalerContext, rule);
            actorName = "CustomRuleActor-" + rule.getRuleId();
        } else if (JobScalingRule.TRIGGER_TYPE_SCHEDULE.equals(rule.getTriggerConfig().getTriggerType())) {
            newActorProps = ScheduleRuleActor.Props(this.jobScalerContext, rule);
            actorName = "ScheduleRuleActor-" + rule.getRuleId();
        } else {
            log.error("Unknown rule trigger type: {}", rule);
            return;
        }

        ActorRef ruleActor = getContext().actorOf(newActorProps, actorName + "-" + System.currentTimeMillis());
        getContext().watch(ruleActor);
        this.ruleActors.put(rule.getRuleId(), ruleActor);
        log.info("{} rule actor created", rule.getRuleId());
    }

    private void initState() {
        // create controller actor
        log.info("[Coordinator initState]: {} on {}", this.jobScalerContext.getJobId(), getSelf());
        this.controllerActor = getContext().actorOf(ScalerControllerActor.Props(this.jobScalerContext));
        getContext().watch(this.controllerActor);

        // process default rule
        this.defaultRule = RuleUtils.getDefaultScalingRule(this.jobScalerContext.getSchedInfo());
        if (this.defaultRule != null) {
            createRuleActor(this.defaultRule);
        }
        log.info("Initialized with default rule: {}", this.defaultRule);

        setupRuleChangeStream();

        // trigger refresh rule. Note this refresh action might happen before rule stream fetch the first event.
        self().tell(RefreshRuleRequest.of(this.jobScalerContext.getJobId()), self());
    }

    private void setupRuleChangeStream() {
        log.info("Setting up rule change stream subscription");
        this.subscription = this.jobScalerContext.getMasterClientApi()
            .jobScalerRulesStream(this.jobScalerContext.getJobId())
            .subscribeOn(Schedulers.io()) // ensure the network calls are not handled by dispatcher
            .observeOn(Schedulers.from(ec))
            .doOnCompleted(() -> log.info("{} Rule stream completed", getSelf()))
            .doOnError(throwable -> log.error("Rule stream error", throwable))
            .subscribe(
                ruleInfo -> {
                    log.info("[Subscription action] new ruleInfo: {}", ruleInfo);
                    self().tell(ruleInfo, ActorRef.noSender());

                },
                throwable -> log.error("fail to process stream rule", throwable)
            );
    }

    @Value
    public static class RefreshRuleRequest {
        String jobId;

        public static RefreshRuleRequest of(String jobId) {
            return new RefreshRuleRequest(jobId);
        }
    }

    @Builder
    @Value
    public static class ActivateRuleRequest {
        String jobId;
        JobScalingRule rule;

        public static ActivateRuleRequest of(String jobId, JobScalingRule rule) {
            return new ActivateRuleRequest(jobId, rule);
        }
    }

    @Value
    public static class DeactivateRuleRequest {
        String jobId;
        String ruleId;

        public static DeactivateRuleRequest of(String jobId, String ruleId) {
            return new DeactivateRuleRequest(jobId, ruleId);
        }
    }

    /// this message type is for testing purpose only.
    @Value
    public static class GetStateRequest {
        String jobId;

        public static GetStateRequest of(String jobId) {
            return new GetStateRequest(jobId);
        }
    }

    @Builder
    @Value
    public static class GetStateResponse {
        JobScalerRuleInfo currentRuleInfo;
        JobScalingRule defaultRule;
        ActorRef controllerActor;
        Map<String, ActorRef> ruleActors;
        Map<String, JobScalingRule> standingRuleActivations;
    }
}
