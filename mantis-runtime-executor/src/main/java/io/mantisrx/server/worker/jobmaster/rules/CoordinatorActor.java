package io.mantisrx.server.worker.jobmaster.rules;


import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
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
            // onRuleRefresh: trigger latest perpetual rule to controller
            .match(RefreshRuleRequest.class, this::onRuleRefresh)
            .match(ActivateRuleRequest.class, ar -> this.controllerActor.tell(ar, self()))
            .match(DeactivateRuleRequest.class, dr -> this.controllerActor.tell(dr, self()))
            .match(Terminated.class, terminated -> { log.info("Actor {} terminated.", terminated.actor());})
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

        Optional<JobScalingRule> activeRule =
            Optional.ofNullable(this.currentRuleInfo)
                .map(JobScalerRuleInfo::getRules)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .filter(RuleUtils::isPerpetualRule)
                .max(Comparator.comparing(rule -> Long.valueOf(rule.getRuleId())));

        // If no perpetual rule is found, fall back to defaultRule if not null
        JobScalingRule finalRule = activeRule.orElse(defaultRule);
        if (finalRule != null) {
            this.ruleActors.get(finalRule.getRuleId())
                .tell(ActivateRuleRequest.of(jobScalerContext.getJobId(), finalRule), self());
        } else {
            log.warn("No active rule found {}", getSelf());
        }
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
    }
}
