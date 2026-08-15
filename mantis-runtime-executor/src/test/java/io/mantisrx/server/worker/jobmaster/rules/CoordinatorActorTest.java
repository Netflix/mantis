package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.jobmaster.JobAutoScalerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.server.worker.jobmaster.akka.rules.CoordinatorActor;
import io.mantisrx.server.worker.jobmaster.akka.rules.ScalerControllerActor;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.subjects.BehaviorSubject;


import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Slf4j
public class CoordinatorActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";
    private static final String RULE_ID_5 = "5";
    private static final Duration Max_Duration = Duration.of(5000, ChronoUnit.MILLIS);
    private static final Duration Interval_Duration = Duration.of(500, ChronoUnit.MILLIS);

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;
    private StageScalingPolicy defaultStageScalingPolicy;

    @Mock
    private MantisMasterGateway masterClientApi;

    @Mock
    private JobAutoScalerService jobAutoScalerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        defaultStageScalingPolicy = TestRuleUtils.createDefaultStageScalingPolicy(1);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .jobAutoScalerServiceFactory((context, rule) -> jobAutoScalerService)
            .schedInfo(new SchedulingInfo.Builder()
                .addStage(StageSchedulingInfo.builder()
                    .scalingPolicy(defaultStageScalingPolicy).build())
                .numberOfStages(1)
                .build())
            .build();

        when(masterClientApi.scaleJobStage(anyString(), anyInt(), anyInt(), anyString()))
            .thenReturn(Observable.just(true));

        doAnswer((Answer<Void>) invocation -> {
            log.info("Test: start job auto scaler service");
            return null;
        }).when(jobAutoScalerService).start();

        doAnswer((Answer<Void>) invocation -> {
            log.info("Test: shutdown job auto scaler service");
            return null;
        }).when(jobAutoScalerService).shutdown();
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        testKit = null;
    }

    @Test
    public void testOnRuleRefreshRuleNoDefault() throws InterruptedException {
        // override default
        this.jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .jobAutoScalerServiceFactory((context, rule) -> jobAutoScalerService)
            .build();

        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JsonSerializer serializer = new JsonSerializer();
        try {
            String jsonStr = serializer.toJson(perpetualRule);
            log.info("Test: perpetual rule json: \n{}", jsonStr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        JobScalerRuleInfo ruleInfo = new JobScalerRuleInfo(
            JOB_ID, false, Collections.singletonList(perpetualRule));

        JobScalerRuleInfo ruleInfo2 = new JobScalerRuleInfo(JOB_ID, false,
            Collections.emptyList());

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(ruleInfo2);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        log.info("Test: create coordinator actor: {}", coordinatorActor);
        final TestKit probe = new TestKit(system);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertEquals(0, state.getCurrentRuleInfo().getRules().size());
                checkActiveControllerRule(state, probe, null);
                return null;
        });

        // add a rule
        ruleInfoSubject.onNext(ruleInfo);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state2 = getState(coordinatorActor, probe);
                assertNotNull(state2);
                assertEquals(ruleInfo.getRules(), state2.getCurrentRuleInfo().getRules());

                // check active controller rule state again
                checkActiveControllerRule(state2, probe, perpetualRule);
                return null;
            });

        // reset again
        ruleInfoSubject.onNext(ruleInfo2);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state3 = getState(coordinatorActor, probe);
                assertNotNull(state3);
                assertEquals(0, state3.getCurrentRuleInfo().getRules().size());

                // check active controller rule state again
                checkActiveControllerRule(state3, probe, null);
                return null;
            });
    }

    @Test
    public void testOnRuleRefreshWithPerpetualRuleWithDefault() throws InterruptedException {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JobScalingRule perpetualRule2 = TestRuleUtils.createPerpetualRule(RULE_ID_2, JOB_ID);
        JobScalerRuleInfo ruleInfo = new JobScalerRuleInfo(
            JOB_ID, false, Collections.singletonList(perpetualRule));

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(ruleInfo);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);
        JobScalerRuleInfo ruleInfo2 = new JobScalerRuleInfo(JOB_ID, false,
            Collections.emptyList());
        JobScalerRuleInfo ruleInfo3 = new JobScalerRuleInfo(JOB_ID, false,
            ImmutableList.of(perpetualRule,perpetualRule2));

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        log.info("Test: create coordinator actor: {}", coordinatorActor);
        final TestKit probe = new TestKit(system);

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);

                assertNotNull(state);
                assertEquals(ruleInfo.getRules(), state.getCurrentRuleInfo().getRules());

                // check active controller rule state
                checkActiveControllerRule(state, probe, ruleInfo.getRules().get(0));
                return null;
            });


        // push direct actor update
        coordinatorActor.tell(ruleInfo2, probe.getRef());
        CoordinatorActor.GetStateResponse state2 = getState(coordinatorActor, probe);
        assertNotNull(state2);
        assertEquals(ruleInfo2.getRules(), state2.getCurrentRuleInfo().getRules());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                // check active controller rule state again
                log.info("Test: check active controller rule state: expect default rule.");
                checkActiveControllerRule(state2, probe, state2.getDefaultRule());
                assertEquals(
                    1, state2.getDefaultRule().getScalerConfig().getStageConfigMap().size());
                assertEquals(
                    this.defaultStageScalingPolicy,
                    state2.getDefaultRule().getScalerConfig().getScalerConfigByStageNum(1).get().getScalingPolicy());
                assertNull(state2.getDefaultRule().getTriggerConfig());

                assertNull(state2.getDefaultRule().getScalerConfig().getScalerConfigByStageNum(1)
                    .get().getDesireSize());
                return null;
            });

        // push update to rule stream
        ruleInfoSubject.onNext(ruleInfo3);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state3 = getState(coordinatorActor, probe);
                assertNotNull(state3);
                assertEquals(ruleInfo3.getRules(), state3.getCurrentRuleInfo().getRules());

                checkActiveControllerRule(state3, probe, ruleInfo3.getRules().get(1));
                return null;
            });

    }

    @Test
    public void testStandingTriggerDrivenRuleIsReElectedWhenHigherRuleRemoved() {
        // two custom rules: the trigger classes cannot be resolved here so the rule actors stay inert and
        // the trigger callbacks are played by hand below, which is what a live custom trigger would send.
        JobScalingRule customRule1 = TestRuleUtils.createCustomTestRule(RULE_ID_1, "NoSuchTrigger");
        JobScalingRule customRule5 = TestRuleUtils.createCustomTestRule(RULE_ID_5, "NoSuchTrigger");

        JobScalerRuleInfo bothRules = new JobScalerRuleInfo(
            JOB_ID, false, ImmutableList.of(customRule1, customRule5));
        JobScalerRuleInfo rule1Only = new JobScalerRuleInfo(
            JOB_ID, false, ImmutableList.of(customRule1));

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(bothRules);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        final TestKit probe = new TestKit(system);

        // no trigger has fired yet so neither custom rule is in effect: default rule is active
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertEquals(bothRules.getRules(), state.getCurrentRuleInfo().getRules());
                checkActiveControllerRule(state, probe, state.getDefaultRule());
                assertTrue(state.getStandingRuleActivations().isEmpty());
                return null;
            });

        // both triggers report themselves in effect, rule 5 wins on ranking
        JobScalingRule activatedRule1 = triggerComputedRule(RULE_ID_1, 7);
        JobScalingRule activatedRule5 = triggerComputedRule(RULE_ID_5, 20);
        coordinatorActor.tell(
            CoordinatorActor.ActivateRuleRequest.of(JOB_ID, activatedRule1), probe.getRef());
        coordinatorActor.tell(
            CoordinatorActor.ActivateRuleRequest.of(JOB_ID, activatedRule5), probe.getRef());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertEquals(activatedRule1, state.getStandingRuleActivations().get(RULE_ID_1));
                assertEquals(activatedRule5, state.getStandingRuleActivations().get(RULE_ID_5));
                checkActiveControllerRule(state, probe, activatedRule5);
                return null;
            });

        // rule 5 is deleted: rule 1 is still in effect and must take over instead of the default rule
        ruleInfoSubject.onNext(rule1Only);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertEquals(rule1Only.getRules(), state.getCurrentRuleInfo().getRules());

                // the deleted rule must not linger as a re-election candidate
                assertFalse(state.getStandingRuleActivations().containsKey(RULE_ID_5));

                // the trigger computed rule is re-elected, not the declared one and not the default rule
                assertEquals(activatedRule1, state.getStandingRuleActivations().get(RULE_ID_1));
                checkActiveControllerRule(state, probe, activatedRule1);
                return null;
            });

        // once the trigger says it is done the rule is dropped and the default rule takes over
        coordinatorActor.tell(
            CoordinatorActor.DeactivateRuleRequest.of(JOB_ID, RULE_ID_1), probe.getRef());
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertTrue(state.getStandingRuleActivations().isEmpty());
                checkActiveControllerRule(state, probe, state.getDefaultRule());
                return null;
            });
    }

    /**
     * Mimics what a custom trigger sends on activation: the rule it computed at that moment, carrying no
     * trigger config because the trigger only fills in ruleId and scaler config.
     */
    private JobScalingRule triggerComputedRule(String ruleId, int desireSize) {
        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(JobScalingRule.ScalerConfig.builder()
                .type("custom")
                .stageConfigMap(ImmutableMap.of(
                    "1",
                    JobScalingRule.StageScalerConfig.builder()
                        .scalingPolicy(TestRuleUtils.createDefaultStageScalingPolicy())
                        .desireSize(desireSize)
                        .build()))
                .build())
            .metadata(Collections.emptyMap())
            .build();
    }

    private CoordinatorActor.GetStateResponse getState(ActorRef coordinatorActor, TestKit probe) {
        coordinatorActor.tell(CoordinatorActor.GetStateRequest.of(JOB_ID), probe.getRef());
        return probe.expectMsgClass(CoordinatorActor.GetStateResponse.class);
    }

    private void checkActiveControllerRule(
        CoordinatorActor.GetStateResponse state,
        TestKit probe,
        JobScalingRule expectedRule) {
        state.getControllerActor().tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse activeRuleResponse =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertNotNull(activeRuleResponse);
        assertEquals(expectedRule, activeRuleResponse.getRule());
    }
}
