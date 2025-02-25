package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.lifecycle.ServiceLocator;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@Slf4j
public class CustomRuleActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";
    private static final Duration Max_Duration = Duration.of(5000, ChronoUnit.MILLIS);
    private static final Duration Interval_Duration = Duration.of(500, ChronoUnit.MILLIS);

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;

    @Mock
    private ServiceLocator mockServiceLocator;

    @Mock
    private Context mockContext;

    private final TestCustomRuleTrigger testCustomRuleTrigger = new TestCustomRuleTrigger();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .context(mockContext)
            .build();

        when(mockContext.getServiceLocator()).thenReturn(mockServiceLocator);
        when(mockServiceLocator.service("TestCustomRuleTrigger", JobScalingRuleCustomTrigger.class))
            .thenReturn(testCustomRuleTrigger);
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        testKit = null;
    }

    @Test
    public void testBasic() {
        // init the actor and verify the cron gets scheduled and triggered correctly
        JobScalingRule customRule = TestRuleUtils.createCustomTestRule(
            RULE_ID_1, "TestCustomRuleTrigger");

        ActorRef parentActor = system.actorOf(
            TestRuleUtils.TestParentActor.Props(jobScalerContext, customRule), "customRuleActorParent");
        final TestKit probe = new TestKit(system);

        parentActor.tell(new TestRuleUtils.TestParentActor.GetStateRequest(), probe.getRef());
        TestRuleUtils.TestParentActor.GetStateResponse response =
            probe.expectMsgClass(TestRuleUtils.TestParentActor.GetStateResponse.class);
        assertFalse(response.getRuleActivated().get());
        assertEquals(0, response.getRuleActivateCnt().get());
        assertEquals(0, response.getRuleDeactivateCnt().get());

        testCustomRuleTrigger.activateLatch.countDown();

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                parentActor.tell(new TestRuleUtils.TestParentActor.GetStateRequest(), probe.getRef());
                TestRuleUtils.TestParentActor.GetStateResponse response2 =
                    probe.expectMsgClass(TestRuleUtils.TestParentActor.GetStateResponse.class);
                assertTrue(response2.getRuleActivated().get());
                assertEquals(1, response2.getRuleActivateCnt().get());
                assertEquals(0, response2.getRuleDeactivateCnt().get());
                return null;
            });

        testCustomRuleTrigger.deactivateLatch.countDown();

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                parentActor.tell(new TestRuleUtils.TestParentActor.GetStateRequest(), probe.getRef());
                TestRuleUtils.TestParentActor.GetStateResponse response2 =
                    probe.expectMsgClass(TestRuleUtils.TestParentActor.GetStateResponse.class);
                assertFalse(response2.getRuleActivated().get());
                assertEquals(1, response2.getRuleActivateCnt().get());
                assertEquals(1, response2.getRuleDeactivateCnt().get());
                return null;
            });
    }

    public static class TestCustomRuleTrigger implements JobScalingRuleCustomTrigger {
        public CountDownLatch activateLatch = new CountDownLatch(1);
        public CountDownLatch deactivateLatch = new CountDownLatch(1);

        JobScalerContext context;
        JobScalingRule rule;
        CustomRuleTriggerHandler customRuleTriggerHandler;

        @Override
        public void init(JobScalerContext context, JobScalingRule rule, CustomRuleTriggerHandler customRuleTriggerHandler) {
            log.info("TestCustomRuleTrigger.init");
            this.context = context;
            this.rule = rule;
            this.customRuleTriggerHandler = customRuleTriggerHandler;
        }

        @Override
        public void run() {
            log.info("TestCustomRuleTrigger.run");
            try {
                boolean activateWait = activateLatch.await(3, TimeUnit.SECONDS);
                if (activateWait) {
                    customRuleTriggerHandler.activate(this.rule);
                    log.info("TestCustomRuleTrigger.activate ruleId={}", this.rule.getRuleId());
                }

                boolean deactivateWait = deactivateLatch.await(3, TimeUnit.SECONDS);
                if (deactivateWait) {
                    customRuleTriggerHandler.deactivate(this.rule.getRuleId());
                    log.info("TestCustomRuleTrigger.deactivate ruleId={}", this.rule.getRuleId());

                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void shutdown() {
            log.info("TestCustomRuleTrigger.shutdown");
        }
    }
}
