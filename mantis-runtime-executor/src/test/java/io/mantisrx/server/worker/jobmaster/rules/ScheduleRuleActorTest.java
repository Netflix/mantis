package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.jobmaster.JobAutoScalerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@Slf4j
public class ScheduleRuleActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";
    private static final Duration Max_Duration = Duration.of(5000, ChronoUnit.MILLIS);
    private static final Duration Interval_Duration = Duration.of(500, ChronoUnit.MILLIS);

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;

    @Mock
    private MantisMasterGateway masterClientApi;

    @Mock
    private JobAutoScalerService jobAutoScalerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .build();
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        testKit = null;
    }

    @Test
    public void testBasicScheduleFromCron() {
        // init the actor and verify the cron gets scheduled and triggered correctly
        JobScalingRule scheduleRule = TestRuleUtils.createScheduleRule(
            RULE_ID_1, "0/1 * * * * ?", null);

        ActorRef parentActor = system.actorOf(
            ScheduleTestParentActor.Props(jobScalerContext, scheduleRule), "scheduleRuleActorParent");
        final TestKit probe = new TestKit(system);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                parentActor.tell(new ScheduleTestParentActor.GetStateRequest(), probe.getRef());
                ScheduleTestParentActor.GetStateResponse response =
                    probe.expectMsgClass(ScheduleTestParentActor.GetStateResponse.class);
                assertTrue(response.ruleActivated.get());
                assertEquals(1, response.ruleActivateCnt.get());
                return null;
            });
    }

    @Test
    public void testScheduleFromCronWithDuration() {
        // init the actor and verify the cron gets scheduled and triggered correctly
        JobScalingRule scheduleRule = TestRuleUtils.createScheduleRule(
            RULE_ID_1, buildCron(1), "PT2S");

        ActorRef parentActor = system.actorOf(
            ScheduleTestParentActor.Props(jobScalerContext, scheduleRule), "scheduleRuleActorParent");
        final TestKit probe = new TestKit(system);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                parentActor.tell(new ScheduleTestParentActor.GetStateRequest(), probe.getRef());
                ScheduleTestParentActor.GetStateResponse response =
                    probe.expectMsgClass(ScheduleTestParentActor.GetStateResponse.class);
                assertTrue(response.ruleActivated.get());
                assertEquals(1, response.ruleActivateCnt.get());
                return null;
            });

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                parentActor.tell(new ScheduleTestParentActor.GetStateRequest(), probe.getRef());
                ScheduleTestParentActor.GetStateResponse response =
                    probe.expectMsgClass(ScheduleTestParentActor.GetStateResponse.class);
                assertFalse(response.ruleActivated.get());
                assertEquals(1, response.ruleDeactivateCnt.get());
                return null;
            });
    }

    @Test
    public void testScheduleWithDurationRecurring() {
        // init the actor and verify the cron gets scheduled and triggered correctly
        JobScalingRule scheduleRule = TestRuleUtils.createScheduleRule(
            RULE_ID_1, "0/3 * * * * ?", "PT1S");

        ActorRef parentActor = system.actorOf(
            ScheduleTestParentActor.Props(jobScalerContext, scheduleRule), "scheduleRuleActorParent");
        final TestKit probe = new TestKit(system);
        for (int i = 0; i < 2; i ++) {
            final int cnt = i;
            testKit.awaitAssert(Max_Duration, Interval_Duration,
                () -> {
                    parentActor.tell(new ScheduleTestParentActor.GetStateRequest(), probe.getRef());
                    ScheduleTestParentActor.GetStateResponse response =
                        probe.expectMsgClass(ScheduleTestParentActor.GetStateResponse.class);
                    assertTrue(response.ruleActivated.get());
                    assertEquals(cnt + 1, response.ruleActivateCnt.get());
                    return null;
                });

            testKit.awaitAssert(Max_Duration, Interval_Duration,
                () -> {
                    parentActor.tell(new ScheduleTestParentActor.GetStateRequest(), probe.getRef());
                    ScheduleTestParentActor.GetStateResponse response =
                        probe.expectMsgClass(ScheduleTestParentActor.GetStateResponse.class);
                    assertFalse(response.ruleActivated.get());
                    assertEquals(cnt + 1, response.ruleActivateCnt.get());
                    return null;
                });
        }
    }

    public static class ScheduleTestParentActor extends AbstractActor {
        public final JobScalerContext jobScalerContext;
        public final JobScalingRule rule;
        public ActorRef schedulerRuleActor;
        public AtomicBoolean ruleActivated = new AtomicBoolean(false);
        public AtomicInteger ruleActivateCnt = new AtomicInteger(0);
        public AtomicInteger ruleDeactivateCnt = new AtomicInteger(0);

        public static Props Props(JobScalerContext context, JobScalingRule rule) {
            return Props.create(ScheduleTestParentActor.class, context, rule);
        }

        public ScheduleTestParentActor(JobScalerContext context, JobScalingRule rule) {
            this.jobScalerContext = context;
            this.rule = rule;
        }

        @Override
        public AbstractActor.Receive createReceive() {
            return receiveBuilder()
                .match(GetStateRequest.class, req -> {
                    getSender().tell(
                        GetStateResponse.builder()
                            .schedulerRuleActor(schedulerRuleActor)
                            .ruleActivated(ruleActivated)
                            .ruleActivateCnt(ruleActivateCnt)
                            .ruleDeactivateCnt(ruleDeactivateCnt)
                            .build(),
                        getSelf());
                })
                .match(CoordinatorActor.ActivateRuleRequest.class,
                    req -> {
                    this.ruleActivated.set(true);
                    this.ruleActivateCnt.incrementAndGet();
                })
                .match(CoordinatorActor.DeactivateRuleRequest.class,
                    req -> {
                        this.ruleActivated.set(false);
                        this.ruleDeactivateCnt.incrementAndGet();
                    })
                .match(KillScheduleActorRequest.class, req -> {
                    getContext().stop(schedulerRuleActor);
                })
                .matchAny(this::unhandled)
                .build();
        }

        @Override
        public void preStart() {
            schedulerRuleActor = getContext().actorOf(
                ScheduleRuleActor.Props(jobScalerContext, rule), "testScheduleRuleActor");
        }

        @Value
        public static class GetStateRequest {
        }

        @Value
        public static class KillScheduleActorRequest {
        }

        @Builder
        @Value
        public static class GetStateResponse {
            ActorRef schedulerRuleActor;
            AtomicBoolean ruleActivated;
            AtomicInteger ruleActivateCnt;
            AtomicInteger ruleDeactivateCnt;
        }

    }

    private String buildCron(int secondsInFuture) {
        // Get the current time
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, secondsInFuture); // Add one second

        // Extract the time components
        int second = calendar.get(Calendar.SECOND);
        int minute = calendar.get(Calendar.MINUTE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
        int month = calendar.get(Calendar.MONTH) + 1; // Months are 0-based in Calendar

        // Create a cron expression for the next second
        return String.format("%d %d %d %d %d ? %d", second, minute, hour, dayOfMonth, month, calendar.get(Calendar.YEAR));
    }
}
