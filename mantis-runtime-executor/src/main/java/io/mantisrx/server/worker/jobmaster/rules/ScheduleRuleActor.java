package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronExpression;
import scala.concurrent.duration.Duration;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

@Slf4j
public class ScheduleRuleActor extends AbstractActorWithTimers {
    final JobScalerContext jobScalerContext;
    final JobScalingRule rule;

    CronExpression currentCron;
    java.time.Duration currentDuration;

    // Timer keys
    private static final String TICK_TIMER_KEY = "TickTimer";
    private static final String STOP_TIMER_KEY = "StopTimer";

    public static Props Props(JobScalerContext context, JobScalingRule rule) {
        return Props.create(ScheduleRuleActor.class, context, rule);
    }

    public ScheduleRuleActor(JobScalerContext context, JobScalingRule rule) {
        this.jobScalerContext = context;
        this.rule = rule;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Tick.class, this::handleTick)
            .match(Stop.class, this::handleStop)
            .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("ScheduleRuleActor started");
        handleScheduleMessage();
    }

    @Override
    public void postStop() throws Exception {
        log.info("ScheduleRuleActor stopped, cancelling timers");
        getTimers().cancel(TICK_TIMER_KEY);
        getTimers().cancel(STOP_TIMER_KEY);
        super.postStop();
    }

    private void handleScheduleMessage() {
        log.info("setup schedule for user rule: {}", rule);

        // Cancel existing timers if any
        getTimers().cancel(TICK_TIMER_KEY);
        getTimers().cancel(STOP_TIMER_KEY);

        // Parse cron expression using Quartz
        String cronStr = this.rule.getTriggerConfig().getScheduleCron();
        log.info("Parsing cron expression: {}", cronStr);
        try {
            currentCron = new CronExpression(cronStr);
            currentCron.setTimeZone(TimeZone.getDefault()); // Ensure timezone consistency
        } catch (ParseException e) {
            log.error("Invalid cron expression '{}': {}",cronStr, e.getMessage());
            return;
        }

        // Calculate next valid execution time
        Date now = new Date();
        Date nextValidTime = currentCron.getNextValidTimeAfter(now);
        if (nextValidTime == null) {
            log.error("No next valid time found for cron expression '{}'", cronStr);
            throw new IllegalArgumentException("invalid cron expression: " + cronStr);
        }

        // Calculate delay in milliseconds
        long delayMillis = nextValidTime.getTime() - now.getTime();
        if (delayMillis < 0) {
            log.error("Computed delay is negative: {} ms from {}", delayMillis, nextValidTime);
            throw new IllegalArgumentException("invalid cron expression compared to current time: " + cronStr);
        }

        log.info("Scheduling Tick to trigger in {} ms at {}", delayMillis, nextValidTime);
        getTimers().startSingleTimer(TICK_TIMER_KEY, Tick.INSTANCE, Duration.create(delayMillis, "millis"));

        // If durationMillis is present, schedule the Stop message
        if (!Strings.isNullOrEmpty(this.rule.getTriggerConfig().getScheduleDuration())) {
            this.currentDuration = java.time.Duration.parse(this.rule.getTriggerConfig().getScheduleDuration());
            long durationMillis = this.currentDuration.toMillis();
            long stopDelayMillis = delayMillis + durationMillis;
            Date stopTime = new Date(nextValidTime.getTime() + durationMillis);
            log.info("Scheduling Stop to trigger in {} ms at {}", stopDelayMillis, stopTime);

            // Schedule the Stop message
            getTimers().startSingleTimer(STOP_TIMER_KEY, Stop.INSTANCE, Duration.create(stopDelayMillis, "millis"));
        }
    }

    /**
     * Handles the Tick message triggered by the timer.
     *
     * @param tick Tick message indicating the cron spec has been triggered
     */
    private void handleTick(Tick tick) {
        log.info("Tick triggered at {}", new Date());

        if (currentCron == null) {
            log.error("No active schedule found to handle Tick.");
            return;
        }

        // Send the messageRule to the parent actor
        log.info("Schedule actor to activate rule: {}", this.rule.getRuleId());
        ActorRef parent = getContext().getParent();
        parent.tell(CoordinatorActor.ActivateRuleRequest.of(this.jobScalerContext.getJobId(), this.rule), getSelf());

        Date now = new Date();
        Date nextValidTime = currentCron.getNextValidTimeAfter(now);
        if (nextValidTime == null) {
            log.warn("No subsequent valid time found for cron expression '{}'", currentCron.getCronExpression());
            return;
        }

        // Calculate delay for the next Tick
        long delayMillis = nextValidTime.getTime() - now.getTime();
        if (delayMillis < 0) {
            log.error("Computed delay for next Tick is negative: {} ms", delayMillis);
            return;
        }

        log.info("Rescheduling Tick to trigger in {} ms at {}", delayMillis, nextValidTime);
        // Reschedule the Tick message
        getTimers().startSingleTimer(TICK_TIMER_KEY, Tick.INSTANCE, Duration.create(delayMillis, "millis"));
    }

    /**
     * Handles the Stop message triggered by the timer.
     *
     * @param stop Stop message indicating the duration has elapsed
     */
    private void handleStop(Stop stop) {
        log.info("Deactivate rule as schedule duration finish: {}", this.rule.getRuleId());
        ActorRef parent = getContext().getParent();
        parent.tell(
            CoordinatorActor.DeactivateRuleRequest.of(this.jobScalerContext.getJobId(), this.rule.getRuleId()),
            getSelf());

        Date now = new Date();
        Date nextValidTime = currentCron.getNextValidTimeAfter(now);
        if (nextValidTime == null) {
            log.info("No more scheduling from cron spec after stop '{}'", currentCron.getCronExpression());
            return;
        }

        long stopDelayMillis = nextValidTime.getTime() + this.currentDuration.toMillis() - now.getTime();
        if (stopDelayMillis < 0) {
            log.warn("Computed delay for next Stop is negative: {} ms, ignore", stopDelayMillis);
            return;
        }

        Date stopTime = new Date(nextValidTime.getTime() + this.currentDuration.toMillis());
        log.info("Scheduling Stop to trigger in {} ms at {}", stopDelayMillis, stopTime);

        // Schedule the Stop message
        getTimers().startSingleTimer(STOP_TIMER_KEY, Stop.INSTANCE, Duration.create(stopDelayMillis, "millis"));
    }

    /**
     * Internal Tick message class.
     */
    private static class Tick {
        static final Tick INSTANCE = new Tick();

        private Tick() {
        }
    }

    /**
     * Internal Stop message class.
     */
    private static class Stop {
        static final Stop INSTANCE = new Stop();

        private Stop() {
        }
    }
}
