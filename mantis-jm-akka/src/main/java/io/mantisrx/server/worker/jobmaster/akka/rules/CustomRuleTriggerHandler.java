package io.mantisrx.server.worker.jobmaster.akka.rules;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import lombok.Builder;
import lombok.Value;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Utility class to handle custom rule triggers. This ensure the callbacks are executed in the correct executor
 * (rule actor dispatcher).
 */
@Builder
@Value
public class CustomRuleTriggerHandler {
    Consumer<JobScalingRule> activateCallback;
    Consumer<String> deactivateCallback;
    Executor callBackExecutor;

    public void activate(JobScalingRule rule) {
        callBackExecutor.execute(() -> activateCallback.accept(rule));
    }

    public void deactivate(String ruleId) {
        callBackExecutor.execute(() -> deactivateCallback.accept(ruleId));
    }
}
