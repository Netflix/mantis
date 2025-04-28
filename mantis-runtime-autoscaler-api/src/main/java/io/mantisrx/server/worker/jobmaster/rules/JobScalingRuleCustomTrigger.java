package io.mantisrx.server.worker.jobmaster.rules;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;

/**
 * Job User can implement this interface in the user job artifact to provide custom rule trigger logic.
 * The recommended injection is via the {@link io.mantisrx.runtime.lifecycle.ServiceLocator} in job context and this
 * requires setting the custom trigger name used in {@link JobScalingRule#getTriggerConfig()} to mach the service name.
 * There is also fallback support to instantiate the custom trigger class directly via java reflection using the
 * class name.
 * <p>
 * [Custom Trigger Implementation Notes]
 * - The custom rule id can be changed inside the trigger but the state needs to be maintained by the trigger to ensure
 * the activate/deactivate calls are consistent wit the rule id.
 * - The trigger is executed by an isolated executor service and its lifecycle needs to be managed by the trigger logic.
 * - If a long-running loop or subscription is needed, it needs to be implemented in the "run" method.
 */
public interface JobScalingRuleCustomTrigger {
    void init(JobScalerContext context,
              JobScalingRule rule,
              CustomRuleTriggerHandler customRuleTriggerHandler);

    void run();

    void shutdown();
}
