/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.server.master.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.triggers.CronTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.fenzo.triggers.exceptions.TriggerNotFoundException;
import io.mantisrx.server.master.store.NamedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SLA {

    private static final Logger logger = LoggerFactory.getLogger(SLA.class);
    @JsonIgnore
    private static final int MaxValueForSlaMin = 5;
    @JsonIgnore
    private static final int MaxValueForSlaMax = 100;
    @JsonIgnore
    private static final TriggerOperator triggerOperator;

    static {
        triggerOperator = new TriggerOperator(1);
        try {
            triggerOperator.initialize();
        } catch (SchedulerException e) {
            logger.error("Unexpected: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private final int min;
    private final int max;
    private final String cronSpec;
    private final IJobClusterDefinition.CronPolicy cronPolicy;
    @JsonIgnore
    private final boolean hasCronSpec;
    @JsonIgnore
    private final IJobClusterDefinition.CronPolicy defaultPolicy = IJobClusterDefinition.CronPolicy.KEEP_EXISTING;
    @JsonIgnore
    private CronTrigger<NamedJob> scheduledTrigger;
    @JsonIgnore
    private String triggerGroup = null;
    @JsonIgnore
    private String triggerId = null;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public SLA(
            @JsonProperty("min") int min,
            @JsonProperty("max") int max,
            @JsonProperty("cronSpec") String cronSpec,
            @JsonProperty("cronPolicy") IJobClusterDefinition.CronPolicy cronPolicy
    ) {
        if (cronSpec != null && !cronSpec.isEmpty()) {
            this.cronSpec = cronSpec;
            hasCronSpec = true;
            this.max = 1;
            this.min = 0;
            this.cronPolicy = cronPolicy == null ? defaultPolicy : cronPolicy;
        } else {
            hasCronSpec = false;
            this.min = min;
            this.max = max;
            this.cronSpec = null;
            this.cronPolicy = null;
        }

        validate();

    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public String getCronSpec() {
        return cronSpec;
    }

    public JobClusterDefinitionImpl.CronPolicy getCronPolicy() {
        return cronPolicy;
    }

    private void validate() throws IllegalArgumentException {
        if (max < min)
            throw new IllegalArgumentException("Cannot have max=" + max + " < min=" + min);
        if (min > MaxValueForSlaMin)
            throw new IllegalArgumentException("Specified min sla value " + min + " cannot be >" + MaxValueForSlaMin);
        if (max > MaxValueForSlaMax)
            throw new IllegalArgumentException("Max sla value " + max + " cannot be >" + MaxValueForSlaMax);
    }

    // caller must lock to avoid concurrent access with destroyCron()
    private void initCron(NamedJob job) throws SchedulerException {
        if (!hasCronSpec || triggerId != null)
            return;
        logger.info("Init'ing cron for " + job.getName());
        triggerGroup = job.getName() + "-" + this;
        try {
            scheduledTrigger = new CronTrigger<>(cronSpec, job.getName(), job, NamedJob.class, NamedJob.CronTriggerAction.class);
            triggerId = triggerOperator.registerTrigger(triggerGroup, scheduledTrigger);
        } catch (IllegalArgumentException e) {
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    // caller must lock to avoid concurrent access with initCron()
    private void destroyCron() {
        try {
            if (triggerId != null) {
                logger.info("Destroying cron " + triggerId);
                triggerOperator.deleteTrigger(triggerGroup, triggerId);
                triggerId = null;
            }
        } catch (TriggerNotFoundException | SchedulerException e) {
            logger.warn("Couldn't delete trigger group " + triggerGroup + ", id " + triggerId);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cronPolicy == null) ? 0 : cronPolicy.hashCode());
        result = prime * result + ((cronSpec == null) ? 0 : cronSpec.hashCode());
        result = prime * result + ((defaultPolicy == null) ? 0 : defaultPolicy.hashCode());
        result = prime * result + (hasCronSpec ? 1231 : 1237);
        result = prime * result + max;
        result = prime * result + min;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SLA other = (SLA) obj;
        if (cronPolicy != other.cronPolicy)
            return false;
        if (cronSpec == null) {
            if (other.cronSpec != null)
                return false;
        } else if (!cronSpec.equals(other.cronSpec))
            return false;
        if (defaultPolicy != other.defaultPolicy)
            return false;
        if (hasCronSpec != other.hasCronSpec)
            return false;
        if (max != other.max)
            return false;
        return min == other.min;
    }

    @Override
    public String toString() {
        return "SLA [min=" + min + ", max=" + max + ", cronSpec=" + cronSpec + ", cronPolicy=" + cronPolicy
                + ", hasCronSpec=" + hasCronSpec + ", defaultPolicy=" + defaultPolicy + ", scheduledTrigger="
                + scheduledTrigger + ", triggerGroup=" + triggerGroup + ", triggerId=" + triggerId + "]";
    }
}

