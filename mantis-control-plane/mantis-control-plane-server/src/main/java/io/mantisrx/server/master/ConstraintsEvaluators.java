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

package io.mantisrx.server.master;

import com.netflix.fenzo.AsSoftConstraint;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.BalancedHostAttrConstraint;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.plugins.UniqueHostAttrConstraint;
import io.mantisrx.runtime.JobConstraints;
import io.mantisrx.server.master.config.ConfigurationProvider;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConstraintsEvaluators {

    private static final String MANTISAGENT_MAIN_M4 = "mantisagent-main-m4";
    private static final String MANTISAGENT_MAIN_M3 = "mantisagent-main-m3";
    private static final String MANTISAGENT_MAIN_M5 = "mantisagent-main-m5";
    private static final int EXPECTED_NUM_ZONES = 3;
    private static final Logger logger = LoggerFactory.getLogger(ConstraintsEvaluators.class);
    public static ExclusiveHostConstraint exclusiveHostConstraint = new ExclusiveHostConstraint();

    public static ConstraintEvaluator hardConstraint(JobConstraints constraint, final Set<String> coTasks) {
        switch (constraint) {
        case ExclusiveHost:
            return exclusiveHostConstraint;
        case UniqueHost:
            return new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
                @Override
                public Set<String> call(String s) {
                    return coTasks;
                }
            });
        case ZoneBalance:
            return new BalancedHostAttrConstraint(new Func1<String, Set<String>>() {
                @Override
                public Set<String> call(String s) {
                    return coTasks;
                }
            }, zoneAttributeName(), EXPECTED_NUM_ZONES);
        case M4Cluster:
            return new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M4);
        case M3Cluster:
            return new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M3);
        case M5Cluster:
            return new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M5);
        default:
            logger.error("Unknown job hard constraint " + constraint);
            return null;
        }
    }

    public static String asgAttributeName() {
        return ConfigurationProvider.getConfig().getActiveSlaveAttributeName();
    }

    public static String zoneAttributeName() {
        return ConfigurationProvider.getConfig().getHostZoneAttributeName();
    }

    public static VMTaskFitnessCalculator softConstraint(JobConstraints constraint, final Set<String> coTasks) {
        switch (constraint) {
        case ExclusiveHost:
            return AsSoftConstraint.get(exclusiveHostConstraint);
        case UniqueHost:
            return AsSoftConstraint.get(new UniqueHostAttrConstraint(new Func1<String, Set<String>>() {
                @Override
                public Set<String> call(String s) {
                    return coTasks;
                }
            }));
        case ZoneBalance:
            return new BalancedHostAttrConstraint(new Func1<String, Set<String>>() {
                @Override
                public Set<String> call(String s) {
                    return coTasks;
                }
            }, zoneAttributeName(), EXPECTED_NUM_ZONES).asSoftConstraint();
        case M4Cluster:
            return AsSoftConstraint.get(new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M4));
        case M3Cluster:
            return AsSoftConstraint.get(new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M3));
        case M5Cluster:
            return AsSoftConstraint.get(new ClusterAffinityConstraint(asgAttributeName(), MANTISAGENT_MAIN_M5));
        default:
            logger.error("Unknown job soft constraint " + constraint);
            return null;
        }
    }
}
