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

package io.mantisrx.runtime;

import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.EnumParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;

/**
 * Utility class providing parameter definitions for eager subscription strategies
 * in perpetual jobs. These parameters control when jobs start processing data.
 */
public class EagerSubscriptionParameters {

    /**
     * Parameter name for eager subscription strategy
     */
    public static final String EAGER_SUBSCRIPTION_STRATEGY_PARAM = "eagerSubscriptionStrategy";

    /**
     * Parameter name for subscription timeout in seconds (used with TIMEOUT_BASED strategy)
     */
    public static final String SUBSCRIPTION_TIMEOUT_SECS_PARAM = "subscriptionTimeoutSecs";

    /**
     * Parameter definition for eager subscription strategy.
     * Controls when perpetual jobs start data processing.
     * 
     * @return ParameterDefinition for eager subscription strategy
     */
    public static ParameterDefinition<Enum<EagerSubscriptionStrategy>> eagerSubscriptionStrategy() {
        return new EnumParameter<EagerSubscriptionStrategy>(EagerSubscriptionStrategy.class)
                .name(EAGER_SUBSCRIPTION_STRATEGY_PARAM)
                .defaultValue(EagerSubscriptionStrategy.IMMEDIATE)
                .description("Strategy for when perpetual jobs start eager subscription:\n" +
                           "IMMEDIATE - Start processing data immediately (default, backward compatible)\n" +
                           "ON_FIRST_CLIENT - Wait for first SSE client connection before processing\n" +
                           "TIMEOUT_BASED - Wait for first client OR timeout (uses subscriptionTimeoutSecs)")
                .build();
    }

    /**
     * Parameter definition for subscription timeout in seconds.
     * Used with TIMEOUT_BASED eager subscription strategy.
     * 
     * @return ParameterDefinition for subscription timeout
     */
    public static ParameterDefinition<Integer> subscriptionTimeoutSecs() {
        return new IntParameter()
                .name(SUBSCRIPTION_TIMEOUT_SECS_PARAM)
                .defaultValue(60)
                .description("Timeout in seconds for TIMEOUT_BASED eager subscription strategy. " +
                           "If no client connects within this time, job starts processing anyway. " +
                           "Must be greater than 0.")
                .validator(Validators.range(1, 3600)) // 1 second to 1 hour
                .build();
    }
}