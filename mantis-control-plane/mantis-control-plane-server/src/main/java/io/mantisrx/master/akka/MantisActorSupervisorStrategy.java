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

package io.mantisrx.master.akka;

import akka.actor.ActorInitializationException;
import akka.actor.ActorKilledException;
import akka.actor.DeathPactException;
import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategyConfigurator;
import akka.japi.pf.DeciderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The standard Mantis Actor supervisor strategy.
 */
public class MantisActorSupervisorStrategy implements SupervisorStrategyConfigurator {
    private static final Logger LOGGER = LoggerFactory.getLogger(MantisActorSupervisorStrategy.class);

    private static final MantisActorSupervisorStrategy INSTANCE = new MantisActorSupervisorStrategy();

    public static MantisActorSupervisorStrategy getInstance() {
        return INSTANCE;
    }

    @Override
    public SupervisorStrategy create() {
        // custom supervisor strategy to resume the child actors on Exception instead of the default restart behavior
        return new OneForOneStrategy(DeciderBuilder
            .match(ActorInitializationException.class, e -> {
                ActorSystemMetrics.getInstance().incrementActorInitExceptionCount();
                return SupervisorStrategy.stop();
            })
            .match(ActorKilledException.class, e -> {
                ActorSystemMetrics.getInstance().incrementActorKilledCount();
                return SupervisorStrategy.stop();
            })
            .match(DeathPactException.class, e -> {
                ActorSystemMetrics.getInstance().incrementActorDeathPactExcCount();
                return SupervisorStrategy.stop();
            })
            .match(Exception.class, e -> {
                LOGGER.info("resuming actor on exception {}", e.getMessage(), e);
                ActorSystemMetrics.getInstance().incrementActorResumeCount();
                return SupervisorStrategy.resume();
            })
            .build());
    }
}
