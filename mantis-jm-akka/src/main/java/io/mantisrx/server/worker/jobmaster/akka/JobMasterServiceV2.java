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

package io.mantisrx.server.worker.jobmaster.akka;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.server.worker.jobmaster.JobManagerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.server.worker.jobmaster.akka.rules.CoordinatorActor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobMasterServiceV2 implements JobManagerService {
    final JobScalerContext jobScalerContext;
    ActorSystem system;

    public JobMasterServiceV2(JobScalerContext jobScalerContext) {
        this.jobScalerContext = jobScalerContext;
    }

    @Override
    public void start() {
        log.info("Starting JobMasterServiceV2");
        String configString = "akka {\n  actor {\n    serialize-messages = off\n  }\n}";
        Config config = ConfigFactory.parseString(configString).withFallback(ConfigFactory.load());
        system = ActorSystem.create("MantisJobMasterV2", config);
        // log the configuration of the actor system
        if (log.isDebugEnabled()) {
            system.logConfiguration();
        }
        system.logConfiguration();

        // no need to block and wait (done in root OperationExecution)
        system.actorOf(CoordinatorActor.Props(this.jobScalerContext), "JobScalerCoordinatorActor");
    }

    @Override
    public void shutdown() {
        system.terminate(); //todo: shall we wait for the termination future?
    }

    @Override
    public void enterActiveMode() {
    }
}
