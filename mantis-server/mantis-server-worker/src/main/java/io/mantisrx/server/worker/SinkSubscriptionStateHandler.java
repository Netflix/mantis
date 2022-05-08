/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.worker;

import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.Service;
import java.time.Clock;
import java.util.function.Function;

public interface SinkSubscriptionStateHandler extends Service {
    void onSinkSubscribed();

    void onSinkUnsubscribed();

    // Factory for making subscription state handlers. the first argument of the apply() function
    // corresponds to the ExecuteStageRequest which contains details about the job/stage that needs
    // to be executed in Mantis.
    interface Factory extends Function<ExecuteStageRequest, SinkSubscriptionStateHandler> {
        static Factory forEphemeralJobsThatNeedToBeKilledInAbsenceOfSubscriber(MantisMasterGateway gateway, Clock clock) {
            return executeStageRequest -> {
                if (executeStageRequest.getSubscriptionTimeoutSecs() > 0) {
                    return new SubscriptionStateHandlerImpl(
                            executeStageRequest.getJobId(),
                            gateway,
                            executeStageRequest.getSubscriptionTimeoutSecs(),
                            executeStageRequest.getMinRuntimeSecs(),
                            clock);
                } else {
                    return NOOP_INSTANCE;
                }
            };
        }
    }

    static SinkSubscriptionStateHandler noop() {
        return NOOP_INSTANCE;
    }

    static final SinkSubscriptionStateHandler NOOP_INSTANCE =
            new NoopSinkSubscriptionStateHandler();

    class NoopSinkSubscriptionStateHandler extends AbstractIdleService implements SinkSubscriptionStateHandler {
        @Override
        public void onSinkSubscribed() {
        }

        @Override
        public void onSinkUnsubscribed() {
        }

        @Override
        protected void startUp() throws Exception {
        }

        @Override
        protected void shutDown() throws Exception {
        }
    }
}
