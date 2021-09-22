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

package io.mantisrx.server.agent;

import io.mantisrx.server.core.Service;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisAgent implements Service {

    private static final Logger logger = LoggerFactory.getLogger(MantisAgent.class);
    private CountDownLatch blockUntilShutdown = new CountDownLatch(1);

    public MantisAgent() {
        Thread t = new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        };
        t.setDaemon(true);
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(t);
    }

    public static void main(String[] args) {
        MantisAgent agent = new MantisAgent();
        agent.start(); // block until shutdown hook (ctrl-c)
    }

    @Override
    public void start() {
        logger.info("Starting Mantis Agent");

        try {
            blockUntilShutdown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down Mantis Agent");
        blockUntilShutdown.countDown();
    }

    @Override
    public void enterActiveMode() {}
}
