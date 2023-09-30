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

package io.mantisrx.server.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action0;


public abstract class BaseService implements Service {

    private static final AtomicInteger SERVICES_COUNTER = new AtomicInteger(0);
    private final boolean awaitsActiveMode;
    private final ActiveMode activeMode = new ActiveMode();
    private final int myServiceCount;
    private BaseService predecessor = null;
    protected BaseService() {
        this(false);
    }

    protected BaseService(boolean awaitsActiveMode) {
        this.awaitsActiveMode = awaitsActiveMode;
        if (!this.awaitsActiveMode) {
            activeMode.isInited.set(true);
        }
        myServiceCount = SERVICES_COUNTER.getAndIncrement();
    }

    @Override
    public abstract void start();

    /**
     * If this is set to await active mode, then, asynchronously (in a new thread) waits for entering active mode
     * and then calls the action parameter. It then also waits for any predecessor to be initialized before setting this
     * as initialized.
     * If this is set to not await active mode, then it just sets this as initialized and returns.
     *
     * @param onActive Action to call after waiting for entering active mode.
     */
    protected void awaitActiveModeAndStart(Action0 onActive) {
        activeMode.waitAndStart(onActive, predecessor);
    }

    protected boolean getIsInited() {
        return activeMode.getIsInited() &&
                (predecessor == null || predecessor.getIsInited());
    }

    @Override
    public void enterActiveMode() {
        activeMode.enterActiveMode();
    }

    @Override
    public void shutdown() {

    }

    public void addPredecessor(BaseService service) {
        this.predecessor = service;
    }

    public int getMyServiceCount() {
        return myServiceCount;
    }

    public class ActiveMode {

        private final AtomicBoolean isLeaderMode = new AtomicBoolean(false);
        private final AtomicBoolean isInited = new AtomicBoolean(false);
        Logger logger = LoggerFactory.getLogger(ActiveMode.class);

        public boolean getIsInited() {
            return isInited.get();
        }

        private void awaitLeaderMode() {
            while (!isLeaderMode.get()) {
                synchronized (isLeaderMode) {
                    try {
                        isLeaderMode.wait(10000);
                    } catch (InterruptedException e) {
                        logger.info("Interrupted waiting for leaderModeLatch");
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        public void waitAndStart(final Action0 onActive, final BaseService predecessor) {
            logger.info("{}: Setting up thread to wait for entering leader mode", myServiceCount);
            Runnable runnable = () -> {
                awaitLeaderMode();
                logger.info("{}: done waiting for leader mode", myServiceCount);
                if (predecessor != null) {
                    predecessor.activeMode.awaitInit();
                }
                logger.info("{}: done waiting for predecessor init", myServiceCount);
                if (onActive != null) {
                    onActive.call();
                }
                synchronized (isInited) {
                    isInited.set(true);
                    isInited.notify();
                }
            };
            Thread thr = new Thread(runnable, "BaseService-LeaderModeWaitThread-" + myServiceCount);
            thr.setDaemon(true);
            thr.start();
        }

        private void awaitInit() {
            while (!isInited.get()) {
                synchronized (isInited) {
                    try {
                        isInited.wait(5000);
                    } catch (InterruptedException e) {
                        logger.info("Interrupted waiting for predecessor init");
                        Thread.currentThread().interrupt();
                    }
                }
            }
            if (predecessor != null) {
                predecessor.activeMode.awaitInit();
            }
        }

        public void enterActiveMode() {
            logger.info("{}: Entering leader mode", myServiceCount);
            synchronized (isLeaderMode) {
                isLeaderMode.set(true);
                isLeaderMode.notify();
            }
        }
    }

    public static BaseService wrap(io.mantisrx.shaded.com.google.common.util.concurrent.Service service) {
        return new BaseService() {
            @Override
            public void start() {

            }

            @Override
            public void enterActiveMode() {
                service.startAsync().awaitRunning();
                super.enterActiveMode();
            }

            @Override
            public void shutdown() {
                service.stopAsync().awaitTerminated();
                super.shutdown();
            }
        };
    }
}
