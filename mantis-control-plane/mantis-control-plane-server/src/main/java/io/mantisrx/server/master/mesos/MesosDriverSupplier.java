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

package io.mantisrx.server.master.mesos;

import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.functions.Action1;


public class MesosDriverSupplier implements Supplier<MesosSchedulerDriver> {

    private static final Logger logger = LoggerFactory.getLogger(MesosDriverSupplier.class);

    private final MesosSettings mesosSettings;
    private final Observer<String> vmLeaseRescindedObserver;
    private final JobMessageRouter jobMessageRouter;
    private final WorkerRegistry workerRegistry;
    private final AtomicReference<MesosSchedulerDriver> mesosDriverRef = new AtomicReference<>(null);
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private volatile Action1<List<VirtualMachineLease>> addVMLeaseAction = null;
    private final AtomicInteger numAttemptsToInit = new AtomicInteger(0);

    public MesosDriverSupplier(final MesosSettings mesosSettings,
                               final Observer<String> vmLeaseRescindedObserver,
                               final JobMessageRouter jobMessageRouter,
                               final WorkerRegistry workerRegistry) {
        this.mesosSettings = mesosSettings;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.jobMessageRouter = jobMessageRouter;
        this.workerRegistry = workerRegistry;
    }

    Optional<MesosSchedulerDriver> initMesosSchedulerDriverWithTimeout(MesosSchedulerCallbackHandler mesosSchedulerCallbackHandler,
                                                                       Protos.FrameworkInfo framework) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        long mesosSchedulerDriverInitTimeoutSec = mesosSettings.getSchedulerDriverInitTimeout().getSeconds();
        logger.info("initializing mesos scheduler driver with timeout of {} sec", mesosSchedulerDriverInitTimeoutSec);
        Optional<MesosSchedulerDriver> mesosSchedulerDriverO = Optional.empty();
        try {
            Future<MesosSchedulerDriver> driverF = executorService.submit(() -> new MesosSchedulerDriver(mesosSchedulerCallbackHandler, framework, mesosSettings.getMasterLocation()));
            MesosSchedulerDriver mesosSchedulerDriver = driverF.get(mesosSchedulerDriverInitTimeoutSec, TimeUnit.SECONDS);
            mesosSchedulerDriverO = Optional.ofNullable(mesosSchedulerDriver);
        } catch (Exception e) {
            logger.info("failed to initialize MesosSchedulerDriver", e);
        } finally {
            executorService.shutdown();
        }
        return mesosSchedulerDriverO;
    }

    @Override
    public MesosSchedulerDriver get() {
        if (addVMLeaseAction == null) {
            logger.warn("addVMLeaseAction is null, attempt to get Mesos Driver before MesosDriverSupplier init");
            throw new IllegalStateException("addVMLeaseAction must be set before creating MesosSchedulerDriver");
        }

        if (isInitialized.compareAndSet(false, true)) {
            if (numAttemptsToInit.incrementAndGet() > mesosSettings.getSchedulerDriverInitMaxAttempts()) {
                logger.error("Failed to initialize Mesos scheduler driver after {} attempts, will terminate master",
                    numAttemptsToInit.get() - 1);
                System.exit(2);
            }
            logger.info("initializing mesos scheduler callback handler");
            final MesosSchedulerCallbackHandler mesosSchedulerCallbackHandler =
                    new MesosSchedulerCallbackHandler(addVMLeaseAction, mesosSettings, vmLeaseRescindedObserver, jobMessageRouter,
                            workerRegistry);
            final Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
                    .setUser(mesosSettings.getFrameworkUser())
                    .setName(mesosSettings.getFrameworkName())
                    .setFailoverTimeout(mesosSettings.getFrameworkFailoverTimeout().getSeconds())
                    .setId(Protos.FrameworkID.newBuilder().setValue(mesosSettings.getFrameworkName()))
                    .setCheckpoint(true)
                    .build();
            logger.info("initializing mesos scheduler driver");
            MesosSchedulerDriver mesosDriver = initMesosSchedulerDriverWithTimeout(mesosSchedulerCallbackHandler, framework).orElseGet(() -> {
                logger.info("initialize MesosSchedulerDriver failed, will retry");
                isInitialized.compareAndSet(true, false);
                return this.get();
            });

            boolean result = mesosDriverRef.compareAndSet(null, mesosDriver);
            logger.info("initialized mesos scheduler driver {}", result);
        } else {
            int sleepIntervalMillis = 1000;
            long maxTimeToWaitMillis =
                mesosSettings.getSchedulerDriverInitMaxAttempts() * mesosSettings.getSchedulerDriverInitTimeout().toMillis();
            // block maxTimeToWaitMillis till mesosDriver is not null
            while (mesosDriverRef.get() == null) {
                if (maxTimeToWaitMillis <= 0) {
                    logger.error("mesos driver init taking too long, exiting");
                    System.exit(2);
                }
                try {
                    logger.info("mesos scheduler driver null, sleep for 1 sec awaiting init");
                    Thread.sleep(sleepIntervalMillis);
                    maxTimeToWaitMillis -= sleepIntervalMillis;
                } catch (InterruptedException e) {
                    logger.warn("thread interrupted during sleep", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        return mesosDriverRef.get();
    }

    public void setAddVMLeaseAction(final Action1<List<VirtualMachineLease>> addVMLeaseAction) {
        Preconditions.checkNotNull(addVMLeaseAction);
        this.addVMLeaseAction = addVMLeaseAction;
    }

    public void shutdown() {
        MesosSchedulerDriver mesosSchedulerDriver = mesosDriverRef.get();
        if (mesosSchedulerDriver != null) {
            mesosSchedulerDriver.stop(true);
        } else {
            logger.info("mesos driver null, continue shutdown");
        }
    }
}
