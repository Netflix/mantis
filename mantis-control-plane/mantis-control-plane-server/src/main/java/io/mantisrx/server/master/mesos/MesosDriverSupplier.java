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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.mantisrx.shaded.com.google.common.base.Preconditions;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.functions.Action1;


public class MesosDriverSupplier implements Supplier<MesosSchedulerDriver> {

    private static final Logger logger = LoggerFactory.getLogger(MesosDriverSupplier.class);

    private final MasterConfiguration masterConfig;
    private final Observer<String> vmLeaseRescindedObserver;
    private final JobMessageRouter jobMessageRouter;
    private final WorkerRegistry workerRegistry;
    private final AtomicReference<MesosSchedulerDriver> mesosDriverRef = new AtomicReference<>(null);
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private volatile Action1<List<VirtualMachineLease>> addVMLeaseAction = null;
    private final AtomicInteger numAttemptsToInit = new AtomicInteger(0);

    public MesosDriverSupplier(final MasterConfiguration masterConfig,
                               final Observer<String> vmLeaseRescindedObserver,
                               final JobMessageRouter jobMessageRouter,
                               final WorkerRegistry workerRegistry) {
        this.masterConfig = masterConfig;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.jobMessageRouter = jobMessageRouter;
        this.workerRegistry = workerRegistry;
    }

    MesosSchedulerDriver initMesosSchedulerDriverWithTimeout(MesosSchedulerCallbackHandler mesosSchedulerCallbackHandler,
                                                             Protos.FrameworkInfo framework) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        int mesosSchedulerDriverInitTimeoutSec = masterConfig.getMesosSchedulerDriverInitTimeoutSec();
        logger.info("initializing mesos scheduler driver with timeout of {} sec", mesosSchedulerDriverInitTimeoutSec);
        Future<MesosSchedulerDriver> driverF = executorService.submit(() -> new MesosSchedulerDriver(mesosSchedulerCallbackHandler, framework, masterConfig.getMasterLocation()));
        MesosSchedulerDriver mesosSchedulerDriver = driverF.get(mesosSchedulerDriverInitTimeoutSec, TimeUnit.SECONDS);
        executorService.shutdown();
        return mesosSchedulerDriver;
    }

    @Override
    public MesosSchedulerDriver get() {
        if (addVMLeaseAction == null) {
            logger.warn("addVMLeaseAction is null, attempt to get Mesos Driver before MesosDriverSupplier init");
            throw new IllegalStateException("addVMLeaseAction must be set before creating MesosSchedulerDriver");
        }

        if (isInitialized.compareAndSet(false, true)) {
            if (numAttemptsToInit.incrementAndGet() >= masterConfig.getMesosSchedulerDriverInitNumRetries()) {
                logger.error("too many attempts({} > {}) to initialize Mesos scheduler driver, will terminate master",
                    numAttemptsToInit.get(), masterConfig.getMesosSchedulerDriverInitNumRetries());
                System.exit(2);
            }
            logger.info("initializing mesos scheduler callback handler");
            final MesosSchedulerCallbackHandler mesosSchedulerCallbackHandler =
                    new MesosSchedulerCallbackHandler(addVMLeaseAction, vmLeaseRescindedObserver, jobMessageRouter,
                            workerRegistry);
            final Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
                    .setUser("")
                    .setName(masterConfig.getMantisFrameworkName())
                    .setFailoverTimeout(masterConfig.getMesosFailoverTimeOutSecs())
                    .setId(Protos.FrameworkID.newBuilder().setValue(masterConfig.getMantisFrameworkName()))
                    .setCheckpoint(true)
                    .build();
            logger.info("initializing mesos scheduler driver");
            MesosSchedulerDriver mesosDriver;
            try {
                mesosDriver = initMesosSchedulerDriverWithTimeout(mesosSchedulerCallbackHandler, framework);
            } catch (Exception e) {
                logger.info("timed out trying to initialize MesosSchedulerDriver, will retry", e);
                isInitialized.compareAndSet(true, false);
                mesosDriver = get();
            }

            boolean result = mesosDriverRef.compareAndSet(null, mesosDriver);
            logger.info("initialized mesos scheduler driver {}", result);
        } else {
            // block till mesosDriver is not null
            while (mesosDriverRef.get() == null) {
                try {
                    logger.info("mesos scheduler driver null, sleep for 1 sec awaiting init");
                    Thread.sleep(1000);
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
}
