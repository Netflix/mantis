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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
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

    public MesosDriverSupplier(final MasterConfiguration masterConfig,
                               final Observer<String> vmLeaseRescindedObserver,
                               final JobMessageRouter jobMessageRouter,
                               final WorkerRegistry workerRegistry) {
        this.masterConfig = masterConfig;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.jobMessageRouter = jobMessageRouter;
        this.workerRegistry = workerRegistry;
    }

    @Override
    public MesosSchedulerDriver get() {
        if (addVMLeaseAction == null) {
            throw new IllegalStateException("addVMLeaseAction must be set before creating MesosSchedulerDriver");
        }

        if (isInitialized.compareAndSet(false, true)) {
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
            final MesosSchedulerDriver mesosDriver =
                    new MesosSchedulerDriver(mesosSchedulerCallbackHandler, framework, masterConfig.getMasterLocation());
            mesosDriverRef.compareAndSet(null, mesosDriver);
        }

        return mesosDriverRef.get();
    }

    public void setAddVMLeaseAction(final Action1<List<VirtualMachineLease>> addVMLeaseAction) {
        Preconditions.checkNotNull(addVMLeaseAction);
        this.addVMLeaseAction = addVMLeaseAction;
    }
}
