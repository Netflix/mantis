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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.mantisrx.shaded.com.google.common.base.Preconditions;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus;
import io.mantisrx.server.master.scheduler.WorkerResourceStatus.VMResourceState;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;


public class MesosSchedulerCallbackHandler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(MesosSchedulerCallbackHandler.class);
    private final Action1<List<VirtualMachineLease>> addVMLeaseAction;
    private final WorkerRegistry workerRegistry;
    private final Gauge lastOfferReceivedMillis;
    private final Gauge lastValidOfferReceiveMillis;
    private final Counter numMesosRegistered;
    private final Counter numMesosDisconnects;
    private final Counter numOfferRescinded;
    private final Counter numReconcileTasks;
    private final Counter numInvalidOffers;
    private final Counter numOfferTooSmall;
    private Observer<String> vmLeaseRescindedObserver;
    private JobMessageRouter jobMessageRouter;
    private volatile ScheduledFuture reconcilerFuture = null;
    private AtomicLong lastOfferReceivedAt = new AtomicLong(System.currentTimeMillis());
    private AtomicLong lastValidOfferReceivedAt = new AtomicLong(System.currentTimeMillis());
    private long reconciliationTrial = 0;

    public MesosSchedulerCallbackHandler(
            final Action1<List<VirtualMachineLease>> addVMLeaseAction,
            final Observer<String> vmLeaseRescindedObserver,
            final JobMessageRouter jobMessageRouter,
            final WorkerRegistry workerRegistry) {
        this.addVMLeaseAction = Preconditions.checkNotNull(addVMLeaseAction);
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.jobMessageRouter = jobMessageRouter;
        this.workerRegistry = workerRegistry;
        Metrics m = new Metrics.Builder()
                .name(MesosSchedulerCallbackHandler.class.getCanonicalName())
                .addCounter("numMesosRegistered")
                .addCounter("numMesosDisconnects")
                .addCounter("numOfferRescinded")
                .addCounter("numReconcileTasks")
                .addGauge("lastOfferReceivedMillis")
                .addGauge("lastValidOfferReceiveMillis")
                .addCounter("numInvalidOffers")
                .addCounter("numOfferTooSmall")
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        numMesosRegistered = m.getCounter("numMesosRegistered");
        numMesosDisconnects = m.getCounter("numMesosDisconnects");
        numOfferRescinded = m.getCounter("numOfferRescinded");
        numReconcileTasks = m.getCounter("numReconcileTasks");
        lastOfferReceivedMillis = m.getGauge("lastOfferReceivedMillis");
        lastValidOfferReceiveMillis = m.getGauge("lastValidOfferReceiveMillis");
        numInvalidOffers = m.getCounter("numInvalidOffers");
        numOfferTooSmall = m.getCounter("numOfferTooSmall");
        Observable
                .interval(10, 10, TimeUnit.SECONDS)
                .doOnNext(aLong -> {
                    lastOfferReceivedMillis.set(System.currentTimeMillis() - lastOfferReceivedAt.get());
                    lastValidOfferReceiveMillis.set(System.currentTimeMillis() - lastValidOfferReceivedAt.get());
                })
                .subscribe();
    }

    // simple offer resource validator
    private boolean validateOfferResources(Offer offer) {
        for (Protos.Resource resource : offer.getResourcesList()) {
            if ("cpus".equals(resource.getName())) {
                final double cpus = resource.getScalar().getValue();
                if (cpus < 0.1) {
                    logger.warn("Declining offer due to too few CPUs in offer from " + offer.getHostname() +
                            ": " + cpus);
                    return false;
                }
            } else if ("mem".equals(resource.getName())) {
                double memoryMB = resource.getScalar().getValue();
                if (memoryMB < 1) {
                    logger.warn("Declining offer due to too few memory in offer from " + offer.getHostname() +
                            ": " + memoryMB);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        lastOfferReceivedAt.set(System.currentTimeMillis());
        double refuseSecs = 10000;
        final List<VirtualMachineLease> leases = new ArrayList<>();
        for (Offer offer : offers) {
            //            if(!filterActiveVMs(offer)) {
            //                // decline offer from inactive VMs
            //                logger.info("Declining offer from host that is not active: " + offer.getHostname());
            //                driver.declineOffer(offer.getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(60).build());
            //                numInvalidOffers.increment();
            //                continue;
            //            }
            if (ConfigurationProvider.getConfig().getUseSlaveFiltering()) {
                String attrName = ConfigurationProvider.getConfig().getSlaveFilterAttributeName();
                String attrValue = null;
                if (offer.getAttributesCount() > 0) {
                    for (Protos.Attribute attribute : offer.getAttributesList()) {
                        if (attrName.equals(attribute.getName())) {
                            attrValue = attribute.getText().getValue();
                            break;
                        }
                    }
                }
                if (attrValue == null || !attrValue.equals(System.getenv(attrName))) {
                    driver.declineOffer(offer.getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(refuseSecs).build());
                    logger.warn("Declining offer from host " + offer.getHostname() + " due to missing attribute value for " + attrName + " - expecting [" +
                            System.getenv(attrName) + "] got [" + attrValue + "]");
                    numInvalidOffers.increment();
                    continue;
                }
            }
            if (!validateOfferResources(offer)) {
                // decline for a minute
                driver.declineOffer(offer.getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(60).build());
                numOfferTooSmall.increment();
                continue;
            }
            leases.add(new VirtualMachineLeaseMesosImpl(offer));
            lastValidOfferReceivedAt.set(System.currentTimeMillis());
        }
        addVMLeaseAction.call(leases);
    }

    //    private boolean filterActiveVMs(Offer offer) {
    //        if(activeSlaveAttributeName==null || activeSlaveAttributeName.isEmpty())
    //            return true; // not filtering
    //        final List<String> list = activeSlaveAttributeValuesGetter.call();
    //        if(list==null || list.isEmpty())
    //            return true; // all are active
    //        if(offer.getAttributesCount()>0) {
    //            for(Protos.Attribute attribute: offer.getAttributesList()) {
    //                if(activeSlaveAttributeName.equals(attribute.getName())) {
    //                    if(isIn(attribute.getText().getValue(), list))
    //                        return true;
    //                }
    //            }
    //        }
    //        else
    //            logger.info("Filtering slave with no attributes: " + offer.getHostname());
    //        return false;
    //    }

    private boolean isIn(String value, List<String> list) {
        if (value == null || value.isEmpty() || list == null || list.isEmpty())
            return false;
        for (String s : list)
            if (value.equals(s))
                return true;
        return false;
    }

    @Override
    public void disconnected(SchedulerDriver arg0) {
        logger.warn("Mesos driver disconnected: " + arg0);
        numMesosDisconnects.increment();
    }

    @Override
    public void error(SchedulerDriver arg0, String msg) {
        logger.error("Error from Mesos: " + msg);
    }

    @Override
    public void executorLost(SchedulerDriver arg0, ExecutorID arg1,
                             SlaveID arg2, int arg3) {
        logger.warn("Lost executor " + arg1.getValue() + " on slave " + arg2.getValue() + " with status=" + arg3);
    }

    @Override
    public void frameworkMessage(SchedulerDriver arg0, ExecutorID arg1,
                                 SlaveID arg2, byte[] arg3) {
        logger.warn("Unexpected framework message: executorId=" + arg1.getValue() +
                ", slaveID=" + arg2.getValue() + ", message=" + arg3);
    }

    @Override
    public void offerRescinded(SchedulerDriver arg0, OfferID arg1) {
        logger.warn("Offer rescinded: offerID=" + arg1.getValue());
        vmLeaseRescindedObserver.onNext(arg1.getValue());
        numOfferRescinded.increment();
    }

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkID,
                           MasterInfo masterInfo) {
        logger.info("Mesos registered: " + driver + ", ID=" + frameworkID.getValue() + ", masterInfo=" + masterInfo.getId());
        initializeNewDriver(driver);
        numMesosRegistered.increment();
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo arg1) {
        logger.info("Mesos re-registered: " + driver + ", masterInfo=" + arg1.getId());
        initializeNewDriver(driver);
        numMesosRegistered.increment();
    }

    private synchronized void initializeNewDriver(final SchedulerDriver driver) {
        vmLeaseRescindedObserver.onNext("ALL");
        if (reconcilerFuture != null)
            reconcilerFuture.cancel(true);
        reconcilerFuture = new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                reconcileTasks(driver);
            }
        }, 30, ConfigurationProvider.getConfig().getMesosTaskReconciliationIntervalSecs(), TimeUnit.SECONDS);
    }

    private void reconcileTasks(final SchedulerDriver driver) {
        try {
            if (reconciliationTrial++ % 2 == 0)
                reconcileTasksKnownToUs(driver);
            else
                reconcileAllMesosTasks(driver);
        } catch (Exception e) {
            // we don't want to throw errors lest periodically scheduled reconciliation be cancelled
            logger.error("Unexpected error (continuing): " + e.getMessage(), e);
        }
    }

    private void reconcileTasksKnownToUs(SchedulerDriver driver) {
        final List<TaskStatus> tasksToInitialize = new ArrayList<>();
        for (Map.Entry<WorkerId, String> workerIdSlaveId : workerRegistry.getAllRunningWorkerSlaveIdMappings().entrySet()) {
            final WorkerId workerId = workerIdSlaveId.getKey();
            final String slaveId = workerIdSlaveId.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("reconcile running worker mapping {} -> {}", workerId.getId(), slaveId);
            }
            tasksToInitialize.add(TaskStatus.newBuilder()
                    .setTaskId(
                            Protos.TaskID.newBuilder()
                                    .setValue(workerId.getId())
                                    .build())
                    .setState(Protos.TaskState.TASK_RUNNING)
                    .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
                    .build()
            );
        }
        if (!tasksToInitialize.isEmpty()) {
            Protos.Status status = driver.reconcileTasks(tasksToInitialize);
            numReconcileTasks.increment();
            logger.info("Sent request to reconcile " + tasksToInitialize.size() + " tasks, status=" + status);
            logger.info("Last offer received " + (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000 + " secs ago");
            logger.info("Last valid offer received " + (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000 + " secs ago");
            switch (status) {
            case DRIVER_ABORTED:
            case DRIVER_STOPPED:
                logger.error("Unexpected to see Mesos driver status of " + status + " from reconcile request. Committing suicide!");
                System.exit(2);
            }
        }
    }

    private void reconcileAllMesosTasks(SchedulerDriver driver) {
        Protos.Status status = driver.reconcileTasks(Collections.emptyList());
        numReconcileTasks.increment();
        logger.info("Sent request to reconcile all tasks known to Mesos");
        logger.info("Last offer received " + (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000 + " secs ago");
        logger.info("Last valid offer received " + (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000 + " secs ago");
        switch (status) {
        case DRIVER_ABORTED:
        case DRIVER_STOPPED:
            logger.error("Unexpected to see Mesos driver status of " + status + " from reconcile request (all tasks). Committing suicide!");
            System.exit(2);
        }
    }

    @Override
    public void slaveLost(SchedulerDriver arg0, SlaveID arg1) {
        logger.warn("Lost slave " + arg1.getValue());
    }

    @Override
    public void statusUpdate(final SchedulerDriver arg0, TaskStatus arg1) {
        Optional<WorkerId> workerIdO = WorkerId.fromId(arg1.getTaskId().getValue());
        logger.debug("Task status update: ({}) state: {}({}) - {}",
                arg1.getTaskId().getValue(),
                arg1.getState(),
                arg1.getState().getNumber(),
                arg1.getMessage());
        if (workerIdO.isPresent()) {
            WorkerId workerId = workerIdO.get();
            VMResourceState state;
            String mesg = "Mesos task " + arg1.getState() + "-" + arg1.getMessage();
            switch (arg1.getState()) {
            case TASK_FAILED:
            case TASK_LOST:
                state = VMResourceState.FAILED;
                break;
            case TASK_FINISHED:
                state = VMResourceState.COMPLETED;
                break;
            case TASK_RUNNING:
                state = VMResourceState.STARTED;
                break;
            case TASK_STAGING:
            case TASK_STARTING:
                state = VMResourceState.START_INITIATED;
                break;
            default:
                logger.warn("Unexpected Mesos task state " + arg1.getState());
                return;
            }
            jobMessageRouter.routeWorkerEvent(new WorkerResourceStatus(workerId, mesg, state));
        } else {
            logger.error("Failed to parse workerId from Mesos task update {}", arg1.getTaskId().getValue());
        }
    }

}
