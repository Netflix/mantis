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

package io.mantisrx.server.master;

import static io.mantisrx.server.master.scheduler.ScheduleRequest.DEFAULT_Q_ATTRIBUTES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.netflix.fenzo.AutoScaleAction;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueueException;
import com.netflix.fenzo.queues.tiered.TieredQueue;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.MetricId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.LaunchTaskRequest;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import io.mantisrx.server.master.scheduler.SchedulingStateManager;
import io.mantisrx.server.master.scheduler.WorkerLaunchFailed;
import io.mantisrx.server.master.scheduler.WorkerLaunched;
import io.mantisrx.server.master.scheduler.WorkerRegistry;
import io.mantisrx.server.master.scheduler.WorkerUnscheduleable;
import org.HdrHistogram.SynchronizedHistogram;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

//import io.mantisrx.server.core.domain.WorkerPorts;


public class SchedulingService extends BaseService implements MantisScheduler {

    private static final Logger logger = LoggerFactory.getLogger(SchedulingService.class);
    private final JobMessageRouter jobMessageRouter;
    private final WorkerRegistry workerRegistry;
    private final TaskScheduler taskScheduler;
    private final TaskSchedulingService taskSchedulingService;
    private final TieredQueue taskQueue;
    private final Counter numWorkersLaunched;
    private final Counter numResourceOffersReceived;
    private final Counter numResourceAllocations;
    private final Counter numResourceOffersRejected;
    private final Gauge workersToLaunch;
    private final Gauge pendingWorkers;
    private final Gauge schedulerRunMillis;
    private final Counter perWorkerSchedulingTimeMs;
    private final SynchronizedHistogram workerAcceptedToLaunchedDistMs = new SynchronizedHistogram(3_600_000L, 3);
    private final Gauge totalActiveAgents;
    private final Counter numAgentsUsed;
    private final Gauge idleAgents;
    private final Gauge totalAvailableCPUs;
    private final Gauge totalAllocatedCPUs;
    private final Gauge totalAvailableMemory;
    private final Gauge totalAllocatedMemory;
    private final Gauge totalAvailableNwMbps;
    private final Gauge totalAllocatedNwMbps;
    private final Gauge cpuUtilization;
    private final Gauge memoryUtilization;
    private final Gauge networkUtilization;
    private final Gauge dominantResUtilization;
    private final Gauge fenzoLaunchedTasks;
    private final Gauge jobMgrRunningWorkers;
    private final Counter numAutoScaleUpActions;
    private final Counter numAutoScaleDownActions;
    private final Counter numMissingWorkerPorts;
    private final Counter schedulingResultExceptions;
    private final Counter schedulingCallbackExceptions;
    private final SchedulingStateManager schedulingState;
    private final AtomicInteger idleMachinesCount = new AtomicInteger();
    private final String slaveClusterAttributeName;
    private final long vmCurrentStatesCheckInterval = 10000;
    private final AtomicLong lastVmCurrentStatesCheckDone = new AtomicLong(System.currentTimeMillis());
    private VirtualMachineMasterService virtualMachineService;
    private long SCHEDULING_ITERATION_INTERVAL_MILLIS = 50;
    private long MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITER = 5_000;
    private AtomicLong lastSchedulingResultCallback = new AtomicLong(System.currentTimeMillis());

    public SchedulingService(final JobMessageRouter jobMessageRouter,
                             final WorkerRegistry workerRegistry,
                             final Observable<String> vmLeaseRescindedObservable,
                             final VirtualMachineMasterService virtualMachineService) {
        super(true);
        this.schedulingState = new SchedulingStateManager();
        this.jobMessageRouter = jobMessageRouter;
        this.workerRegistry = workerRegistry;
        this.virtualMachineService = virtualMachineService;
        this.slaveClusterAttributeName = ConfigurationProvider.getConfig().getSlaveClusterAttributeName();
        SCHEDULING_ITERATION_INTERVAL_MILLIS = ConfigurationProvider.getConfig().getSchedulerIterationIntervalMillis();
        AgentFitnessCalculator agentFitnessCalculator = new AgentFitnessCalculator();
        TaskScheduler.Builder schedulerBuilder = new TaskScheduler.Builder()
                .withLeaseRejectAction(virtualMachineService::rejectLease)
                .withLeaseOfferExpirySecs(ConfigurationProvider.getConfig().getMesosLeaseOfferExpirySecs())
                .withFitnessCalculator(agentFitnessCalculator)
                .withFitnessGoodEnoughFunction(agentFitnessCalculator.getFitnessGoodEnoughFunc())
                .withAutoScaleByAttributeName(ConfigurationProvider.getConfig().getAutoscaleByAttributeName()); // set this always
        if (ConfigurationProvider.getConfig().getDisableShortfallEvaluation())
            schedulerBuilder = schedulerBuilder.disableShortfallEvaluation();
        taskScheduler = setupTaskSchedulerAndAutoScaler(vmLeaseRescindedObservable, schedulerBuilder);
        taskScheduler.setActiveVmGroupAttributeName(ConfigurationProvider.getConfig().getActiveSlaveAttributeName());

        taskQueue = new TieredQueue(2);

        taskSchedulingService = setupTaskSchedulingService(taskScheduler);

        setupAutoscaleRulesDynamicUpdater();
        MetricGroupId metricGroupId = new MetricGroupId(SchedulingService.class.getCanonicalName());
        Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addCounter("numWorkersLaunched")
                .addCounter("numResourceOffersReceived")
                .addCounter("numResourceAllocations")
                .addCounter("numResourceOffersRejected")
                .addGauge("workersToLaunch")
                .addGauge("pendingWorkers")
                .addGauge("schedulerRunMillis")
                .addCounter("perWorkerSchedulingTimeMillis")
                .addGauge(new GaugeCallback(metricGroupId, "workerAcceptedToLaunchedMsP50", () -> (double) workerAcceptedToLaunchedDistMs.getValueAtPercentile(50)))
                .addGauge(new GaugeCallback(metricGroupId, "workerAcceptedToLaunchedMsP95", () -> (double) workerAcceptedToLaunchedDistMs.getValueAtPercentile(95)))
                .addGauge(new GaugeCallback(metricGroupId, "workerAcceptedToLaunchedMsP99", () -> (double) workerAcceptedToLaunchedDistMs.getValueAtPercentile(99)))
                .addGauge(new GaugeCallback(metricGroupId, "workerAcceptedToLaunchedMsMax", () -> (double) workerAcceptedToLaunchedDistMs.getValueAtPercentile(100)))
                .addGauge("totalActiveAgents")
                .addCounter("numAgentsUsed")
                .addGauge("idleAgents")
                .addGauge("totalAvailableCPUs")
                .addGauge("totalAllocatedCPUs")
                .addGauge("totalAvailableMemory")
                .addGauge("totalAllocatedMemory")
                .addGauge("totalAvailableNwMbps")
                .addGauge("totalAllocatedNwMbps")
                .addGauge("cpuUtilization")
                .addGauge("memoryUtilization")
                .addGauge("networkUtilization")
                .addGauge("dominantResUtilization")
                .addCounter("numAutoScaleUpActions")
                .addCounter("numAutoScaleDownActions")
                .addGauge("fenzoLaunchedTasks")
                .addGauge("jobMgrRunningWorkers")
                .addCounter("numMissingWorkerPorts")
                .addCounter("schedulingResultExceptions")
                .addCounter("schedulingCallbackExceptions")
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        numWorkersLaunched = m.getCounter("numWorkersLaunched");
        numResourceOffersReceived = m.getCounter("numResourceOffersReceived");
        numResourceAllocations = m.getCounter("numResourceAllocations");
        numResourceOffersRejected = m.getCounter("numResourceOffersRejected");
        workersToLaunch = m.getGauge("workersToLaunch");
        pendingWorkers = m.getGauge("pendingWorkers");
        schedulerRunMillis = m.getGauge("schedulerRunMillis");
        totalActiveAgents = m.getGauge("totalActiveAgents");
        numAgentsUsed = m.getCounter("numAgentsUsed");
        idleAgents = m.getGauge("idleAgents");
        totalAvailableCPUs = m.getGauge("totalAvailableCPUs");
        totalAllocatedCPUs = m.getGauge("totalAllocatedCPUs");
        totalAvailableMemory = m.getGauge("totalAvailableMemory");
        totalAllocatedMemory = m.getGauge("totalAllocatedMemory");
        totalAvailableNwMbps = m.getGauge("totalAvailableNwMbps");
        totalAllocatedNwMbps = m.getGauge("totalAllocatedNwMbps");
        cpuUtilization = m.getGauge("cpuUtilization");
        memoryUtilization = m.getGauge("memoryUtilization");
        networkUtilization = m.getGauge("networkUtilization");
        dominantResUtilization = m.getGauge("dominantResUtilization");
        numAutoScaleUpActions = m.getCounter("numAutoScaleUpActions");
        numAutoScaleDownActions = m.getCounter("numAutoScaleDownActions");
        fenzoLaunchedTasks = m.getGauge("fenzoLaunchedTasks");
        jobMgrRunningWorkers = m.getGauge("jobMgrRunningWorkers");
        numMissingWorkerPorts = m.getCounter("numMissingWorkerPorts");
        schedulingResultExceptions = m.getCounter("schedulingResultExceptions");
        schedulingCallbackExceptions = m.getCounter("schedulingCallbackExceptions");
        perWorkerSchedulingTimeMs = m.getCounter("perWorkerSchedulingTimeMillis");
    }

    private TaskScheduler setupTaskSchedulerAndAutoScaler(Observable<String> vmLeaseRescindedObservable,
                                                          TaskScheduler.Builder schedulerBuilder) {
        int minMinIdle = 4;
        schedulerBuilder = schedulerBuilder
                .withAutoScaleDownBalancedByAttributeName(ConfigurationProvider.getConfig().getHostZoneAttributeName())
                .withAutoScalerMapHostnameAttributeName(ConfigurationProvider.getConfig().getAutoScalerMapHostnameAttributeName());
        final AgentClustersAutoScaler agentClustersAutoScaler = AgentClustersAutoScaler.get();
        try {
            if (agentClustersAutoScaler != null) {
                Set<AutoScaleRule> rules = agentClustersAutoScaler.getRules();
                if (rules != null && !rules.isEmpty()) {
                    for (AutoScaleRule rule : rules) {
                        schedulerBuilder = schedulerBuilder.withAutoScaleRule(rule);
                        minMinIdle = Math.min(minMinIdle, rule.getMinIdleHostsToKeep());
                    }
                } else
                    logger.warn("No auto scale rules setup");
            }
        } catch (IllegalStateException e) {
            logger.warn("Ignoring: " + e.getMessage());
        }
        schedulerBuilder = schedulerBuilder.withMaxOffersToReject(Math.max(1, minMinIdle));
        final TaskScheduler scheduler = schedulerBuilder.build();
        vmLeaseRescindedObservable
                .doOnNext(new Action1<String>() {
                    @Override
                    public void call(String s) {
                        if (s.equals("ALL"))
                            scheduler.expireAllLeases();
                        else
                            scheduler.expireLease(s);
                    }
                })
                .subscribe();
        if (agentClustersAutoScaler != null) {
            final Observer<AutoScaleAction> autoScaleActionObserver = agentClustersAutoScaler.getAutoScaleActionObserver();
            scheduler.setAutoscalerCallback(new com.netflix.fenzo.functions.Action1<AutoScaleAction>() {
                @Override
                public void call(AutoScaleAction action) {
                    try {
                        switch (action.getType()) {
                        case Up:
                            numAutoScaleUpActions.increment();
                            break;
                        case Down:
                            numAutoScaleDownActions.increment();
                            break;
                        }
                        autoScaleActionObserver.onNext(action);
                    } catch (Exception e) {
                        logger.warn("Will continue after exception calling autoscale action observer: " + e.getMessage(), e);
                    }
                }
            });
        }
        return scheduler;
    }

    private void setupAutoscaleRulesDynamicUpdater() {
        final Set<String> emptyHashSet = new HashSet<>();
        Schedulers.computation().createWorker().schedulePeriodically(() -> {
            try {
                logger.debug("Updating cluster autoscale rules");
                final AgentClustersAutoScaler agentClustersAutoScaler = AgentClustersAutoScaler.get();
                if (agentClustersAutoScaler == null) {
                    logger.warn("No agent cluster autoscaler defined, not setting up Fenzo autoscaler rules");
                    return;
                }
                final Set<AutoScaleRule> newRules = agentClustersAutoScaler.getRules();

                final Collection<AutoScaleRule> currRules = taskScheduler.getAutoScaleRules();
                final Set<String> currRulesNames = currRules == null || currRules.isEmpty() ?
                        emptyHashSet :
                        currRules.stream().collect((Supplier<Set<String>>) HashSet::new,
                                (strings, autoScaleRule) -> strings.add(autoScaleRule.getRuleName()),
                                Set::addAll);
                if (newRules != null && !newRules.isEmpty()) {
                    for (AutoScaleRule r : newRules) {
                        logger.debug("Setting up autoscale rule: " + r);
                        taskScheduler.addOrReplaceAutoScaleRule(r);
                        currRulesNames.remove(r.getRuleName());
                    }
                }
                if (!currRulesNames.isEmpty()) {
                    for (String ruleName : currRulesNames) {
                        logger.info("Removing autoscale rule " + ruleName);
                        taskScheduler.removeAutoScaleRule(ruleName);
                    }
                }
            } catch (Exception e) {
                logger.warn("Unexpected error updating cluster autoscale rules: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private TaskSchedulingService setupTaskSchedulingService(TaskScheduler taskScheduler) {
        TaskSchedulingService.Builder builder = new TaskSchedulingService.Builder()
                .withTaskScheduler(taskScheduler)
                .withLoopIntervalMillis(SCHEDULING_ITERATION_INTERVAL_MILLIS)
                .withMaxDelayMillis(MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITER) // sort of rate limiting when no assignments were made and no new offers available
                .withSchedulingResultCallback(this::schedulingResultHandler)
                .withTaskQueue(taskQueue)
                .withOptimizingShortfallEvaluator();
        return builder.build();
    }

    private Optional<String> getAttribute(final VirtualMachineLease lease, final String attributeName) {
        boolean hasValue = lease.getAttributeMap() != null
                && lease.getAttributeMap().get(attributeName) != null
                && lease.getAttributeMap().get(attributeName).getText().hasValue();
        return hasValue ? Optional.of(lease.getAttributeMap().get(attributeName).getText().getValue()) : Optional.empty();
    }

    /**
     * Attempts to launch tasks given some number of leases from Mesos.
     *
     * When a task is launched successfully, the following will happen:
     *
     * 1. Emit a {@link WorkerLaunched} event to be handled by the corresponding actor.
     * 2. Makes a call to the underlying Mesos driver to launch the task.
     *
     * A task can fail to launch if:
     *
     * 1. It doesn't receive enough metadata for {@link WorkerPorts} to pass its preconditions.
     *      - No launch task request will be made for this assignment result.
     *      - Proactively unschedule the worker.
     * 2. It fails to emit a {@link WorkerLaunched} event.
     *      - The worker will get unscheduled for this launch task request.
     * 3. There are no launch tasks for this assignment result.
     *      - All of these leases are rejected.
     *      - Eventually, the underlying Mesos driver will decline offers since there are no launch task requests.
     *
     * @param requests collection of assignment results received by the scheduler.
     * @param leases list of resource offers from Mesos.
     */
    private void launchTasks(Collection<TaskAssignmentResult> requests, List<VirtualMachineLease> leases) {
        List<LaunchTaskRequest> launchTaskRequests = new ArrayList<>();

        for (TaskAssignmentResult assignmentResult : requests) {
            ScheduleRequest request = (ScheduleRequest) assignmentResult.getRequest();

            WorkerPorts workerPorts = null;
            try {
                workerPorts = new WorkerPorts(assignmentResult.getAssignedPorts());
            } catch (IllegalArgumentException | IllegalStateException e) {
                logger.error("problem launching tasks for assignment result {}: {}", assignmentResult, e);
                numMissingWorkerPorts.increment();
            }

            if (workerPorts != null) {
                boolean success = jobMessageRouter.routeWorkerEvent(new WorkerLaunched(
                        request.getWorkerId(),
                        request.getStageNum(),
                        leases.get(0).hostname(),
                        leases.get(0).getVMID(),
                        getAttribute(leases.get(0), slaveClusterAttributeName),
                        workerPorts));

                if (success) {
                    launchTaskRequests.add(new LaunchTaskRequest(request, workerPorts));
                } else {
                    unscheduleWorker(request.getWorkerId(), Optional.ofNullable(leases.get(0).hostname()));
                }
            } else {
                unscheduleWorker(request.getWorkerId(), Optional.ofNullable(leases.get(0).hostname()));
            }
        }

        if (launchTaskRequests.isEmpty()) {
            for (VirtualMachineLease l : leases)
                virtualMachineService.rejectLease(l);
        }

        Map<ScheduleRequest, LaunchTaskException> launchErrors = virtualMachineService.launchTasks(launchTaskRequests, leases);
        for (TaskAssignmentResult result : requests) {
            final ScheduleRequest sre = (ScheduleRequest) result.getRequest();
            if (launchErrors.containsKey(sre)) {
                String errorMessage = getWorkerStringPrefix(sre.getStageNum(), sre.getWorkerId()) +
                        " failed due to " + launchErrors.get(sre).getMessage();
                boolean success = jobMessageRouter.routeWorkerEvent(new WorkerLaunchFailed(sre.getWorkerId(), sre.getStageNum(), errorMessage));
                if (!success) {
                    logger.warn("Failed to route WorkerLaunchFailed for {} (err {})", sre.getWorkerId(), errorMessage);
                }
            }
        }
    }

    private String getWorkerStringPrefix(int stageNum, final WorkerId workerId) {
        return "stage " + stageNum + " worker index=" + workerId.getWorkerIndex() + " number=" + workerId.getWorkerNum();
    }

    private void schedulingResultHandler(SchedulingResult schedulingResult) {
        try {
            lastSchedulingResultCallback.set(System.currentTimeMillis());

            final List<Exception> exceptions = schedulingResult.getExceptions();
            for (Exception exc : exceptions) {
                logger.error("Scheduling result got exception: {}", exc.getMessage(), exc);
                schedulingResultExceptions.increment();
            }
            int workersLaunched = 0;
            SchedulerCounters.getInstance().incrementResourceAllocationTrials(schedulingResult.getNumAllocations());
            Map<String, VMAssignmentResult> assignmentResultMap = schedulingResult.getResultMap();
            final int assignmentResultSize;
            if (assignmentResultMap != null) {
                assignmentResultSize = assignmentResultMap.size();
                long now = System.currentTimeMillis();
                for (Map.Entry<String, VMAssignmentResult> aResult : assignmentResultMap.entrySet()) {
                    launchTasks(aResult.getValue().getTasksAssigned(), aResult.getValue().getLeasesUsed());
                    for (TaskAssignmentResult r : aResult.getValue().getTasksAssigned()) {
                        final ScheduleRequest request = (ScheduleRequest) r.getRequest();
                        final Optional<Long> acceptedAt = workerRegistry.getAcceptedAt(request.getWorkerId());
                        acceptedAt.ifPresent(acceptedAtTime -> workerAcceptedToLaunchedDistMs.recordValue(now - acceptedAtTime));
                        perWorkerSchedulingTimeMs.increment(now - request.getReadyAt());
                    }
                    workersLaunched += aResult.getValue().getTasksAssigned().size();
                }
            } else {
                assignmentResultSize = 0;
            }
            // for workers that didn't get scheduled, rate limit them
            for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry : schedulingResult.getFailures().entrySet()) {
                final ScheduleRequest req = (ScheduleRequest) entry.getKey();
                boolean success = jobMessageRouter.routeWorkerEvent(new WorkerUnscheduleable(req.getWorkerId(), req.getStageNum()));
                if (!success) {
                    logger.warn("Failed to route {} WorkerUnscheduleable event", req.getWorkerId());
                    if (logger.isTraceEnabled()) {
                        logger.trace("Unscheduleable worker {} assignmentresults {}", req.getWorkerId(), entry.getValue());
                    }
                }
            }
            numWorkersLaunched.increment(workersLaunched);
            numResourceOffersReceived.increment(schedulingResult.getLeasesAdded());
            numResourceAllocations.increment(schedulingResult.getNumAllocations());
            numResourceOffersRejected.increment(schedulingResult.getLeasesRejected());
            final int requestedWorkers = workersLaunched + schedulingResult.getFailures().size();
            workersToLaunch.set(requestedWorkers);
            pendingWorkers.set(schedulingResult.getFailures().size());
            schedulerRunMillis.set(schedulingResult.getRuntime());
            totalActiveAgents.set(schedulingResult.getTotalVMsCount());
            numAgentsUsed.increment(assignmentResultSize);
            final int idleVMsCount = schedulingResult.getIdleVMsCount();
            idleAgents.set(idleVMsCount);
            SchedulerCounters.getInstance().endIteration(requestedWorkers, workersLaunched, assignmentResultSize,
                    schedulingResult.getLeasesRejected());
            if (requestedWorkers > 0 && SchedulerCounters.getInstance().getCounter().getIterationNumber() % 10 == 0) {
                logger.info("Scheduling iteration result: " + SchedulerCounters.getInstance().toJsonString());
            }
            if (idleVMsCount != idleMachinesCount.get()) {
                logger.info("Idle machines: " + idleVMsCount);
                idleMachinesCount.set(idleVMsCount);
            }

            try {
                taskSchedulingService.requestVmCurrentStates(vmCurrentStates -> {
                    if (lastVmCurrentStatesCheckDone.get() < (System.currentTimeMillis() - vmCurrentStatesCheckInterval)) {
                        schedulingState.setVMCurrentState(vmCurrentStates);
                        verifyAndReportResUsageMetrics(vmCurrentStates);
                        lastVmCurrentStatesCheckDone.set(System.currentTimeMillis());
                    }
                });
            } catch (final TaskQueueException e) {
                logger.warn("got exception requesting VM states from Fenzo", e);
            }
            publishJobManagerAndFenzoWorkerMetrics();
        } catch (final Exception e) {
            logger.error("unexpected exception in scheduling result callback", e);
            schedulingCallbackExceptions.increment();
        }
    }

    @Override
    public void initializeRunningWorker(final ScheduleRequest request, String hostname) {
        taskSchedulingService.initializeRunningTask(request, hostname);
    }

    @Override
    public void scheduleWorker(final ScheduleRequest scheduleRequest) {
        taskQueue.queueTask(scheduleRequest);
    }

    @Override
    public void unscheduleWorker(final WorkerId workerId, final Optional<String> hostname) {
        taskSchedulingService.removeTask(workerId.getId(), DEFAULT_Q_ATTRIBUTES, hostname.orElse(null));
    }

    @Override
    public void unscheduleAndTerminateWorker(final WorkerId workerId, final Optional<String> hostname) {
        taskSchedulingService.removeTask(workerId.getId(), DEFAULT_Q_ATTRIBUTES, hostname.orElse(null));
        virtualMachineService.killTask(workerId);
    }

    @Override
    public void updateWorkerSchedulingReadyTime(WorkerId workerId, long when) {
        if (logger.isTraceEnabled()) {
            logger.trace("setting task {} ready time to {}", workerId, new DateTime(when));
        }
        taskSchedulingService.setTaskReadyTime(workerId.toString(), DEFAULT_Q_ATTRIBUTES, when);
    }

    @Override
    public void rescindOffer(final String offerId) {
        if (offerId.equals("ALL")) {
            taskScheduler.expireAllLeases();
        } else {
            taskScheduler.expireLease(offerId);
        }
    }

    @Override
    public void addOffers(final List<VirtualMachineLease> offers) {
        taskSchedulingService.addLeases(offers);
    }

    @Override
    public void rescindOffers(final String hostname) {
        taskScheduler.expireAllLeases(hostname);
    }

    @Override
    public void disableVM(String hostname, long durationMillis) throws IllegalStateException {
        taskScheduler.disableVM(hostname, durationMillis);
    }

    @Override
    public void enableVM(final String hostname) {
        taskScheduler.enableVM(hostname);
    }

    @Override
    public List<VirtualMachineCurrentState> getCurrentVMState() {
        return schedulingState.getVMCurrentState();
    }

    @Override
    public void setActiveVmGroups(final List<String> activeVmGroups) {
        if (activeVmGroups != null) {
            taskScheduler.setActiveVmGroups(activeVmGroups);
        }
    }

    private void setupSchedulingServiceWatcherMetric() {
        logger.info("Setting up SchedulingServiceWatcher metrics");
        lastSchedulingResultCallback.set(System.currentTimeMillis());
        final String metricGroup = "SchedulingServiceWatcher";
        final GaugeCallback timeSinceLastSchedulingRunGauge = new GaugeCallback(new MetricId(metricGroup, "timeSinceLastSchedulingRunMs"),
                () -> (double) (System.currentTimeMillis() - lastSchedulingResultCallback.get()),
                SpectatorRegistryFactory.getRegistry());
        final Metrics schedulingServiceWatcherMetrics = new Metrics.Builder()
                .id(metricGroup)
                .addGauge(timeSinceLastSchedulingRunGauge)
                .build();
        MetricsRegistry.getInstance().registerAndGet(schedulingServiceWatcherMetrics);
    }

    @Override
    public void start() {
        super.awaitActiveModeAndStart(() -> {
            logger.info("Scheduling service starting now");
            taskSchedulingService.start();

            setupSchedulingServiceWatcherMetric();
            if (logger.isDebugEnabled()) {
                try {
                    taskSchedulingService.requestAllTasks(taskStateCollectionMap -> taskStateCollectionMap.forEach((state, tasks) -> {
                        logger.debug("state {} tasks {}", state, tasks.toString());
                    }));
                } catch (TaskQueueException e) {
                    logger.error("caught exception", e);
                }
            }
        });
    }

    private void publishJobManagerAndFenzoWorkerMetrics() {
        try {
            taskSchedulingService.requestAllTasks(taskStateCollectionMap -> taskStateCollectionMap.forEach((state, tasks) -> {
                final int fenzoTaskSetSize = tasks.size();
                if (state == TaskQueue.TaskState.LAUNCHED) {
                    final int numRunningWorkers = workerRegistry.getNumRunningWorkers();
                    fenzoLaunchedTasks.set(fenzoTaskSetSize);
                    jobMgrRunningWorkers.set(numRunningWorkers);

                    if (numRunningWorkers != fenzoTaskSetSize) {
                        logger.error("{} running workers as per Job Manager, {} tasks launched as per Fenzo", numRunningWorkers, fenzoTaskSetSize);
                        if (logger.isDebugEnabled()) {
                            final Set<String> jobMgrWorkers = workerRegistry.getAllRunningWorkers()
                                    .stream()
                                    .map(w -> w.getId())
                                    .collect(Collectors.toSet());

                            final Set<String> fenzoWorkers = tasks
                                    .stream()
                                    .map(t -> t.getId())
                                    .collect(Collectors.toSet());

                            final Sets.SetView<String> extraJobMgrWorkers = Sets.difference(jobMgrWorkers, fenzoWorkers);
                            logger.debug("Job Manager workers not in Fenzo {}", extraJobMgrWorkers);
                            final Sets.SetView<String> extraFenzoWorkers = Sets.difference(fenzoWorkers, jobMgrWorkers);
                            logger.debug("Fenzo workers not in JobManagers {}", extraFenzoWorkers);
                        }
                    }

                } else {
                    logger.debug("{} {} tasks {}", fenzoTaskSetSize, state, tasks);
                }
            }));
        } catch (Exception e) {
            logger.error("caught exception when publishing worker metrics", e);
        }
    }

    private void verifyAndReportResUsageMetrics(List<VirtualMachineCurrentState> vmCurrentStates) {
        double totalCPU = 0.0;
        double usedCPU = 0.0;
        double totalMemory = 0.0;
        double usedMemory = 0.0;
        double totalNwMbps = 0.0;
        double usedNwMbps = 0.0;
        for (VirtualMachineCurrentState state : vmCurrentStates) {
            final VirtualMachineLease currAvailableResources = state.getCurrAvailableResources();
            if (currAvailableResources != null) {
                totalCPU += currAvailableResources.cpuCores();
                totalMemory += currAvailableResources.memoryMB();
                totalNwMbps += currAvailableResources.networkMbps();
            }
            final Collection<TaskRequest> runningTasks = state.getRunningTasks();
            if (runningTasks != null) {
                for (TaskRequest t : runningTasks) {
                    Optional<WorkerId> workerId = WorkerId.fromId(t.getId());
                    if (!workerId.isPresent() || !workerRegistry.isWorkerValid(workerId.get())) {
                        taskSchedulingService.removeTask(t.getId(), DEFAULT_Q_ATTRIBUTES, state.getHostname());
                    } else {
                        usedCPU += t.getCPUs();
                        totalCPU += t.getCPUs();
                        usedMemory += t.getMemory();
                        totalMemory += t.getMemory();
                        usedNwMbps += t.getNetworkMbps();
                        totalNwMbps += t.getNetworkMbps();
                    }
                }
            }
        }
        totalAvailableCPUs.set((long) totalCPU);
        totalAllocatedCPUs.set((long) usedCPU);
        cpuUtilization.set((long) (usedCPU * 100.0 / totalCPU));
        double DRU = usedCPU * 100.0 / totalCPU;
        totalAvailableMemory.set((long) totalMemory);
        totalAllocatedMemory.set((long) usedMemory);
        memoryUtilization.set((long) (usedMemory * 100.0 / totalMemory));
        DRU = Math.max(DRU, usedMemory * 100.0 / totalMemory);
        totalAvailableNwMbps.set((long) totalNwMbps);
        totalAllocatedNwMbps.set((long) usedNwMbps);
        networkUtilization.set((long) (usedNwMbps * 100.0 / totalNwMbps));
        DRU = Math.max(DRU, usedNwMbps * 100.0 / totalNwMbps);
        dominantResUtilization.set((long) DRU);
    }

    @Override
    public void shutdown() {
        if (!taskSchedulingService.isShutdown()) {
            logger.info("shutting down Task Scheduling Service");
            taskSchedulingService.shutdown();
        }
    }
}
