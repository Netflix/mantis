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

package io.mantisrx.master.vm;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.util.DateTimeExt;
import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.AgentClustersAutoScaler;
import io.mantisrx.server.master.persistence.IMantisStorageProvider;
import io.mantisrx.server.master.scheduler.JobMessageRouter;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.server.master.scheduler.WorkerOnDisabledVM;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class AgentClusterOperationsImpl extends BaseService implements AgentClusterOperations {
    static class ActiveVmAttributeValues {
        private final List<String> values;
        @JsonCreator
        ActiveVmAttributeValues(@JsonProperty("values") List<String> values) {
            this.values = values;
        }
        List<String> getValues() {
            return values;
        }
        boolean isEmpty() {
            return values==null || values.isEmpty();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AgentClusterOperationsImpl.class);
    private final IMantisStorageProvider storageProvider;
    private final JobMessageRouter jobMessageRouter;
    private final MantisScheduler scheduler;
    private final LifecycleEventPublisher lifecycleEventPublisher;
    private volatile ActiveVmAttributeValues activeVmAttributeValues=null;
    private final ConcurrentMap<String, List<VirtualMachineCurrentState>> vmStatesMap;
    private final AgentClustersAutoScaler agentClustersAutoScaler;
    private final String attrName;
    private final Counter listJobsOnVMsCount;

    public AgentClusterOperationsImpl(final IMantisStorageProvider storageProvider,
                                      final JobMessageRouter jobMessageRouter,
                                      final MantisScheduler scheduler,
                                      final LifecycleEventPublisher lifecycleEventPublisher,
                                      final String activeSlaveAttributeName) {
        super(true);
        Preconditions.checkNotNull(storageProvider, "storageProvider");
        Preconditions.checkNotNull(jobMessageRouter, "jobMessageRouter");
        Preconditions.checkNotNull(scheduler, "scheduler");
        Preconditions.checkNotNull(lifecycleEventPublisher, "lifecycleEventPublisher");
        Preconditions.checkNotNull(activeSlaveAttributeName, "activeSlaveAttributeName");
        this.storageProvider = storageProvider;
        this.jobMessageRouter = jobMessageRouter;
        this.scheduler = scheduler;
        this.lifecycleEventPublisher = lifecycleEventPublisher;
        this.vmStatesMap = new ConcurrentHashMap<>();
        this.agentClustersAutoScaler = AgentClustersAutoScaler.get();
        this.attrName = activeSlaveAttributeName;
        Metrics metrics = new Metrics.Builder()
            .id("AgentClusterOperations")
            .addCounter("listJobsOnVMsCount")
            .build();
        this.listJobsOnVMsCount = metrics.getCounter("listJobsOnVMsCount");
    }

    @Override
    public void start() {
        super.awaitActiveModeAndStart(() -> {
            try {
                Schedulers.computation().createWorker().schedulePeriodically(
                    () -> checkInactiveVMs(scheduler.getCurrentVMState()),
                    1,
                    30,
                    TimeUnit.SECONDS);
                List<String> activeVmGroups = storageProvider.initActiveVmAttributeValuesList();
                activeVmAttributeValues = new ActiveVmAttributeValues(activeVmGroups);
                scheduler.setActiveVmGroups(activeVmAttributeValues.getValues());
                logger.info("Initialized activeVmAttributeValues=" + (activeVmAttributeValues == null ? "null" : activeVmAttributeValues.getValues()));
            } catch (IOException e) {
                logger.error("Can't initialize activeVM attribute values list: " + e.getMessage());
            }
        });
    }

    @Override
    public void setActiveVMsAttributeValues(List<String> values) throws IOException {
        logger.info("setting active VMs to {}", values);
        storageProvider.setActiveVmAttributeValuesList(values);
        activeVmAttributeValues = new ActiveVmAttributeValues(values);
        List<String> activeVMGroups = activeVmAttributeValues.getValues();
        scheduler.setActiveVmGroups(activeVMGroups);
        lifecycleEventPublisher.publishAuditEvent(
            new LifecycleEventsProto.AuditEvent(LifecycleEventsProto.AuditEvent.AuditEventType.CLUSTER_ACTIVE_VMS,
                "ActiveVMs", String.join(", ", values))
        );
    }

    @Override
    public List<String> getActiveVMsAttributeValues() {
        return activeVmAttributeValues==null?
            null :
            activeVmAttributeValues.values;
    }

    private List<JobsOnVMStatus> getJobsOnVMStatus() {
        List<AgentClusterOperations.JobsOnVMStatus> result = new ArrayList<>();
        final List<VirtualMachineCurrentState> vmCurrentStates = scheduler.getCurrentVMState();
        if (vmCurrentStates != null && !vmCurrentStates.isEmpty()) {
            for (VirtualMachineCurrentState currentState: vmCurrentStates) {
                final VirtualMachineLease currAvailableResources = currentState.getCurrAvailableResources();
                if (currAvailableResources != null) {
                    final Protos.Attribute attribute = currAvailableResources.getAttributeMap().get(attrName);
                    if(attribute!=null) {
                        AgentClusterOperations.JobsOnVMStatus s =
                            new AgentClusterOperations.JobsOnVMStatus(currAvailableResources.hostname(),
                                attribute.getText().getValue());
                        for (TaskRequest r: currentState.getRunningTasks()) {
                            final Optional<WorkerId> workerId = WorkerId.fromId(r.getId());
                            s.addJob(new AgentClusterOperations.JobOnVMInfo(
                                workerId.map(w -> w.getJobId()).orElse("InvalidJobId"),
                                -1,
                                workerId.map(w -> w.getWorkerIndex()).orElse(-1),
                                workerId.map(w -> w.getWorkerNum()).orElse(-1)));
                        }
                        result.add(s);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, List<JobsOnVMStatus>> getJobsOnVMs() {
        listJobsOnVMsCount.increment();
        Map<String, List<JobsOnVMStatus>> result = new HashMap<>();
        final List<JobsOnVMStatus> statusList = getJobsOnVMStatus();
        if (statusList != null && !statusList.isEmpty()) {
            for (JobsOnVMStatus status: statusList) {
                List<JobsOnVMStatus> jobsOnVMStatuses = result.get(status.getAttributeValue());
                if (jobsOnVMStatuses == null) {
                    jobsOnVMStatuses = new ArrayList<>();
                    result.put(status.getAttributeValue(), jobsOnVMStatuses);
                }
                jobsOnVMStatuses.add(status);
            }
        }
        return result;
    }

    private boolean isIn(String name, List<String> activeVMs) {
        for(String vm: activeVMs)
            if(vm.equals(name))
                return true;
        return false;
    }

    @Override
    public boolean isActive(String name) {
        return activeVmAttributeValues==null || activeVmAttributeValues.isEmpty() ||
            isIn(name, activeVmAttributeValues.getValues());
    }

    @Override
    public void setAgentInfos(List<VirtualMachineCurrentState> vmStates) {
        vmStatesMap.put("0", vmStates);
    }

    @Override
    public List<AgentInfo> getAgentInfos() {
        List<VirtualMachineCurrentState> vmStates = vmStatesMap.get("0");
        List<AgentInfo> agentInfos = new ArrayList<>();
        if (vmStates != null && !vmStates.isEmpty()) {
            for (VirtualMachineCurrentState s : vmStates) {
                List<VirtualMachineLease.Range> ranges = s.getCurrAvailableResources().portRanges();
                int ports = 0;
                if (ranges != null && !ranges.isEmpty())
                    for (VirtualMachineLease.Range r : ranges)
                        ports += r.getEnd() - r.getBeg();
                Map<String, Protos.Attribute> attributeMap = s.getCurrAvailableResources().getAttributeMap();
                Map<String, String> attributes = new HashMap<>();
                if (attributeMap != null && !attributeMap.isEmpty()) {
                    for (Map.Entry<String, Protos.Attribute> entry : attributeMap.entrySet()) {
                        attributes.put(entry.getKey(), entry.getValue().getText().getValue());
                    }
                }
                agentInfos.add(new AgentInfo(
                    s.getHostname(), s.getCurrAvailableResources().cpuCores(),
                    s.getCurrAvailableResources().memoryMB(), s.getCurrAvailableResources().diskMB(),
                    ports, s.getCurrAvailableResources().getScalarValues(), attributes, s.getResourceSets().keySet(),
                    getTimeString(s.getDisabledUntil())
                ));
            }
        }
        return agentInfos;
    }

    @Override
    public Map<String, AgentClusterAutoScaleRule> getAgentClusterAutoScaleRules() {
        final Set<AutoScaleRule> agentAutoscaleRules = agentClustersAutoScaler.getRules();
        final Map<String, AgentClusterAutoScaleRule> result = new HashMap<>();
        if (agentAutoscaleRules != null && !agentAutoscaleRules.isEmpty()) {
            for (AutoScaleRule r: agentAutoscaleRules) {
                result.put(r.getRuleName(),
                    new AgentClusterOperations.AgentClusterAutoScaleRule(
                        r.getRuleName(),
                        r.getCoolDownSecs(),
                        r.getMinIdleHostsToKeep(),
                        r.getMaxIdleHostsToKeep(),
                        r.getMinSize(),
                        r.getMaxSize()));
            }
        }
        return result;
    }

    private String getTimeString(long disabledUntil) {
        if (System.currentTimeMillis() > disabledUntil)
            return null;
        return DateTimeExt.toUtcDateTimeString(disabledUntil);
    }

    List<String> manageActiveVMs(final List<VirtualMachineCurrentState> currentStates) {
        List<String> inactiveVMs = new ArrayList<>();
        if(currentStates!=null && !currentStates.isEmpty()) {
            final List<String> values = getActiveVMsAttributeValues();
            if(values==null || values.isEmpty())
                return Collections.EMPTY_LIST; // treat no valid active VMs attribute value as all are active
            for(VirtualMachineCurrentState currentState: currentStates) {
                final VirtualMachineLease lease = currentState.getCurrAvailableResources();
                //logger.info("Lease for VM: " + currentState.getCurrAvailableResources());
                if(lease != null) {
                    final Collection<TaskRequest> runningTasks = currentState.getRunningTasks();
                    if(runningTasks!=null && !runningTasks.isEmpty()) {
                        final Map<String,Protos.Attribute> attributeMap = lease.getAttributeMap();
                        if(attributeMap!=null && !attributeMap.isEmpty()) {
                            final Protos.Attribute attribute = attributeMap.get(attrName);
                            if(attribute!=null && attribute.hasText()) {
                                if(!isIn(attribute.getText().getValue(), values)) {
                                    inactiveVMs.add(lease.hostname());
                                    for(TaskRequest t: runningTasks) {
                                        Optional<WorkerId> workerIdO = WorkerId.fromId(t.getId());
                                        workerIdO.ifPresent(workerId -> jobMessageRouter.routeWorkerEvent(new WorkerOnDisabledVM(workerId)));
                                    }
                                }
                            }
                            else
                                logger.warn("No attribute value for " + attrName + " found on VM " + lease.hostname() +
                                    " that has " + runningTasks.size() + " tasks on it");
                        }
                        else
                            logger.warn("No attributes found on VM " + lease.hostname() + " that has " + runningTasks.size() + " tasks on it");
                    }

                }
            }
        }
        return inactiveVMs;
    }

    private void checkInactiveVMs(List<VirtualMachineCurrentState> vmCurrentStates) {
        logger.debug("Checking on any workers on VMs that are not active anymore");
        final List<String> inactiveVMs = manageActiveVMs(vmCurrentStates);
        if (inactiveVMs!=null && !inactiveVMs.isEmpty()) {
            for(String vm: inactiveVMs) {
                logger.info("expiring all leases of inactive vm " + vm);
                scheduler.rescindOffers(vm);
            }
        }
    }
}
