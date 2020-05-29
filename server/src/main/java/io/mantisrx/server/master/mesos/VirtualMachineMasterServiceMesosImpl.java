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

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.netflix.fenzo.VirtualMachineLease;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ExecuteStageRequest;
import io.mantisrx.server.core.WorkerTopologyInfo;
import io.mantisrx.server.core.domain.JobMetadata;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.LaunchTaskException;
import io.mantisrx.server.master.VirtualMachineMasterService;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.server.master.scheduler.LaunchTaskRequest;
import io.mantisrx.server.master.scheduler.ScheduleRequest;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action0;


public class VirtualMachineMasterServiceMesosImpl extends BaseService implements VirtualMachineMasterService {

    private static final Logger logger = LoggerFactory.getLogger(VirtualMachineMasterServiceMesosImpl.class);
    private final String masterDescriptionJson;
    private final Supplier<MesosSchedulerDriver> mesosDriver;
    private final AtomicBoolean initializationDone = new AtomicBoolean(false);
    private volatile int workerJvmMemoryScaleBackPct;
    private MasterConfiguration masterConfig;
    private ExecutorService executor;
    private ObjectMapper mapper = new ObjectMapper();

    public VirtualMachineMasterServiceMesosImpl(
            final MasterConfiguration masterConfig,
            final String masterDescriptionJson,
            final Supplier<MesosSchedulerDriver> mesosSchedulerDriverSupplier) {
        super(true);
        this.masterConfig = masterConfig;
        this.masterDescriptionJson = masterDescriptionJson;
        this.mesosDriver = mesosSchedulerDriverSupplier;
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "vm_master_mesos_scheduler_thread");
                t.setDaemon(true);
                return t;
            }
        });
        workerJvmMemoryScaleBackPct = Math.min(99, ConfigurationProvider.getConfig().getWorkerJvmMemoryScaleBackPercentage());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // NOTE: All leases are for the same agent.
    @Override
    public Map<ScheduleRequest, LaunchTaskException> launchTasks(List<LaunchTaskRequest> requests, List<VirtualMachineLease> leases) {
        if (!super.getIsInited()) {
            logger.error("Not in leader mode, not launching tasks");
            return new HashMap<>();
        }
        Protos.SlaveID slaveID = leases.get(0).getOffer().getSlaveId();
        List<Protos.OfferID> offerIDs = new ArrayList<>();
        for (VirtualMachineLease vml : leases)
            offerIDs.add(vml.getOffer().getId());
        Map<ScheduleRequest, LaunchTaskException> errorResults = new HashMap<>();
        List<TaskInfo> taskInfos = new ArrayList<>();
        for (LaunchTaskRequest request : requests) {
            try {
                taskInfos.addAll(createTaskInfo(slaveID, request));
            } catch (LaunchTaskException e) {
                errorResults.put(request.getScheduleRequest(), e);
            }
        }
        if (!taskInfos.isEmpty())
            mesosDriver.get().launchTasks(offerIDs, taskInfos);
        else { // reject offers to prevent offer leak, but shouldn't happen
            for (VirtualMachineLease l : leases) {
                mesosDriver.get().declineOffer(l.getOffer().getId());
            }
        }
        return errorResults;
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        if (!super.getIsInited()) {
            logger.error("Not in leader mode, not rejecting lease");
            return;
        }
        VirtualMachineLeaseMesosImpl mesosLease = (VirtualMachineLeaseMesosImpl) lease;
        Offer offer = mesosLease.getOffer();
        mesosDriver.get().declineOffer(offer.getId());
    }

    private Collection<TaskInfo> createTaskInfo(Protos.SlaveID slaveID, final LaunchTaskRequest launchTaskRequest) throws LaunchTaskException {
        final ScheduleRequest scheduleRequest = launchTaskRequest.getScheduleRequest();
        String name = scheduleRequest.getWorkerId().getJobCluster() + " (stage: "
                + scheduleRequest.getStageNum() + " of " + scheduleRequest.getJobMetadata().getTotalStages() + ")";
        TaskID taskId = TaskID.newBuilder()
                .setValue(scheduleRequest.getWorkerId().getId())
                .build();
        MachineDefinition machineDefinition = scheduleRequest.getMachineDefinition();

        // grab ports within range
        List<Integer> ports = launchTaskRequest.getPorts().getAllPorts();

        TaskInfo taskInfo = null;
        try {
            TaskInfo.Builder taskInfoBuilder = TaskInfo.newBuilder();
            ExecuteStageRequest executeStageRequest = new ExecuteStageRequest(
                    scheduleRequest.getWorkerId().getJobCluster(),
                    scheduleRequest.getWorkerId().getJobId(),
                    scheduleRequest.getWorkerId().getWorkerIndex(),
                    scheduleRequest.getWorkerId().getWorkerNum(),
                    scheduleRequest.getJobMetadata().getJobJarUrl(),
                    scheduleRequest.getStageNum(),
                    scheduleRequest.getJobMetadata().getTotalStages(),
                    ports,
                    getTimeoutSecsToReportStart(),
                    launchTaskRequest.getPorts().getMetricsPort(),
                    scheduleRequest.getJobMetadata().getParameters(),
                    scheduleRequest.getJobMetadata().getSchedulingInfo(),
                    scheduleRequest.getDurationType(),
                    scheduleRequest.getJobMetadata().getSubscriptionTimeoutSecs(),
                    scheduleRequest.getJobMetadata().getMinRuntimeSecs() - (System.currentTimeMillis() - scheduleRequest.getJobMetadata().getMinRuntimeSecs()),
                    launchTaskRequest.getPorts()
            );
            taskInfoBuilder
                    .setName(name)
                    .setTaskId(taskId)
                    .setSlaveId(slaveID)
                    .addResources(
                            Resource.newBuilder()
                                    .setName("cpus")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(
                                            Value.Scalar.newBuilder()
                                                    .setValue(machineDefinition.getCpuCores())))
                    .addResources(
                            Resource.newBuilder()
                                    .setName("mem")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(
                                            Value.Scalar.newBuilder()
                                                    .setValue(machineDefinition.getMemoryMB())))
                    .addResources(
                            Resource.newBuilder()
                                    .setName("disk")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(
                                            Value.Scalar.newBuilder()
                                                    .setValue(machineDefinition.getDiskMB())))
                    .addResources(
                            Resource.newBuilder()
                                    .setName("network")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(
                                            Value.Scalar.newBuilder()
                                                    .setValue(machineDefinition.getNetworkMbps())
                                    )
                    )
                    .setExecutor(
                            createMantisWorkerExecutor(executeStageRequest,
                                    launchTaskRequest, machineDefinition.getMemoryMB(), machineDefinition.getCpuCores()))
                    .setData(
                            ByteString.copyFrom(
                                    mapper.writeValueAsBytes(
                                            executeStageRequest)));

            if (!ports.isEmpty()) {
                for (Integer port : ports) {
                    // add ports
                    taskInfoBuilder.addResources(
                            Resource
                                    .newBuilder()
                                    .setName("ports")
                                    .setType(Value.Type.RANGES)
                                    .setRanges(
                                            Value.Ranges
                                                    .newBuilder()
                                                    .addRange(Value.Range.newBuilder()
                                                            .setBegin(port)
                                                            .setEnd(port))));
                }
            }

            taskInfo = taskInfoBuilder.build();
        } catch (JsonProcessingException e) {
            throw new LaunchTaskException("Failed to build a TaskInfo instance: " + e.getMessage(), e);
        }
        List<TaskInfo> tasks = new ArrayList<>(1);
        tasks.add(taskInfo);
        return tasks;
    }

    private int getMemSize(int original) {
        // If job asked for >999MB but <4000MB, subtract out 500 MB for JVM, meta_space, code_cache, etc.
        // leaving rest for the heap, Xmx.
        if (original < 4000)
            return original > 999 ? original - 500 : original;
        // If job asked for >4000, subtract out based on scale back percentage, but at least 500 MB
        return original - Math.max((int) (original * workerJvmMemoryScaleBackPct / 100.0), 500);
    }

    private ExecutorInfo createMantisWorkerExecutor(final ExecuteStageRequest executeStageRequest,
                                                    final LaunchTaskRequest launchTaskRequest,
                                                    final double memoryMB,
                                                    final double cpuCores) {
        final int memSize = getMemSize((int) memoryMB);
        final int numCpu = (int) Math.ceil(cpuCores);

        final WorkerId workerId = launchTaskRequest.getScheduleRequest().getWorkerId();
        String executorName = workerId.getId();
        JobMetadata jobMetadata = launchTaskRequest.getScheduleRequest().getJobMetadata();
        URL jobJarUrl = jobMetadata.getJobJarUrl();
        Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder()
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JOB_URL")
                                .setValue(jobJarUrl.toString()))
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JOB_NAME")
                                .setValue(executorName))
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("WORKER_LIB_DIR")
                                .setValue(getWorkerLibDir()))
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JVM_MEMORY_MB")
                                .setValue("" + (memSize))
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JVM_META_SPACE_MB")
                                .setValue("100")
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JVM_CODE_CACHE_SIZE_MB")
                                .setValue("200")
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JVM_COMP_CLASS_SIZE_MB")
                                .setValue("100")
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("WORKER_INDEX")
                                .setValue("" + (workerId.getWorkerIndex()))
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("WORKER_NUMBER")
                                .setValue("" + (workerId.getWorkerNum()))
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("JOB_ID")
                                .setValue(workerId.getJobId())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("MANTIS_WORKER_DEBUG_PORT")
                                .setValue("" + launchTaskRequest.getPorts().getDebugPort())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("MANTIS_WORKER_CONSOLE_PORT")
                                .setValue("" + launchTaskRequest.getPorts().getConsolePort())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("MANTIS_USER")
                                .setValue("" + jobMetadata.getUser())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("STAGE_NUMBER")
                                .setValue("" + launchTaskRequest.getScheduleRequest().getStageNum())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("NUM_CPU")
                                .setValue("" + numCpu)
                );
        // add worker info
        Map<String, String> envVars = new WorkerTopologyInfo.Writer(executeStageRequest).getEnvVars();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            envBuilder = envBuilder
                    .addVariables(
                            Protos.Environment.Variable.newBuilder()
                                    .setName(entry.getKey())
                                    .setValue(entry.getValue())
                    );
        }
        // add job parameters
        for (Parameter parameter : executeStageRequest.getParameters()) {
            if (parameter.getName() != null && parameter.getValue() != null) {
                envBuilder = envBuilder.addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName(String.format("JOB_PARAM_" + parameter.getName()))
                                .setValue(parameter.getValue())
                );
            }
        }
        // add ZooKeeper properties
        Protos.Environment env = envBuilder
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("mantis.zookeeper.connectString")
                                .setValue(masterConfig.getZkConnectionString())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("mantis.zookeeper.root")
                                .setValue(masterConfig.getZkRoot())
                )
                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("mantis.zookeeper.leader.announcement.path")
                                .setValue(masterConfig.getLeaderAnnouncementPath())
                )

                .addVariables(
                        Protos.Environment.Variable.newBuilder()
                                .setName("MASTER_DESCRIPTION")
                                .setValue(masterDescriptionJson))
                .build();


        return ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(executorName))
                .setCommand(
                        CommandInfo.newBuilder()
                                .setValue(getWorkerExecutorStartupScriptFullPath())
                                .setEnvironment(env))
                .setName(getWorkerExecutorName())
                .setSource(workerId.getJobId())
                .build();
    }

    @Override
    public void killTask(final WorkerId workerId) {
        if (!super.getIsInited()) {
            logger.error("Not in leader mode, not killing task");
            return;
        }
        final String taskIdString = workerId.getId();
        logger.info("Calling mesos to kill " + taskIdString);

        try {
            Protos.Status status = mesosDriver.get().killTask(
                    TaskID.newBuilder()
                            .setValue(taskIdString)
                            .build());
            logger.info("Kill status = " + status);
            switch (status) {
                case DRIVER_ABORTED:
                case DRIVER_STOPPED:
                    logger.error("Unexpected to see Mesos driver status of " + status + " from kill task request. Committing suicide!");
                    System.exit(2);
            }
        } catch (RuntimeException e) {
            // IllegalStateException from no mesosDriver's addVMLeaseAction or NPE from mesosDriver.get() being null.
            logger.error("Unexpected to see Mesos driver not initialized", e);
            System.exit(2);
        }
    }

    @Override
    public void start() {
        super.awaitActiveModeAndStart(new Action0() {
            @Override
            public void call() {
                logger.info("Registering Mantis Framework with Mesos");
                if (!initializationDone.compareAndSet(false, true))
                    throw new IllegalStateException("Duplicate start() call");

                executor.execute(() -> {
                    try {
                        logger.info("invoking the Mesos driver run");
                        mesosDriver.get().run();
                    } catch (Exception e) {
                        logger.error("Failed to register Mantis Framework with Mesos", e);
                        System.exit(2);
                    }
                });
            }
        });
    }

    @Override
    public void shutdown() {
        logger.info("Unregistering Mantis Framework with Mesos");
        mesosDriver.get().stop(true);
        executor.shutdown();
    }

    public String getMesosMasterHostAndPort() {
        return masterConfig.getMasterLocation();
    }

    public String getWorkerInstallDir() {
        return masterConfig.getWorkerInstallDir();
    }

    public String getWorkerLibDir() {
        return Paths.get(getWorkerInstallDir(), "libs").toString();
    }

    private String getWorkerExecutorScript() {
        return masterConfig.getWorkerExecutorScript();
    }

    private boolean getUseSlaveFiltering() {
        return masterConfig.getUseSlaveFiltering();
    }

    private String getSlaveFilterAttributeName() {
        return masterConfig.getSlaveFilterAttributeName();
    }

    public String getWorkerBinDir() {
        return Paths.get(getWorkerInstallDir(), "bin").toString();
    }

    private String getWorkerExecutorStartupScriptFullPath() {
        return Paths.get(getWorkerBinDir(), getWorkerExecutorScript()).toString();
    }

    public String getMantisFrameworkName() {
        return masterConfig.getMantisFrameworkName();
    }

    public String getWorkerExecutorName() {
        return masterConfig.getWorkerExecutorName();
    }

    public long getTimeoutSecsToReportStart() {
        return masterConfig.getTimeoutSecondsToReportStart();
    }

    private double getMesosFailoverTimeoutSecs() {
        return masterConfig.getMesosFailoverTimeOutSecs();
    }

}
