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

package io.mantisrx.master.api.akka.route;

import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
//import com.google.protobuf.util.JsonFormat;
import io.mantisrx.master.api.akka.route.proto.JobClusterProtoAdapter;
//import io.mantisrx.master.api.proto.JobArchivedWorkersResponse;
//import io.mantisrx.master.core.proto.WorkerMetadata;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.fail;

public class JacksonTest {
    @Test
    @Ignore
    public void testDeser() throws IOException {
        final String json = "[{\"jobMetadata\":{\"jobId\":{\"jobNum\":1,\"cluster\":\"sine-function\",\"id\":\"sine-function-1\"},\"submittedAt\":1526923218174,\"jobDefinition\":{\"name\":\"sine-function\",\"user\":\"nmahilani\",\"artifactName\":\"\",\"version\":\"0.1.39 2018-03-13 09:40:53\",\"parameters\":[],\"jobSla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"},\"subscriptionTimeoutSecs\":0,\"schedulingInfo\":{\"stages\":{\"1\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":1024.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1},\"hardConstraints\":[],\"softConstraints\":[],\"scalingPolicy\":null,\"scalable\":true}}},\"numberOfStages\":1,\"labels\":[]},\"state\":\"Launched\",\"nextWorkerNumberToUse\":10,\"startedAt\":-1,\"endedAt\":-1,\"parameters\":[],\"user\":\"nmahilani\",\"labels\":[],\"clusterName\":\"sine-function\",\"submittedAtInstant\":{\"epochSecond\":1526923218,\"nano\":174000000},\"jobJarUrl\":\"http:\",\"startedAtInstant\":null,\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"},\"subscriptionTimeoutSecs\":0,\"minRuntimeSecs\":0,\"endedAtInstant\":null},\"stageMetadataList\":[],\"workerMetadataList\":[]}]";

        List<MantisJobMetadataView> mantisJobMetadataViews = Jackson.fromJSON(json, new TypeReference<List<MantisJobMetadataView>>() {
        });
        System.out.println(mantisJobMetadataViews.toString());

    }

    @Test
    @Ignore
    public void testDeser2() throws IOException {
        List<MantisJobMetadataView> jobIdInfos = Jackson.fromJSON("[{\"jobMetadata\":{\"jobId\":\"sine-function-1\",\"name\":\"sine-function\",\"user\":\"nmahilani\",\"submittedAt\":1527703650220,\"jarUrl\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\",\"numStages\":2,\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"},\"state\":\"Accepted\",\"subscriptionTimeoutSecs\":0,\"parameters\":[{\"name\":\"useRandom\",\"value\":\"True\"}],\"nextWorkerNumberToUse\":11,\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"},\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"},{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"}]},\"stageMetadataList\":[{\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1},\"numWorkers\":1,\"hardConstraints\":null,\"softConstraints\":null,\"scalingPolicy\":null,\"scalable\":false},{\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1},\"numWorkers\":1,\"hardConstraints\":[],\"softConstraints\":[\"M4Cluster\"],\"scalingPolicy\":{\"stage\":1,\"min\":1,\"max\":10,\"increment\":2,\"decrement\":1,\"coolDownSecs\":600,\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":15.0,\"scaleUpAbovePct\":75.0,\"rollingCount\":{\"count\":12,\"of\":20}}},\"enabled\":true},\"scalable\":true}],\"workerMetadataList\":[{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0,\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\",\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650231,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0,\"completedAt\":0,\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0},{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0,\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\",\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650232,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0,\"completedAt\":0,\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0}]}]",
            new TypeReference<List<MantisJobMetadataView>>() {
        });
        System.out.println(jobIdInfos);
    }

    @Test
    @Ignore
    public void testDeser3() throws IOException {
        String json = "{ \"workers\":[{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numberOfPorts\":4,\"" +
            "workerPorts\":{\"metricsPort\":8000,\"debugPort\":9000,\"consolePort\":9010,\"ports\":[]}," +
            "\"state\":\"Started\",\"slave\":\"host1\",\"slaveID\":\"vm1\"," +
            "\"acceptedAt\":1528935765820,\"launchedAt\":1528935765869,\"startingAt\":1528935765872,\"startedAt\":1528935765877,\"completedAt\":-1," +
            "\"reason\":\"Normal\",\"resubmitOf\":0,\"totalResubmitCount\":0,\"isSubscribed\":false,\"cluster\":null," +
            "\"consolePort\":9010,\"metricsPort\":8000,\"ports\":{\"metricsPort\":8000,\"debugPort\":9000,\"consolePort\":9010," +
            "\"ports\":[]},\"debugPort\":9000,\"preferredClusterOptional\":null}]}";
        String json2 = "[{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numberOfPorts\":4," +
            "\"metricsPort\":8000,\"consolePort\":9010,\"debugPort\":9000,\"ports\":[],\"state\":\"Started\",\"slave\":\"host1\"," +
            "\"slaveID\":\"vm1\",\"cluster\":null,\"acceptedAt\":1528936424143,\"launchedAt\":1528936424197," +
            "\"startingAt\":1528936424199,\"startedAt\":1528936424201,\"completedAt\":-1," +
            "\"reason\":\"Normal\",\"resubmitOf\":0,\"totalResubmitCount\":0}]";
//        JobArchivedWorkersResponse resp = null;
//        JobArchivedWorkersResponse.Builder builder = JobArchivedWorkersResponse.newBuilder();
//        Descriptors.FieldDescriptor workersFD = builder.getDescriptorForType().findFieldByName("workers");
//        Message.Builder builder1 = builder.newBuilderForField(workersFD);
////        JsonFormat.parser().usingTypeRegistry()
//        try {
//            JsonFormat.parser().ignoringUnknownFields().merge(json2, builder1);
////                    workers = Jackson.fromJSON(responseMessage, new TypeReference<List<IMantisWorkerMetadata>>() {});
//            resp = builder.build();
//            System.out.println(resp);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testDeser4() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        List<MantisJobMetadataView> jobIdInfos = Jackson.fromJSON(objectMapper,"[{\"jobMetadata\":{\"jobId\":\"sine-function-1\",\"name\":\"sine-function\"," +
                "\"user\":\"nmahilani\",\"submittedAt\":1527703650220,\"jarUrl\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
                "\"numStages\":2,\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"}," +
                "\"state\":\"Accepted\",\"subscriptionTimeoutSecs\":0,\"parameters\":[{\"name\":\"useRandom\",\"value\":\"True\"}],\"nextWorkerNumberToUse\":11," +
                "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"}," +
                "\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"}," +
                "{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"}]}," +
                "\"stageMetadataList\":[{\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1}," +
                "\"numWorkers\":1,\"hardConstraints\":null,\"softConstraints\":null,\"scalingPolicy\":null,\"scalable\":false}," +
                "{\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1},\"numWorkers\":1,\"hardConstraints\":[],\"softConstraints\":[\"M4Cluster\"]," +
                "\"scalingPolicy\":{\"stage\":1,\"min\":1,\"max\":10,\"increment\":2,\"decrement\":1,\"coolDownSecs\":600," +
                "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":15.0,\"scaleUpAbovePct\":75.0,\"rollingCount\":{\"count\":12,\"of\":20}}},\"enabled\":true},\"scalable\":true}]," +
                "\"workerMetadataList\":[{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0," +
                "\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\",\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650231,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0," +
                "\"completedAt\":0,\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0},{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0,\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\"," +
                "\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650232,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0,\"completedAt\":0," +
                "\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0}]}]",
            new TypeReference<List<MantisJobMetadataView>>() {
            });
        System.out.println(jobIdInfos);
        MantisWorkerMetadataWritable mwm = jobIdInfos.get(0).getWorkerMetadataList().get(0);
        mwm.setCluster(Optional.ofNullable("test"));

        System.out.println(objectMapper.writer(Jackson.DEFAULT_FILTER_PROVIDER).writeValueAsString(mwm));
    }
}
