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

package io.mantisrx.master.api.akka.payloads;

public class JobClusterPayloads {
    public static final String JOB_CLUSTER_CREATE = "{\"jobDefinition\":{\"name\":\"sine-function\","
        + "\"user\":\"nmahilani\",\"jobJarFileLocation\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
        "\"version\":\"0.2.9 2018-05-29 16:12:56\",\"schedulingInfo\":{\"stages\":{" +
        "\"1\":{\"numberOfInstances\":\"1\",\"machineDefinition\":{\"cpuCores\":\"1\",\"memoryMB\":\"1024\",\"diskMB\":\"1024\",\"networkMbps\":\"128\",\"numPorts\":\"1\"},\"scalable\":true," +
        "\"scalingPolicy\":{\"stage\":1,\"min\":\"1\",\"max\":\"10\",\"increment\":\"2\",\"decrement\":\"1\",\"coolDownSecs\":\"600\"," +
        "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":\"15\",\"scaleUpAbovePct\":\"75\",\"rollingCount\":{\"count\":\"12\",\"of\":\"20\"}}},\"enabled\":true},\"softConstraints\":[],\"hardConstraints\":[]}}}," +
        "\"parameters\":[],\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"},{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"}]," +
        "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"},\"slaMin\":\"0\",\"slaMax\":\"0\",\"deploymentStrategy\":{\"resourceClusterId\":\"mantisagent\"},\"cronSpec\":null,\"cronPolicy\":\"KEEP_EXISTING\",\"isReadyForJobMaster\":true}," +
        "\"owner\":{\"contactEmail\":\"nmahilani@netflix.com\",\"description\":\"\",\"name\":\"Nick Mahilani\",\"repo\":\"\",\"teamName\":\"\"}}";
    public static final String JOB_CLUSTER_CREATE_RC = "{\"jobDefinition\":{\"name\":\"sine-function-rc\","
        + "\"user\":\"nmahilani\",\"jobJarFileLocation\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
        "\"version\":\"0.2.9 2018-05-29 16:12:56\",\"schedulingInfo\":{\"stages\":{" +
        "\"1\":{\"numberOfInstances\":\"1\",\"machineDefinition\":{\"cpuCores\":\"1\",\"memoryMB\":\"1024\",\"diskMB\":\"1024\",\"networkMbps\":\"128\",\"numPorts\":\"1\"},\"scalable\":true," +
        "\"scalingPolicy\":{\"stage\":1,\"min\":\"1\",\"max\":\"10\",\"increment\":\"2\",\"decrement\":\"1\",\"coolDownSecs\":\"600\"," +
        "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":\"15\",\"scaleUpAbovePct\":\"75\",\"rollingCount\":{\"count\":\"12\",\"of\":\"20\"}}},\"enabled\":true},\"softConstraints\":[],\"hardConstraints\":[]}}}," +
        "\"parameters\":[],\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"},{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"}]," +
        "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"},\"slaMin\":\"0\",\"slaMax\":\"0\",\"deploymentStrategy\":{\"resourceClusterId\":\"mantisagent\"},\"cronSpec\":null,\"cronPolicy\":\"KEEP_EXISTING\",\"isReadyForJobMaster\":true}," +
        "\"owner\":{\"contactEmail\":\"nmahilani@netflix.com\",\"description\":\"\",\"name\":\"Nick Mahilani\",\"repo\":\"\",\"teamName\":\"\"}}";

    public static final String JOB_CLUSTER_VALID_UPDATE = "{\"jobDefinition\":{\"name\":\"sine-function\",\"user\":\"nmahilani\",\"jobJarFileLocation\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
                                                    "\"version\":\"0.2.9 2018-05-29 new version\",\"schedulingInfo\":{\"stages\":{" +
                                                    "\"1\":{\"numberOfInstances\":\"1\",\"machineDefinition\":{\"cpuCores\":\"1\",\"memoryMB\":\"1024\",\"diskMB\":\"1024\",\"networkMbps\":\"128\",\"numPorts\":\"1\"},\"scalable\":true," +
                                                    "\"scalingPolicy\":{\"stage\":1,\"min\":\"1\",\"max\":\"10\",\"increment\":\"2\",\"decrement\":\"1\",\"coolDownSecs\":\"600\"," +
                                                    "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":\"15\",\"scaleUpAbovePct\":\"75\",\"rollingCount\":{\"count\":\"12\",\"of\":\"20\"}}},\"enabled\":true},\"softConstraints\":[],\"hardConstraints\":[]}}}," +
                                                    "\"parameters\":[],\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"},{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"},{\"name\":\"_mantis.resourceCluster\",\"value\":\"testcluster\"}]," +
                                                    "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"},\"slaMin\":\"0\",\"slaMax\":\"0\",\"cronSpec\":null,\"cronPolicy\":\"KEEP_EXISTING\",\"isReadyForJobMaster\":true}," +
                                                    "\"owner\":{\"contactEmail\":\"nmahilani@netflix.com\",\"description\":\"\",\"name\":\"Nick Mahilani\",\"repo\":\"\",\"teamName\":\"\"}}";

    public static final String JOB_CLUSTER_INVALID_UPDATE = "{\"jobDefinition\":{\"name\":\"NonExistent\",\"user\":\"nmahilani\",\"jobJarFileLocation\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
                                                          "\"version\":\"0.2.9 2018-05-29 new version\",\"schedulingInfo\":{\"stages\":{" +
                                                          "\"1\":{\"numberOfInstances\":\"1\",\"machineDefinition\":{\"cpuCores\":\"1\",\"memoryMB\":\"1024\",\"diskMB\":\"1024\",\"networkMbps\":\"128\",\"numPorts\":\"1\"},\"scalable\":true," +
                                                          "\"scalingPolicy\":{\"stage\":1,\"min\":\"1\",\"max\":\"10\",\"increment\":\"2\",\"decrement\":\"1\",\"coolDownSecs\":\"600\"," +
                                                          "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":\"15\",\"scaleUpAbovePct\":\"75\",\"rollingCount\":{\"count\":\"12\",\"of\":\"20\"}}},\"enabled\":true},\"softConstraints\":[],\"hardConstraints\":[]}}}," +
                                                          "\"parameters\":[],\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"},{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"},{\"name\":\"_mantis.resourceCluster\",\"value\":\"testcluster\"}]," +
                                                          "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"},\"slaMin\":\"0\",\"slaMax\":\"0\",\"cronSpec\":null,\"cronPolicy\":\"KEEP_EXISTING\",\"isReadyForJobMaster\":true}," +
                                                          "\"owner\":{\"contactEmail\":\"nmahilani@netflix.com\",\"description\":\"\",\"name\":\"Nick Mahilani\",\"repo\":\"\",\"teamName\":\"\"}}";

    public static final String JOB_CLUSTER_DELETE = "{\n" +
        "    \"name\": \"sine-function\",\n" +
        "    \"user\": \"test\"}";

    public static final String JOB_CLUSTER_DISABLE = "{\n" +
        "    \"name\": \"sine-function\",\n" +
        "    \"user\": \"test\"}";

    public static final String JOB_CLUSTER_DISABLE_NONEXISTENT = "{\n" +
                                                     "    \"name\": \"NonExistent\",\n" +
                                                     "    \"user\": \"test\"}";

    public static final String JOB_CLUSTER_ENABLE = "{\n" +
        "    \"name\": \"sine-function\",\n" +
        "    \"user\": \"test\"}";

    public static final String JOB_CLUSTER_ENABLE_NONEXISTENT = "{\n" +
                                                    "    \"name\": \"NonExistent\",\n" +
                                                    "    \"user\": \"test\"}";

    public static final String JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT = "\n" +
        "{\"name\":\"sine-function\",\"version\":\"0.1.39 2018-03-13 09:40:53\",\"url\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.1.39.zip\",\"skipsubmit\":true,\"user\":\"nmahilani\"}";

    public static final String JOB_CLUSTER_QUICK_UPDATE_AND_SKIP_SUBMIT_NON_EXISTENT = "\n" +
                                                                          "{\"name\":\"NonExistent\",\"version\":\"0.1.39 2018-03-13 09:40:53\",\"url\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.1.39.zip\",\"skipsubmit\":true,\"user\":\"nmahilani\"}";

    public static final String JOB_CLUSTER_UPDATE_SLA =
        "{\"user\":\"nmahilani\",\"name\":\"sine-function\",\"min\":\"0\",\"max\":\"1\",\"cronspec\":\"\",\"cronpolicy\":\"KEEP_EXISTING\",\"forceenable\":false}";
    public static final String JOB_CLUSTER_UPDATE_SLA_NONEXISTENT =
            "{\"user\":\"nmahilani\",\"name\":\"NonExistent\",\"min\":\"0\",\"max\":\"1\",\"cronspec\":\"\",\"cronpolicy\":\"KEEP_EXISTING\",\"forceenable\":false}";

    public static final String JOB_CLUSTER_UPDATE_LABELS =
        "{\"name\":\"sine-function\",\"labels\":[{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.dataOrigin\",\"value\":\"none\"},{\"name\":\"_mantis.resourceCluster\",\"value\":\"testrc\"}],\"user\":\"nmahilani\"}";

    public static final String JOB_CLUSTER_UPDATE_LABELS_NONEXISTENT =
            "{\"name\":\"NonExistent\",\"labels\":[{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.dataOrigin\",\"value\":\"none\"}],\"user\":\"nmahilani\"}";

    public static final String MIGRATE_STRATEGY_UPDATE =
        "{\"name\":\"sine-function\",\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":99,\\\"intervalMs\\\":10000}\"},\"user\":\"nmahilani\"}";

    public static final String MIGRATE_STRATEGY_UPDATE_NONEXISTENT =
            "{\"name\":\"NonExistent\",\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":99,\\\"intervalMs\\\":10000}\"},\"user\":\"nmahilani\"}";

    public static final String QUICK_SUBMIT =
        "{\"name\":\"sine-function\",\"user\":\"nmahilani\",\"jobSla\":{\"durationType\":\"Perpetual\",\"runtimeLimitSecs\":\"0\",\"minRuntimeSecs\":\"0\",\"userProvidedType\":\"\"}}";

    public static final String QUICK_SUBMIT_NONEXISTENT =
            "{\"name\":\"NonExistent\",\"user\":\"nmahilani\",\"jobSla\":{\"durationType\":\"Perpetual\",\"runtimeLimitSecs\":\"0\",\"minRuntimeSecs\":\"0\",\"userProvidedType\":\"\"}}";

    public static final String JOB_CLUSTER_SUBMIT = "{\"name\":\"sine-function\",\"user\":\"nmahilani\",\"jobJarFileLocation\":\"\",\"version\":\"0.2.9 2018-05-29 16:12:56\"," +
        "\"jobSla\":{\"durationType\":\"Perpetual\",\"runtimeLimitSecs\":\"0\",\"minRuntimeSecs\":\"0\",\"userProvidedType\":\"\"}," +
        "\"schedulingInfo\":{\"stages\":{\"0\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1,\"memoryMB\":200,\"diskMB\":1024,\"networkMbps\":128,\"numPorts\":\"1\"},\"scalable\":false}," +
        "\"1\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1,\"memoryMB\":200,\"diskMB\":1024,\"networkMbps\":128,\"numPorts\":\"1\"},\"scalable\":true," +
        "\"scalingPolicy\":{\"stage\":1,\"min\":1,\"max\":10,\"increment\":2,\"decrement\":1,\"coolDownSecs\":600," +
        "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":15,\"scaleUpAbovePct\":75,\"rollingCount\":{\"count\":12,\"of\":20}}},\"enabled\":true}," +
        "\"softConstraints\":[\"M4Cluster\"],\"hardConstraints\":[]}}},\"parameters\":[{\"name\":\"useRandom\",\"value\":\"True\"}, {\"name\":\"periodInSeconds\",\"value\":2}],\"isReadyForJobMaster\":true}";

    public static final String JOB_CLUSTER_SUBMIT_NonExistent = "{\"name\":\"NonExistent\",\"user\":\"nmahilani\",\"jobJarFileLocation\":\"\",\"version\":\"0.2.9 2018-05-29 16:12:56\"," +
                                                    "\"jobSla\":{\"durationType\":\"Perpetual\",\"runtimeLimitSecs\":\"0\",\"minRuntimeSecs\":\"0\",\"userProvidedType\":\"\"}," +
                                                    "\"schedulingInfo\":{\"stages\":{\"0\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1,\"memoryMB\":200,\"diskMB\":1024,\"networkMbps\":128,\"numPorts\":\"1\"},\"scalable\":false}," +
                                                    "\"1\":{\"numberOfInstances\":1,\"machineDefinition\":{\"cpuCores\":1,\"memoryMB\":200,\"diskMB\":1024,\"networkMbps\":128,\"numPorts\":\"1\"},\"scalable\":true," +
                                                    "\"scalingPolicy\":{\"stage\":1,\"min\":1,\"max\":10,\"increment\":2,\"decrement\":1,\"coolDownSecs\":600," +
                                                    "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":15,\"scaleUpAbovePct\":75,\"rollingCount\":{\"count\":12,\"of\":20}}},\"enabled\":true}," +
                                                    "\"softConstraints\":[\"M4Cluster\"],\"hardConstraints\":[]}}},\"parameters\":[{\"name\":\"useRandom\",\"value\":\"True\"}, {\"name\":\"periodInSeconds\",\"value\":2}],\"isReadyForJobMaster\":true}";
}
