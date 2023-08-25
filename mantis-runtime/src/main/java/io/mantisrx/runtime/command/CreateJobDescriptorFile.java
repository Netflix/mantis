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

package io.mantisrx.runtime.command;

import io.mantisrx.common.SystemParameters;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.StageConfig;
import io.mantisrx.runtime.descriptor.JobDescriptor;
import io.mantisrx.runtime.descriptor.JobInfo;
import io.mantisrx.runtime.descriptor.MetadataInfo;
import io.mantisrx.runtime.descriptor.ParameterInfo;
import io.mantisrx.runtime.descriptor.StageInfo;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;


public class CreateJobDescriptorFile implements Command {

    private final static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @SuppressWarnings("rawtypes")
    private final Job job;
    private final String project;
    private final String version;
    private final File descriptorFile;
    private final boolean readyForJobMaster;

    @SuppressWarnings("rawtypes")
    public CreateJobDescriptorFile(final Job job,
                                   final File descriptorFile,
                                   final String version,
                                   final String project) {
        this(job, descriptorFile, version, project, false);
    }

    @SuppressWarnings("rawtypes")
    public CreateJobDescriptorFile(final Job job,
                                   final File descriptorFile,
                                   final String version,
                                   final String project,
                                   final boolean readyForJobMaster) {
        this.job = job;
        this.descriptorFile = descriptorFile;
        this.version = version;
        this.project = project;
        this.readyForJobMaster = readyForJobMaster;
    }

    private MetadataInfo toMetaDataInfo(Metadata metadata) {
        MetadataInfo metadataInfo = null;
        if (metadata != null) {
            metadataInfo = new MetadataInfo(metadata.getName(),
                    metadata.getDescription());
        }
        return metadataInfo;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() throws CommandException {

        Metadata jobMetadata = job.getMetadata();

        // create stage info
        Map<Integer, StageInfo> stagesInfo = new HashMap<>();
        List<StageConfig<?, ?>> stages = job.getStages();
        int numStages = 0;
        for (StageConfig<?, ?> stage : stages) {
            stagesInfo.put(numStages,
                    new StageInfo(numStages, stage.getDescription()));
            numStages++;
        }

        // create parameter info
        final Map<String, ParameterInfo> parameterInfo = createParameterInfo(job.getParameterDefinitions());
        final Map<String, ParameterInfo> systemParameterInfo = createParameterInfo(ParameterUtils.getSystemParameters());
        final int totalNumStages = numStages;
        final Map<String, ParameterInfo> sysParams = systemParameterInfo.entrySet().stream().filter(sysParam -> {
            for (int stageNum = totalNumStages + 1; stageNum <= SystemParameters.MAX_NUM_STAGES_FOR_JVM_OPTS_OVERRIDE; stageNum++) {
                if (sysParam.getKey().equals(String.format(SystemParameters.PER_STAGE_JVM_OPTS_FORMAT, stageNum))) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        parameterInfo.putAll(sysParams);
        // create source/sink info
        Metadata sourceMetadata = job.getSource().getMetadata();
        MetadataInfo sourceMetadataInfo = toMetaDataInfo(sourceMetadata);

        Metadata sinkMetadata = job.getSink().getMetadata();
        MetadataInfo sinkMetadataInfo = toMetaDataInfo(sinkMetadata);

        JobInfo jobInfo = new JobInfo(jobMetadata.getName(),
                jobMetadata.getDescription(),
                numStages, parameterInfo, sourceMetadataInfo, sinkMetadataInfo,
                stagesInfo);

        JobDescriptor jobDescriptor = new JobDescriptor(jobInfo,
                project, version, System.currentTimeMillis(), readyForJobMaster);

        try {
            mapper.writeValue(descriptorFile, jobDescriptor);
        } catch (IOException e) {
            throw new DescriptorException(e);
        }
    }

    private Map<String, ParameterInfo> createParameterInfo(final Map<String, ParameterDefinition<?>> parameters) {
        final Map<String, ParameterInfo> parameterInfo = new HashMap<>();

        for (Entry<String, ParameterDefinition<?>> entry : parameters.entrySet()) {
            ParameterDefinition<?> definition = entry.getValue();
            String defaultValue = null;
            if (definition.getDefaultValue() != null) {
                defaultValue = definition.getDefaultValue().toString();
            }
            parameterInfo.put(entry.getKey(), new ParameterInfo(definition.getName(),
                    definition.getDescription(), defaultValue,
                    definition.getTypeDescription(), definition.getValidator()
                    .getDescription(), definition.isRequired()));
        }
        return parameterInfo;
    }

}
