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

package com.netflix.mantis.samples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.mantis.samples.stage.EchoStage;

import io.mantisrx.connector.job.core.MantisSourceJobConnector;
import io.mantisrx.connector.job.source.JobSource;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.sink.Sinks;
import lombok.extern.slf4j.Slf4j;


/**
 * This sample demonstrates how to connect to the output of another job using the {@link JobSource}
 * If the target job is a source job then you can request a filtered stream of events from the source job
 * by passing an MQL query.
 * In this example we connect to the latest running instance of SyntheticSourceJob using the query
 * select country from stream where status==500 and simply echo the output.
 *
 * Run this sample by executing the main method of this class. Then look for the SSE port where the output of this job
 * will be available for streaming. E.g  Serving modern HTTP SSE server sink on port: 8299
 * via command line do ../gradlew execute
 *
 * Note: this sample may not work in your IDE as the Mantis runtime needs to discover the location of the
 * SyntheticSourceJob.
 */

@Slf4j
public class JobConnectorJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {

        return MantisJob
                // Stream Events from a job specified via job parameters
                .source(new JobSource())

                // Simple echoes the data
                .stage(new EchoStage(), EchoStage.config())

                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.eagerSubscribe(
                        Sinks.sse((String data) -> data)))

                .metadata(new Metadata.Builder()
                        .name("ConnectToJob")
                        .description("Connects to the output of another job"
                                + " and simply echoes the data")
                        .build())
                .create();

    }

    public static void main(String[] args) throws JsonProcessingException {
        Map<String,Object> targetMap = new HashMap<>();
        List<JobSource.TargetInfo> targetInfos = new ArrayList<>();

        JobSource.TargetInfo targetInfo = new JobSource.TargetInfoBuilder().withClientId("abc")
                .withSourceJobName("SyntheticSourceJob")
                .withQuery("select country from stream where status==500")
                .build();
        targetInfos.add(targetInfo);
        targetMap.put("targets",targetInfos);
        ObjectMapper mapper = new ObjectMapper();
        String target = mapper.writeValueAsString(targetMap);

        // To run locally we use the LocalJobExecutor
        LocalJobExecutorNetworked.execute(new JobConnectorJob().getJobInstance(),
                new Parameter(MantisSourceJobConnector.MANTIS_SOURCEJOB_TARGET_KEY, target));
    }
}