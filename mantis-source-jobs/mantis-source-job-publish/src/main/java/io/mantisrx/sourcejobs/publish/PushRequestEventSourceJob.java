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

package io.mantisrx.sourcejobs.publish;

import com.mantisrx.common.utils.MantisSourceJobConstants;
import io.mantisrx.connector.publish.core.QueryRegistry;
import io.mantisrx.connector.publish.source.http.PushHttpSource;
import io.mantisrx.connector.publish.source.http.SourceSink;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.sourcejobs.publish.core.RequestPostProcessor;
import io.mantisrx.sourcejobs.publish.core.RequestPreProcessor;
import io.mantisrx.sourcejobs.publish.core.Utils;
import io.mantisrx.sourcejobs.publish.stages.EchoStage;


public class PushRequestEventSourceJob extends MantisJobProvider<String> {

    private static final String MANTIS_CLIENT_ID = "MantisPushRequestEvents";

    @Override
    public Job<String> getJobInstance() {
        String jobId = Utils.getEnvVariable("JOB_ID", "PushRequestEventSourceJobLocal-1");
        String mantisClientId = MANTIS_CLIENT_ID + "_" + jobId;

        QueryRegistry queryRegistry = new QueryRegistry.Builder()
                .withClientIdPrefix(mantisClientId)
                .build();

        return
                MantisJob
                        .source(new PushHttpSource(queryRegistry))
                        .stage(new EchoStage(), EchoStage.config())
                        .sink(new SourceSink(
                                new RequestPreProcessor(queryRegistry),
                                new RequestPostProcessor(queryRegistry),
                                mantisClientId))
                        .parameterDefinition(new IntParameter()
                                .name(MantisSourceJobConstants.ECHO_STAGE_BUFFER_MILLIS)
                                .description("millis to buffer events before processing")
                                .validator(Validators.range(100, 1000))
                                .defaultValue(250)
                                .build())
                        .metadata(new Metadata.Builder()
                                .name("PushRequestEventSourceJob")
                                .description("Fetches request events from any source in a distributed manner. "
                                        + "The output is served via HTTP server using SSE protocol.")
                                .build())
                        .create();
    }

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new PushRequestEventSourceJob().getJobInstance());
    }
}
