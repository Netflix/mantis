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

package io.mantisrx.sourcejob.synthetic;

import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.sourcejob.synthetic.core.TaggedData;
import io.mantisrx.sourcejob.synthetic.sink.QueryRequestPostProcessor;
import io.mantisrx.sourcejob.synthetic.sink.QueryRequestPreProcessor;
import io.mantisrx.sourcejob.synthetic.sink.TaggedDataSourceSink;
import io.mantisrx.sourcejob.synthetic.source.SyntheticSource;
import io.mantisrx.sourcejob.synthetic.stage.TaggingStage;

/**
 * A sample queryable source job that generates synthetic request events.
 * Clients connect to this job via the Sink port using an MQL expression. The job then sends only the data
 * that matches the query to the client. The client can be another Mantis Job or a user manually running a GET request.
 *
 * Run this sample by executing the main method of this class. Then look for the SSE port where the output of this job
 * will be available for streaming. E.g  Serving modern HTTP SSE server sink on port: 8299
 * Usage: curl "localhost:<sseport>?clientId=<myId>&subscriptionId=<someid>&criterion=<valid mql query>
 *
 * E.g <code>curl "localhost:8498?subscriptionId=nj&criterion=select%20country%20from%20stream%20where%20status%3D%3D500&clientId=nj2"</code>
 * Here the user is submitted an MQL query select country from stream where status==500.
 */
public class SyntheticSourceJob extends MantisJobProvider<TaggedData> {

    @Override
    public Job<TaggedData> getJobInstance() {

        return
            MantisJob
                 // synthetic source generates random RequestEvents.
                .source(new SyntheticSource())
                 // Tags events with queries that match
                .stage(new TaggingStage(), TaggingStage.config())
                 // A custom sink that processes query parameters to register and deregister MQL queries
                .sink(new TaggedDataSourceSink(new QueryRequestPreProcessor(), new QueryRequestPostProcessor()))
                // required parameters
                .create();
    }

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new SyntheticSourceJob().getJobInstance());
    }
}
