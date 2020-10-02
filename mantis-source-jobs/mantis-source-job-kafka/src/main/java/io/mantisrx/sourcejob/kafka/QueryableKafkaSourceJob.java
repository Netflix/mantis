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

package io.mantisrx.sourcejob.kafka;

import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.source.KafkaSource;
import io.mantisrx.connector.kafka.source.serde.ParserType;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.sourcejob.kafka.core.TaggedData;
import io.mantisrx.sourcejob.kafka.sink.QueryRequestPostProcessor;
import io.mantisrx.sourcejob.kafka.sink.QueryRequestPreProcessor;
import io.mantisrx.sourcejob.kafka.sink.TaggedDataSourceSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * Generic queryable source job to connect to configured kafka topic
 */
public class QueryableKafkaSourceJob extends MantisJobProvider<TaggedData> {

    protected AutoAckTaggingStage getAckableTaggingStage() {
        return new CustomizedAutoAckTaggingStage();
    }

    @Override
    public Job<TaggedData> getJobInstance() {
        KafkaSource kafkaSource = new KafkaSource(new DefaultRegistry());
        return
            MantisJob // kafkaSource to connect to kafka and stream events from the configured topic
                .source(kafkaSource)
                .stage(getAckableTaggingStage(), CustomizedAutoAckTaggingStage.config())
                .sink(new TaggedDataSourceSink(new QueryRequestPreProcessor(), new QueryRequestPostProcessor()))
                // required parameters
                .create();
    }

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new QueryableKafkaSourceJob().getJobInstance(),
                                          new Parameter(KafkaSourceParameters.TOPIC, "nf_errors_log"),
                                          new Parameter(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER, "1"),
                                          new Parameter(KafkaSourceParameters.PARSER_TYPE, ParserType.SIMPLE_JSON.getPropName()),
                                          new Parameter(KafkaSourceParameters.PARSE_MSG_IN_SOURCE, "true"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "QueryableKafkaSourceLocal"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.66.49.176:7102"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
    }


}
