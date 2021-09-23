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

package io.mantisrx.connector.kafka.sink;

import com.netflix.spectator.api.Registry;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.Parameters;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public class KafkaSink<T> implements SelfDocumentingSink<T> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private final Func1<T, byte[]> encoder;
    private final Registry registry;
    private final AtomicReference<KafkaProducer<byte[], byte[]>> kafkaProducerAtomicRef = new AtomicReference<>(null);

    KafkaSink(Registry registry, Func1<T, byte[]> encoder) {
        this.encoder = encoder;
        this.registry = registry;
    }

    @Override
    public void call(Context context, PortRequest ignore, Observable<T> dataO) {
        if (kafkaProducerAtomicRef.get() == null) {
            MantisKafkaProducerConfig mantisKafkaProducerConfig = new MantisKafkaProducerConfig(context);
            Map<String, Object> producerProperties = mantisKafkaProducerConfig.getProducerProperties();
            KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(producerProperties);
            kafkaProducerAtomicRef.compareAndSet(null, kafkaProducer);
            logger.info("Kafka Producer initialized");
        }
        KafkaProducer<byte[], byte[]> kafkaProducer = kafkaProducerAtomicRef.get();
        Parameters parameters = context.getParameters();
        String topic = (String)parameters.get(KafkaSinkJobParameters.TOPIC);

        dataO.map(encoder::call)
            .flatMap((dataBytes) ->
                         Observable.from(kafkaProducer.send(new ProducerRecord<>(topic, dataBytes)))
                             .subscribeOn(Schedulers.io()))
            .subscribe();
    }

    @Override
    public List<ParameterDefinition<?>> getParameters() {
        final List<ParameterDefinition<?>> params = new ArrayList<>();
        params.add(new StringParameter()
                       .name(KafkaSinkJobParameters.TOPIC)
                       .description("Kafka topic to write to")
                       .validator(Validators.notNullOrEmpty())
                       .required()
                       .build());
        params.addAll(MantisKafkaProducerConfig.getJobParameterDefinitions());
        return params;
    }

    @Override
    public Metadata metadata() {
        StringBuilder description = new StringBuilder();
        description.append("Writes the output of the job into the configured Kafka topic");

        return new Metadata.Builder()
            .name("Mantis Kafka Sink")
            .description(description.toString())
            .build();
    }
}
