/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.writer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;

import io.mantisrx.connector.iceberg.sink.writer.config.WriterConfig;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rx.RxReactiveStreams;

class IcebergWriterStageTest {

    private IcebergWriterStage.Transformer transformer;

    @BeforeEach
    void setUp() throws IOException {
        WriterConfig config = new WriterConfig(new Parameters(), mock(Configuration.class));
        IcebergWriter writer = mock(IcebergWriter.class);
        when(writer.close()).thenReturn(mock(DataFile.class));
        when(writer.length()).thenReturn(Long.MAX_VALUE);
        this.transformer = new IcebergWriterStage.Transformer(config, writer);
    }

    @Test
    void shouldCloseOnRowGroupSizeThreshold() {
        StepVerifier
                .withVirtualTime(() -> {
                    Flux<Record> upstream = Flux.interval(Duration.ofSeconds(1)).map(i -> mock(Record.class));
                    return RxReactiveStreams.toPublisher(RxReactiveStreams.toObservable(upstream).compose(transformer));
                })
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))
                .thenAwait(Duration.ofSeconds(999))
                .expectNextCount(1)
                .thenAwait(Duration.ofSeconds(1000))
                .expectNextCount(1)
                .expectNoEvent(Duration.ofSeconds(1))
                .thenCancel()
                .verify();
    }
}