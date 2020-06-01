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

package io.mantisrx.connector.iceberg.sink.committer;

import static org.mockito.Mockito.mock;

import java.time.Duration;

import io.mantisrx.connector.iceberg.sink.committer.config.CommitterConfig;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

class IcebergCommitterStageTest {

    private IcebergCommitterStage.Transformer transformer;

    @BeforeEach
    void setUp() {
        CommitterConfig config = new CommitterConfig(new Parameters());
        IcebergCommitter committer = mock(IcebergCommitter.class);
        this.transformer = new IcebergCommitterStage.Transformer(config, committer);
    }

    @AfterEach
    void tearDown() {
        VirtualTimeScheduler.reset();
    }

    @Test
    void shouldCommitPeriodically() {
        StepVerifier
                .withVirtualTime(() -> {
                    Flux<DataFile> upstream = Flux.interval(Duration.ofSeconds(1)).map(i -> mock(DataFile.class));
                    return transformer.transform(upstream);
                })
                .expectSubscription()
                .expectNoEvent(Duration.ofMinutes(1))
                .thenAwait(Duration.ofMinutes(4))
                .expectNextCount(1)
                .thenAwait(Duration.ofMinutes(5))
                .expectNextCount(1)
                .expectNoEvent(Duration.ofMinutes(4))
                .thenCancel()
                .verify();
    }
}