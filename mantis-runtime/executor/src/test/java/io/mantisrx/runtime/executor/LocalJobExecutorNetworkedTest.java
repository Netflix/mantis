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

package io.mantisrx.runtime.executor;


import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.Iterator;

import io.mantisrx.runtime.api.Job;
import io.mantisrx.runtime.api.MantisJob;
import io.mantisrx.runtime.api.ScalarToScalar;
import io.mantisrx.runtime.common.codec.Codecs;
import io.mantisrx.runtime.common.descriptor.SchedulingInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;


public class LocalJobExecutorNetworkedTest {

    private static final Logger logger = LoggerFactory.getLogger(LocalJobExecutorNetworkedTest.class);

    public static void main(String[] args) {
    }

    private Job<Integer> getJobInstance() {
        return MantisJob
                .source((context, index) -> Flux.just(Flux.interval(Duration.ofSeconds(1))
                    .take(4)
                    .map(Long::intValue)))
                .stage((context, t1) -> Flux.from(t1).map(t11 -> t11 * t11),
                    new ScalarToScalar.Config<Integer, Integer>()
                        .codec(Codecs.integer()))
                .sink((context, portRequest, publisher) -> {
                    StepVerifier
                        .create(Flux.from(publisher))
                        .expectNext(0)
                        .expectNext(1)
                        .expectNext(4)
                        .expectNext(9)
                        .verifyComplete();
                })
                .create();
    }

    @Test
    public void testSingleStageVerifySinkEvents() {
        LocalJobExecutorNetworked.execute(getJobInstance());
    }

    @Test
    public void testSingleStage() {

        TestJobSingleStage provider = new TestJobSingleStage();
        LocalJobExecutorNetworked.execute(provider.getJobInstance());

        Iterator<Integer> iter = provider.getItemsWritten().iterator();

        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testSingleStageMultiWorker() {

        TestJobSingleStage provider = new TestJobSingleStage();
        SchedulingInfo scheduling = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerStage(2, MachineDefinitions.micro())
            .build();

        LocalJobExecutorNetworked.execute(provider.getJobInstance(), scheduling);

        Iterator<Integer> iter = provider.getItemsWritten().iterator();
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(20, count);
    }

}
