/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.mantis.examples.sinefunction;

import static io.mantisrx.mantis.examples.sinefunction.SineFunctionJob.INTERVAL_SEC;
import static io.mantisrx.mantis.examples.sinefunction.SineFunctionJob.RANDOM_RATE;
import static io.mantisrx.mantis.examples.sinefunction.SineFunctionJob.RANGE_MAX;
import static io.mantisrx.mantis.examples.sinefunction.SineFunctionJob.RANGE_MIN;
import static io.mantisrx.mantis.examples.sinefunction.SineFunctionJob.USE_RANDOM_FLAG;

import io.mantisrx.mantis.examples.sinefunction.core.Point;
import io.mantisrx.runtime.core.MantisStream;
import io.mantisrx.runtime.core.MantisStreamImpl;
import io.mantisrx.runtime.core.functions.ReduceFunctionImpl;
import io.mantisrx.runtime.core.sinks.ObservableSinkImpl;
import io.mantisrx.runtime.core.sources.ObservableSourceImpl;
import io.mantisrx.runtime.parameter.type.BooleanParameter;
import io.mantisrx.runtime.parameter.type.DoubleParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SineFunction {
    public static void main(String[] args) {
        final double amplitude = 5.0;
        final double frequency = 1;
        final double phase = 0.0;

        MantisStreamImpl.init()
            .source(new ObservableSourceImpl<>(new SineFunctionJob.TimerSource()))
            .filter(x -> x % 2 == 0)
            .map(x -> new Point(x, amplitude * Math.sin((frequency * x) + phase)))
            .keyBy(x -> x.getX() % 10)
            .window(MantisStream.WindowSpec.count(2))
            .reduce((ReduceFunctionImpl<Point>) (acc, i) -> {
                Point point = new Point(acc.getX() + i.getX(), i.getY());
                log.info("received point ({}, {}) -> ({}, {})", i.getX(), i.getY(), point.getX(), point.getY());
                return point;})
            .sink(new ObservableSinkImpl<>(SineFunctionJob.sseSink))
            .parameters(
            new BooleanParameter()
                .name(USE_RANDOM_FLAG)
                .defaultValue(false)
                .description("If true, produce a random sequence of integers.  If false,"
                    + " produce a sequence of integers starting at 0 and increasing by 1.")
                .build()
            , new DoubleParameter()
                .name(RANDOM_RATE)
                .defaultValue(1.0)
                .description("The chance a random integer is generated, for the given period")
                .validator(Validators.range(0, 1))
                .build()
            , new IntParameter()
                .name(INTERVAL_SEC)
                .defaultValue(1)
                .description("Period at which to generate a random integer value to send to sine function")
                .validator(Validators.range(1, 60))
                .build()
            , new IntParameter()
                .name(RANGE_MIN)
                .defaultValue(0)
                .description("Minimun of random integer value")
                .validator(Validators.range(0, 100))
                .build()
            , new IntParameter()
                .name(RANGE_MAX)
                .defaultValue(100)
                .description("Maximum of random integer value")
                .validator(Validators.range(1, 100))
                .build()

            )
            .execute();
    }
}
