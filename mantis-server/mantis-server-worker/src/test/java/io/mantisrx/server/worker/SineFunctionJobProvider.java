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
package io.mantisrx.server.worker;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.codec.JacksonCodecs;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.type.BooleanParameter;
import io.mantisrx.runtime.parameter.type.DoubleParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.ServerSentEventsSink;
import io.mantisrx.runtime.sink.predicate.Predicate;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public class SineFunctionJobProvider extends MantisJobProvider<Point> {
    public static final String INTERVAL_MSEC = "intervalMSec";
    public static final String RANGE_MAX = "max";
    public static final String RANGE_MIN = "min";
    public static final String AMPLITUDE = "amplitude";
    public static final String FREQUENCY = "frequency";
    public static final String PHASE = "phase";
    public static final String RANDOM_RATE = "randomRate";
    public static final String USE_RANDOM_FLAG = "useRandom";

    private final SelfDocumentingSink<Point> sseSink = new ServerSentEventsSink.Builder<Point>()
        .withEncoder(point -> String.format("{\"x\": %f, \"y\": %f}", point.getX(), point.getY()))
        .withPredicate(new Predicate<>(
            "filter=even, returns even x parameters; filter=odd, returns odd x parameters.",
            parameters -> {
                Func1<Point, Boolean> filter = point -> {
                    return true;
                };
                if (parameters != null && parameters.containsKey("filter")) {
                    String filterBy = parameters.get("filter").get(0);
                    // create filter function based on parameter value
                    filter = point -> {
                        // filter by evens or odds for x values
                        if ("even".equalsIgnoreCase(filterBy)) {
                            return (point.getX() % 2 == 0);
                        } else if ("odd".equalsIgnoreCase(filterBy)) {
                            return (point.getX() % 2 != 0);
                        }
                        return true; // if not even/odd
                    };
                }
                return filter;
            }
        ))
        .build();

    public Job<Point> getJobInstance() {
        return MantisJob
            // Define the data source for this job.
            .source(new TimerSource())
            // Add stages to transform the event stream received from the Source.
            .stage(new SinePointGeneratorStage(), stageConfig())
            // Define a sink to output the transformed stream over SSE or an external system like Cassandra, etc.
            .sink(sseSink)
            // Add Job parameters that can be passed in by the user when submitting a job.
            .parameterDefinition(new BooleanParameter()
                .name(USE_RANDOM_FLAG)
                .defaultValue(false)
                .description("If true, produce a random sequence of integers.  If false,"
                    + " produce a sequence of integers starting at 0 and increasing by 1.")
                .build())
            .parameterDefinition(new DoubleParameter()
                .name(RANDOM_RATE)
                .defaultValue(1.0)
                .description("The chance a random integer is generated, for the given period")
                .validator(Validators.range(0, 1))
                .build())
            .parameterDefinition(new IntParameter()
                .name(INTERVAL_MSEC)
                .defaultValue(1)
                .description("Period at which to generate a random integer value to send to sine function")
                .validator(Validators.range(1, 60))
                .build())
            .parameterDefinition(new IntParameter()
                .name(RANGE_MIN)
                .defaultValue(0)
                .description("Minimun of random integer value")
                .validator(Validators.range(0, 100))
                .build())
            .parameterDefinition(new IntParameter()
                .name(RANGE_MAX)
                .defaultValue(100)
                .description("Maximum of random integer value")
                .validator(Validators.range(1, 100))
                .build())
            .parameterDefinition(new DoubleParameter()
                .name(AMPLITUDE)
                .defaultValue(10.0)
                .description("Amplitude for sine function")
                .validator(Validators.range(1, 100))
                .build())
            .parameterDefinition(new DoubleParameter()
                .name(FREQUENCY)
                .defaultValue(1.0)
                .description("Frequency for sine function")
                .validator(Validators.range(1, 100))
                .build())
            .parameterDefinition(new DoubleParameter()
                .name(PHASE)
                .defaultValue(0.0)
                .description("Phase for sine function")
                .validator(Validators.range(0, 100))
                .build())
            .metadata(new Metadata.Builder()
                .name("Sine function")
                .description("Produces an infinite stream of points, along the sine function, using the"
                    + " following function definition: f(x) = amplitude * sin(frequency * x + phase)."
                    + " The input to the function is either random between [min, max], or an integer sequence starting "
                    + " at 0.  The output is served via HTTP server using SSE protocol.")
                .build())
            .create();
    }

    /**
     * This source generates a monotonically increasingly value per tick as per INTERVAL_SEC Job parameter.
     * If USE_RANDOM_FLAG is set, the source generates a random value per tick.
     */
    class TimerSource implements Source<Integer> {
        private Subscription totalNumWorkersSubscription;

        @Override
        public Observable<Observable<Integer>> call(Context context, Index index) {
            // If you want to be informed of scaleup/scale down of the source stage of this job you can subscribe
            // to getTotalNumWorkersObservable like the following.
            totalNumWorkersSubscription =
                index
                    .getTotalNumWorkersObservable()
                    .subscribeOn(Schedulers.io()).subscribe((workerCount) -> {
                        System.out.println("Total worker count changed to -> " + workerCount);
                    });
            final int period = (int)
                context.getParameters().get(INTERVAL_MSEC);
            final int max = (int)
                context.getParameters().get(RANGE_MAX);
            final int min = (int)
                context.getParameters().get(RANGE_MIN);
            final double randomRate = (double)
                context.getParameters().get(RANDOM_RATE);
            final boolean useRandom = (boolean)
                context.getParameters().get(USE_RANDOM_FLAG);

            final Random randomNumGenerator = new Random();
            final Random randomRateVariable = new Random();

            return Observable.just(
                Observable.interval(0, period, TimeUnit.MILLISECONDS)
                    .map(time -> {
                        if (useRandom) {
                            return randomNumGenerator.nextInt((max - min) + 1) + min;
                        } else {
                            return (int) (long) time;
                        }
                    })
                    .filter(x -> {
                        double value = randomRateVariable.nextDouble();
                        return (value <= randomRate);
                    })
            );
        }

        @Override
        public void close() throws IOException {
        }
    }

    class SinePointGeneratorStage implements ScalarComputation<Integer, Point> {

        @Override
        public Observable<Point> call(Context context, Observable<Integer> o) {
            final double amplitude = (double)
                context.getParameters().get(AMPLITUDE);
            final double frequency = (double)
                context.getParameters().get(FREQUENCY);
            final double phase = (double)
                context.getParameters().get(PHASE);
            return
                o
                    .filter(x -> x % 2 == 0)
                    .map(x -> new Point(x, amplitude * Math.sin((frequency * x) + phase)));
        }
    }

    ScalarToScalar.Config<Integer, Point> stageConfig() {
        return new ScalarToScalar.Config<Integer, Point>()
            .codec(JacksonCodecs.pojo(Point.class));
    }
}
