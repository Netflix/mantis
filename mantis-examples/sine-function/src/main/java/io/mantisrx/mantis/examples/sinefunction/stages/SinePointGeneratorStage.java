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

package io.mantisrx.mantis.examples.sinefunction.stages;

import io.mantisrx.mantis.examples.sinefunction.SineFunctionJob;
import io.mantisrx.mantis.examples.sinefunction.core.Point;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.computation.ScalarComputation;
import rx.Observable;


/**
 * This class implements the ScalarComputation type of Mantis Stage and
 * transforms the value received from the Source into a Point on a sine-function curve
 * based on AMPLITUDE, FREQUENCY and PHASE job parameters.
 */
public class SinePointGeneratorStage implements ScalarComputation<Integer, Point> {

    @Override
    public Observable<Point> call(Context context, Observable<Integer> o) {
        final double amplitude = (double)
                context.getParameters().get(SineFunctionJob.AMPLITUDE);
        final double frequency = (double)
                context.getParameters().get(SineFunctionJob.FREQUENCY);
        final double phase = (double)
                context.getParameters().get(SineFunctionJob.PHASE);
        return
                o
                        .filter(x -> x % 2 == 0)
                        .map(x -> new Point(x, amplitude * Math.sin((frequency * x) + phase)));
    }
}
