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

package com.netflix.outliers;

import rx.Observable;

import java.util.Random;

public class Examples {


    public static void main(String[] args) {
        Random rand = new Random(123456);

        Observable.range(1, 1000)
                .map(n -> new Outlier.Observation<String>(n.toString(), rand.nextDouble())) // Fake observation data
                .compose(Outlier.iqr(1.5, 1000)) // Outlier detection
                .filter(result -> result.state == Outlier.State.OUTLIER) // Filter for outliers
                .doOnNext(System.out::println); // Log
    }
}
