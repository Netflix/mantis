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

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import rx.Observable;

public class InterquartileRange<K> implements Observable.Transformer<Outlier.Observation<K>, Outlier.Result<K>> {

    private final UpdateDoublesSketch sketch = DoublesSketch.builder().setK(512).build();
    private final double[] quantiles = {0.25, 0.75};

    private final double factor;
    private final int minObservations;

    public InterquartileRange() {
        this(1.5, 1000);
    }

    /**
     * Detects outliers using the interquartile range (IQR) method. IQR is defined as 75th percentile - 25th percentile.
     * Outlier detection is then performed using 75th percentile + (IQR * factor) as the threshold.
     *
     * @see {<a href="https://en.wikipedia.org/wiki/Interquartile_range">IQR</a>}
     * @param factor Controls the sensitivity of outlier detection. Decreasing factor increases sensitivity. Default 1.5
     * @param minObservations The minimum number of observations before inference begins.
     */
    public InterquartileRange(double factor, int minObservations) {
        this.factor = factor;
        this.minObservations = minObservations;
    }

    @Override
    public Observable<Outlier.Result<K>> call(Observable<Outlier.Observation<K>> observationObservable) {
        return observationObservable
                .doOnNext(x -> sketch.update(x.value))
                .filter(__ -> sketch.getN() > minObservations)
                .map(x -> {

                    double[] percentiles = sketch.getQuantiles(quantiles);
                    double iqr = percentiles[1] - percentiles[0];
                    double outlierThreshold = percentiles[1] + (iqr * factor);

                    return new Outlier.Result<>(x.key, x.value > outlierThreshold ?
                            Outlier.State.OUTLIER : Outlier.State.INLIER);
                });
    }
}

