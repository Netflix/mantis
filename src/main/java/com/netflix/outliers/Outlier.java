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

public class Outlier {
    public enum State {
        INLIER, OUTLIER
    }

    public static class Observation<K> {
        public final K key;
        public final Double value;

        public Observation(K key, Double value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class Result<K> {
        public final K key;
        public final State state;

        public Result(K key, State state) {
            this.key = key;
            this.state = state;
        }
    }

    /**
     * Provides an outlier detector based on Interquartile Range.
     * @param factor Thresholding factor {@see com.netflix.outliers.InterquartileRange}
     * @param minObservations The minimum number of observations required for inference.
     * @param <K> The key type for outlier detection.
     * @return A transformer implementing the desired outlier detection method.
     */
    public static <K> Observable.Transformer<Observation<K>, Result<K>> iqr(double factor, int minObservations) {
        return new InterquartileRange<>(factor, minObservations);
    }
}
