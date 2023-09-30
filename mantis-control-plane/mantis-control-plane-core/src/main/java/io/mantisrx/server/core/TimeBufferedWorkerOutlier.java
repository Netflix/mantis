/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.server.core;

import java.util.HashMap;
import java.util.Map;
import rx.functions.Action1;

/**
 * Worker outlier detector that buffers events based on time before determining outliers. This is used for high volume
 * metrics such as sourcejob drops. Volume may have high variation over time, buffering by time will eliminate the variant.
 */
public class TimeBufferedWorkerOutlier extends WorkerOutlier {
    private final Map<Integer, CumulatedValue> workerValues = new HashMap<>();
    private final long bufferedSecs;

    public TimeBufferedWorkerOutlier(long cooldownSecs, long bufferedSecs, Action1<Integer> outlierTrigger) {
        super(cooldownSecs, outlierTrigger);
        this.bufferedSecs = bufferedSecs;
    }

    @Override
    public void addDataPoint(int workerIndex, double value, int numWorkers) {
        CumulatedValue cumulatedValue;
        synchronized (workerValues) {
            cumulatedValue = workerValues.get(workerIndex);
            if (cumulatedValue == null) {
                cumulatedValue = new CumulatedValue();
                workerValues.put(workerIndex, cumulatedValue);
            }
        }

        double dataPoint = -1;
        synchronized (cumulatedValue) {
            if (System.currentTimeMillis() - cumulatedValue.startTs > bufferedSecs * 1000) {
                dataPoint = cumulatedValue.value;
                cumulatedValue.reset();
            }
            cumulatedValue.increment(value);
        }

        if (dataPoint != -1) {
            super.addDataPoint(workerIndex, dataPoint, numWorkers);
        }
    }

    public static class CumulatedValue {
        private long startTs = System.currentTimeMillis();
        private double value = 0;

        public void increment(double incr) {
            value += incr;
        }

        public void reset() {
            startTs = System.currentTimeMillis();
            value = 0;
        }
    }
}
