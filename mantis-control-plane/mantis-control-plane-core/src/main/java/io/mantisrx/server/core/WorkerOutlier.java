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

package io.mantisrx.server.core;

import io.mantisrx.server.core.stats.SimpleStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.functions.Action1;
import rx.observers.SerializedObserver;
import rx.subjects.PublishSubject;


public class WorkerOutlier {

    private static final Logger logger = LoggerFactory.getLogger(WorkerOutlier.class);
    private final PublishSubject<DataPoint> subject = PublishSubject.create();
    private final Observer<DataPoint> observer = new SerializedObserver<>(subject);
    private final long cooldownSecs;
    private final Action1<Integer> outlierTrigger;
    private long lastTriggeredAt = 0L;
    private long minDataPoints = 16;
    private long maxDataPoints = 20;
    public WorkerOutlier(long cooldownSecs, Action1<Integer> outlierTrigger) {
        this.cooldownSecs = cooldownSecs;
        if (outlierTrigger == null)
            throw new NullPointerException("outlierTrigger is null");
        this.outlierTrigger = outlierTrigger;
        start();
    }

    private void start() {
        logger.info("Starting Worker outlier detector");
        final Map<Integer, Double> values = new HashMap<>();
        final Map<Integer, List<Boolean>> isOutlierMap = new HashMap<>();
        subject
                .doOnNext(new Action1<DataPoint>() {
                    @Override
                    public void call(DataPoint dataPoint) {
                        values.put(dataPoint.index, dataPoint.value);
                        final int currSize = values.size();
                        if (currSize > dataPoint.numWorkers) {
                            for (int i = dataPoint.numWorkers; i < currSize; i++) {
                                values.remove(i);
                                isOutlierMap.remove(i);
                            }
                        }
                        SimpleStats simpleStats = new SimpleStats(values.values());
                        List<Boolean> booleans = isOutlierMap.get(dataPoint.index);
                        if (booleans == null) {
                            booleans = new ArrayList<>();
                            isOutlierMap.put(dataPoint.index, booleans);
                        }
                        if (booleans.size() >= maxDataPoints) // for now hard code to 20 items
                            booleans.remove(0);
                        booleans.add(dataPoint.value > simpleStats.getOutlierThreshold());
                        if ((System.currentTimeMillis() - lastTriggeredAt) > cooldownSecs * 1000) {
                            if (booleans.size() > minDataPoints) {
                                int total = 0;
                                int outlierCnt = 0;
                                for (boolean b : booleans) {
                                    total++;
                                    if (b)
                                        outlierCnt++;
                                }
                                if (outlierCnt > (Math.round((double) total * 0.7))) { // again, hardcode for now
                                    outlierTrigger.call(dataPoint.index);
                                    lastTriggeredAt = System.currentTimeMillis();
                                    booleans.clear();
                                }
                            }
                        }
                    }
                })
                .subscribe();
    }

    public void addDataPoint(int workerIndex, double value, int numWorkers) {
        observer.onNext(new DataPoint(workerIndex, value, numWorkers));
    }

    public void completed() {
        observer.onCompleted();
    }

    private static class DataPoint {

        private final int index;
        private final double value;
        private final int numWorkers;

        private DataPoint(int index, double value, int numWorkers) {
            this.index = index;
            this.value = value;
            this.numWorkers = numWorkers;
        }
    }
}
