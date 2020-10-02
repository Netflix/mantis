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

package io.mantisrx.server.core.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;


// Simple stats to figure out outlier threshold, specifically for low number of data points.
// This is meant for finding out outliers greater than most of the data, not outliers that are smaller.
// We make special cases as needed.
public class SimpleStats {

    private final int maxDataPoints;
    private final ArrayList<Double> dataPoints;

    public SimpleStats(int maxDataPoints) {
        this.maxDataPoints = maxDataPoints;
        dataPoints = new ArrayList<>();
    }

    public SimpleStats(Collection<Double> data) {
        this.maxDataPoints = data.size();
        dataPoints = new ArrayList<>(data);
    }

    public static void main(String[] args) {
        SimpleStats simpleStats = new SimpleStats(5);
        simpleStats.add(4.0);
        for (int i = 1; i < 4; i++)
            simpleStats.add(0.0);
        simpleStats.add(10.0);
        System.out.println(String.format("thresh=%8.2f", simpleStats.getOutlierThreshold()));
    }

    public void add(double d) {
        if (dataPoints.size() == maxDataPoints)
            dataPoints.remove(0);
        dataPoints.add(d);
    }

    public double getOutlierThreshold() {
        if (dataPoints.size() <= 2)
            return twoPointsResults();
        Double[] data = dataPoints.toArray(new Double[0]);
        Arrays.sort(data);
        // special case when the highest item is the major contributor of the total
        double total = 0.0;
        for (double d : data)
            total += d;
        if (data[data.length - 1] / total > 0.75)
            return data[data.length - 2];
        if (dataPoints.size() == 3)
            return threePointsResults(data);
        if (dataPoints.size() == 4)
            return fourPointsResults(data);
        double q1 = data[(int) Math.round((double) data.length / 4.0)];
        double q3 = data[(int) Math.floor((double) data.length * 3.0 / 4.0)];
        return getThresh(q1, q3);
    }

    private double fourPointsResults(Double[] data) {
        return getThresh(data[1], data[2]);
    }

    private double getThresh(double q1, double q3) {
        return q3 + q3 - q1;
    }

    private double threePointsResults(Double[] data) {
        double q1 = (data[0] + data[1]) / 2.0;
        double q3 = (data[1] + data[2]) / 2.0;
        return getThresh(q1, q3);
    }

    private double twoPointsResults() {
        return dataPoints.isEmpty() ?
                0.0 :
                dataPoints.get(0) == 0.0 ?
                        0.0 :
                        dataPoints.get(dataPoints.size() - 1);
    }

    public boolean isSufficientData() {
        return dataPoints.size() > 3;
    }

    @Override
    public String toString() {
        return "SimpleStats{" +
                "maxDataPoints=" + maxDataPoints +
                ", dataPoints=" + dataPoints +
                '}';
    }
}
