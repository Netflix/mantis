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

import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import java.util.ArrayDeque;


public class UsageDataStats {

    private final int capacity;
    private final ArrayDeque<Double> data;
    private final double highThreshold;
    private final double lowThreshold;
    private double sum = 0.0;
    private int countAboveHighThreshold = 0;
    private int countBelowLowThreshold = 0;
    private StageScalingPolicy.RollingCount rollingCount = new StageScalingPolicy.RollingCount(1, 1);

    public UsageDataStats(double highThreshold, double lowThreshold, StageScalingPolicy.RollingCount rollingCount) {
        this.capacity = rollingCount.getOf();
        data = new ArrayDeque<>(capacity);
        this.highThreshold = highThreshold;
        this.lowThreshold = lowThreshold;
        this.rollingCount = rollingCount;
    }

    public void add(double d) {
        if (data.size() >= capacity) {
            final Double removed = data.removeFirst();
            sum -= removed;
            if (removed > highThreshold)
                countAboveHighThreshold--;
            if (removed < lowThreshold && lowThreshold > 0.0) {
                // disable scaleDown for lowThreshold <= 0
                countBelowLowThreshold--;
            }
        }
        data.addLast(d);
        sum += d;
        if (d > highThreshold)
            countAboveHighThreshold++;
        if (d < lowThreshold && lowThreshold > 0.0) {
            // disable scaleDown for lowThreshold <= 0
            countBelowLowThreshold++;
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public double getAverage() {
        return sum / data.size();
    }

    public int getCountAboveHighThreshold() {
        return countAboveHighThreshold;
    }

    public int getCountBelowLowThreshold() {
        return countBelowLowThreshold;
    }

    public int getSize() {
        return data.size();
    }

    public boolean getHighThreshTriggered() {
        return data.size() >= rollingCount.getCount() && countAboveHighThreshold >= rollingCount.getCount();
    }

    public boolean getLowThreshTriggered() {
        return data.size() >= rollingCount.getCount() && countBelowLowThreshold >= rollingCount.getCount();
    }

    public String getCurrentHighCount() {
        return countAboveHighThreshold + " of " + data.size();
    }

    public String getCurrentLowCount() {
        return countBelowLowThreshold + " of " + data.size();
    }
}
