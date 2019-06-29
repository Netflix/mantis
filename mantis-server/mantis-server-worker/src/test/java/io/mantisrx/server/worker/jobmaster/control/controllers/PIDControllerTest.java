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

package io.mantisrx.server.worker.jobmaster.control.controllers;

import static org.junit.Assert.assertEquals;

import io.mantisrx.server.worker.jobmaster.control.Controller;
import org.junit.Test;
import rx.Observable;


public class PIDControllerTest {

    @Test
    public void shouldControlCacheSizeToMaintainDesiredHitRate() {

        // Arrange
        Cache cache = new Cache();
        final double setPoint = 0.75;
        Controller controller = PIDController.of(25.0, 1.0, 0.0);
        double currentSize = 0.0;

        // Act
        for (int step = 0; step <= 100; ++step) {
            double hitRate = cache.process(currentSize);
            double error = setPoint - hitRate;
            double controlAction = Observable.just(error).lift(controller).toBlocking().first();
            currentSize += controlAction;
        }

        // Assert
        assertEquals(0.75, cache.process(currentSize), 0.01); // Hitrate is within 1% of target.
        assertEquals(0.75 * 250, currentSize, 5.0); // Cache size is within 5 of target size.
    }

    private class Cache {

        private double totalNumberOfItems = 250.0;

        public double process(double size) {
            if (size <= 0.0) {
                return 0.0;
            }
            if (size > totalNumberOfItems) {
                return 100.0;
            }

            return size / totalNumberOfItems;
        }
    }
}
