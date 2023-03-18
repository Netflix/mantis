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

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TimeBufferedWorkerOutlierTest {
    @Test
    public void testOutlier() throws Exception {
        long bufferSec = 1;
        CountDownLatch latch = new CountDownLatch(1);
        TimeBufferedWorkerOutlier outlier = new TimeBufferedWorkerOutlier(600, bufferSec, index -> {
            if (index == 0) {
                latch.countDown();
            }
        });

        for (int i = 0; i < 16; i++) {
            // Add multiple data points within the buffer period.
            for (int j = 0; j < 3; j++) {
                outlier.addDataPoint(0, 5, 2);
                outlier.addDataPoint(1, 5, 2);
            }
            assertFalse(latch.await(bufferSec * 1500, TimeUnit.MILLISECONDS));
        }

        outlier.addDataPoint(0, 15, 2);
        assertFalse(latch.await(bufferSec * 1500, TimeUnit.MILLISECONDS));

        // Additional data points with higher value. Need to have > 70% of 20 outliers to trigger.
        for (int i = 0; i < 14; i++) {
            for (int j = 0; j < 3; j++) {
                outlier.addDataPoint(0, 6, 2);
            }
            assertFalse(latch.await(bufferSec * 1500, TimeUnit.MILLISECONDS));
        }

        outlier.addDataPoint(0, 18, 2);
        assertFalse(latch.await(bufferSec * 1500, TimeUnit.MILLISECONDS));
        outlier.addDataPoint(0, 18, 2);
        assertTrue(latch.await(bufferSec * 1500, TimeUnit.MILLISECONDS));
    }
}
