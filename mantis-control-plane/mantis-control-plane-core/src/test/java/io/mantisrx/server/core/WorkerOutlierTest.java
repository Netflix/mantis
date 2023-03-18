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

public class WorkerOutlierTest {
    @Test
    public void testOutlier() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkerOutlier outlier = new WorkerOutlier(600, index -> {
            if (index == 0) {
                latch.countDown();
            }
        });

        for (int i = 0; i < 16; i++) {
            outlier.addDataPoint(0, 5, 2);
            outlier.addDataPoint(1, 5, 2);
            assertEquals(1, latch.getCount());
        }

        outlier.addDataPoint(0, 5, 2);
        assertFalse(latch.await(1, TimeUnit.SECONDS));

        // Additional data points with higher value. Need to have > 70% of 20 outliers to trigger.
        for (int i = 0; i < 14; i++) {
            outlier.addDataPoint(0, 6, 2);
            assertEquals(1, latch.getCount());
        }

        outlier.addDataPoint(0, 6, 2);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testOutlierMultipleWorkers() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkerOutlier outlier = new WorkerOutlier(600, index -> {
            if (index == 0) {
                latch.countDown();
            }
        });

        for (int i = 0; i < 16; i++) {
            outlier.addDataPoint(0, 6, 4);
            outlier.addDataPoint(1, 6, 4);
            outlier.addDataPoint(2, 5, 4);
            outlier.addDataPoint(3, 5, 4);
            assertEquals(1, latch.getCount());
        }

        outlier.addDataPoint(0, 6, 4);
        assertFalse(latch.await(1, TimeUnit.SECONDS));

        // Additional data points with higher value. Need to have > 70% of 20 outliers to trigger.
        for (int i = 0; i < 14; i++) {
            outlier.addDataPoint(0, 8, 4);
            assertEquals(1, latch.getCount());
        }

        outlier.addDataPoint(0, 8, 4);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }
}
