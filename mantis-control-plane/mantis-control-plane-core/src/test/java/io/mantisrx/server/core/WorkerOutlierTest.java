package io.mantisrx.server.core;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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
