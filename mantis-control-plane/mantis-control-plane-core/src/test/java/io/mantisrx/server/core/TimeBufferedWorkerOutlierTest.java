package io.mantisrx.server.core;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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
