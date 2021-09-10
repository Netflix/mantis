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
package io.reactivex.mantis.network.push;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimedChunkerTest {
    private static Logger logger = LoggerFactory.getLogger(TimedChunkerTest.class);

    private TestProcessor<Integer> processor;
    private MonitoredQueue<Integer> monitoredQueue;

    @BeforeEach
    public void setup() {
        monitoredQueue = new MonitoredQueue<>("test-queue", 100, false);
        processor = new TestProcessor<>(0);

    }

    @Test
    public void testProcessData() throws Exception {
<<<<<<< HEAD
<<<<<<< HEAD
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 100, 500, processor, null);
=======
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 100, 1000, processor, null);
>>>>>>> 1d9ec46... Refactor TimedChunker to fix data drop due to race condition
=======
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 100, 500, processor, null);
>>>>>>> 41790fe... fix metric name

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            expected.add(i);
        }
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Void> chunkerFuture = service.submit(timedChunker);
        // Send 50 events in 100ms interval. Should take 5s total to send.
        Future senderFuture = service.submit(() -> {
            for (int i : expected) {
                logger.info("sending event {}", i);
                monitoredQueue.write(i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    logger.info("sender interrupted", ex);
                }
            }
        });
        Thread.sleep(6000);
        assertTrue(senderFuture.isDone());
        chunkerFuture.cancel(true);
        assertEquals(expected, processor.getProcessed());

    }

    @Test
    public void testBufferLength() throws Exception {
<<<<<<< HEAD
<<<<<<< HEAD
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 5, 500, processor, null);
=======
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 5, 1000, processor, null);
>>>>>>> 1d9ec46... Refactor TimedChunker to fix data drop due to race condition
=======
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 5, 500, processor, null);
>>>>>>> 41790fe... fix metric name

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            expected.add(i);
        }
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Void> chunkerFuture = service.submit(timedChunker);
        // Send 50 events in 100ms interval. Should take 5s total to send.
        Future senderFuture = service.submit(() -> {
            for (int i : expected) {
                logger.info("sending event {}", i);
                monitoredQueue.write(i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    logger.info("sender interrupted", ex);
                }
            }
        });
        Thread.sleep(6000);
        assertTrue(senderFuture.isDone());
        chunkerFuture.cancel(true);
        assertEquals(expected, processor.getProcessed());
    }

    @Test
    public void testMaxTime() throws Exception {
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 100, 200, processor, null);

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            expected.add(i);
        }
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Void> chunkerFuture = service.submit(timedChunker);
        // Send 50 events in 100ms interval. Should take 5s total to send.
        Future senderFuture = service.submit(() -> {
            for (int i : expected) {
                logger.info("sending event {}", i);
                monitoredQueue.write(i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    logger.info("sender interrupted", ex);
                }
            }
        });
        Thread.sleep(6000);
        assertTrue(senderFuture.isDone());
        chunkerFuture.cancel(true);
        assertEquals(expected, processor.getProcessed());
    }

    @Test
    public void testLongProcessing() throws Exception {
        // Processing time take longer than drain interval.
        processor = new TestProcessor<>(400);
        TimedChunker<Integer> timedChunker = new TimedChunker<>(monitoredQueue, 100, 200, processor, null);

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            expected.add(i);
        }
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Void> chunkerFuture = service.submit(timedChunker);
        // Send 50 events in 100ms interval. Should take 5s total to send.
        Future senderFuture = service.submit(() -> {
            for (int i : expected) {
                logger.info("sending event {}", i);
                monitoredQueue.write(i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    logger.info("sender interrupted", ex);
                }
            }
        });
        Thread.sleep(6000);
        assertTrue(senderFuture.isDone());
        chunkerFuture.cancel(true);
        assertEquals(expected, processor.getProcessed());
    }

    public static class TestProcessor<T> extends ChunkProcessor<T> {
        private ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor();
        private List<T> processed = new ArrayList();
        private long processingTimeMs = 0;

        public TestProcessor(long processingTimeMs) {
            super(null);
            this.processingTimeMs = processingTimeMs;
        }

        @Override
        public void process(ConnectionManager<T> connectionManager, List<T> chunks) {
            if (processingTimeMs > 0) {
                ScheduledFuture<Boolean> f = scheduledService.schedule(() -> true, processingTimeMs,
                        TimeUnit.MILLISECONDS);
                    while (!f.isDone()) {
                    }
            }
            logger.info("processing {}", chunks);
            processed.addAll(chunks);
        }

        public List<T> getProcessed() {
            return processed;
        }
    }
}
