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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TimedChunker<T> implements Callable<Void> {

    private static ThreadFactory namedFactory = new NamedThreadFactory("TimedChunkerGroup");
    private MonitoredQueue<T> buffer;
    private ChunkProcessor<T> processor;
    private ExecutorService timerPool = Executors.newSingleThreadExecutor(namedFactory);
    private int maxBufferLength;
    private int maxTimeMSec;
    private ConnectionManager<T> connectionManager;

    public TimedChunker(MonitoredQueue<T> buffer, int maxBufferLength,
                        int maxTimeMSec, ChunkProcessor<T> processor,
                        ConnectionManager<T> connectionManager) {
        this.maxBufferLength = maxBufferLength;
        this.maxTimeMSec = maxTimeMSec;
        this.buffer = buffer;
        this.processor = processor;
        this.connectionManager = connectionManager;
    }

    @Override
    public Void call() throws Exception {
        while (!stopCondition()) {
            T data = buffer.get();
            Future<Void> queryFuture = timerPool.submit(new Chunker<T>(processor, data, buffer, maxBufferLength,
                    connectionManager));
            try {
                queryFuture.get(maxTimeMSec, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                queryFuture.cancel(true);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw e;
            } catch (TimeoutException e) {
                // cancel task below
            } finally {
                // send interrupt signal to thread
                queryFuture.cancel(true);
            }
        }
        return null;
    }

    private boolean stopCondition() {
        return Thread.currentThread().isInterrupted();
    }

}
