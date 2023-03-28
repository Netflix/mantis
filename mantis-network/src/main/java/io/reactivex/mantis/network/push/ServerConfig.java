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

import io.mantisrx.common.metrics.MetricsRegistry;
import java.util.List;
import java.util.Map;
import rx.functions.Func1;


public class ServerConfig<T> {

    private String name;
    private int port;
    private int numQueueConsumers = 1; // number of threads chunking queue
    private int bufferCapacity = 100; // max queue size
    private int writeRetryCount = 2; // num retries before fail to write
    private int maxChunkSize = 25;  // max items to read from queue, for a single chunk
    private int maxChunkTimeMSec = 100; // max time to read from queue, for a single chunk
    private int maxNotWritableTimeSec = -1; // max time the channel can stay not writable, <= 0 means unlimited
    private ChunkProcessor<T> chunkProcessor; // logic to process chunk
    private MetricsRegistry metricsRegistry; // registry used to store metrics
    private Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate;
    private boolean useSpscQueue = false;

    public ServerConfig(Builder<T> builder) {
        this.name = builder.name;
        this.port = builder.port;
        this.bufferCapacity = builder.bufferCapacity;
        this.writeRetryCount = builder.writeRetryCount;
        this.maxChunkSize = builder.maxChunkSize;
        this.maxChunkTimeMSec = builder.maxChunkTimeMSec;
        this.chunkProcessor = builder.chunkProcessor;
        this.metricsRegistry = builder.metricsRegistry;
        this.numQueueConsumers = builder.numQueueConsumers;
        this.predicate = builder.predicate;
        this.useSpscQueue = builder.useSpscQueue;
        this.maxNotWritableTimeSec = builder.maxNotWritableTimeSec;
    }

    public Func1<Map<String, List<String>>, Func1<T, Boolean>> getPredicate() {
        return predicate;
    }

    public int getNumQueueConsumers() {
        return numQueueConsumers;
    }

    public String getName() {
        return name;
    }

    public int getPort() {
        return port;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public int getWriteRetryCount() {
        return writeRetryCount;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public int getMaxChunkTimeMSec() {
        return maxChunkTimeMSec;
    }

    public int getMaxNotWritableTimeSec() {
        return maxNotWritableTimeSec;
    }

    public ChunkProcessor<T> getChunkProcessor() {
        return chunkProcessor;
    }

    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    public boolean useSpscQueue() {
        return useSpscQueue;
    }

    public static class Builder<T> {

        private String name;
        private int port;
        private int numQueueConsumers = 1; // number of threads chunking queue
        private int bufferCapacity = 100; // max queue size
        private int writeRetryCount = 2; // num retries before fail to write
        private int maxChunkSize = 25;  // max items to read from queue, for a single chunk
        private int maxChunkTimeMSec = 100; // max time to read from queue, for a single chunk
        private int maxNotWritableTimeSec = -1; // max time the channel can stay not writable, <= 0 means unlimited
        private ChunkProcessor<T> chunkProcessor; // logic to process chunk
        private MetricsRegistry metricsRegistry; // registry used to store metrics
        private Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate;
        private boolean useSpscQueue = false;

        public Builder<T> predicate(Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> bufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder<T> useSpscQueue(boolean useSpsc) {
            this.useSpscQueue = useSpsc;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> numQueueConsumers(int numQueueConsumers) {
            this.numQueueConsumers = numQueueConsumers;
            return this;
        }

        public Builder<T> writeRetryCount(int writeRetryCount) {
            this.writeRetryCount = writeRetryCount;
            return this;
        }

        public Builder<T> maxChunkSize(int maxChunkSize) {
            this.maxChunkSize = maxChunkSize;
            return this;
        }

        public Builder<T> maxChunkTimeMSec(int maxChunkTimeMSec) {
            this.maxChunkTimeMSec = maxChunkTimeMSec;
            return this;
        }

        public Builder<T> maxNotWritableTimeSec(int maxNotWritableTimeSec) {
            this.maxNotWritableTimeSec = maxNotWritableTimeSec;
            return this;
        }

        public Builder<T> groupRouter(Router<T> router) {
            this.chunkProcessor = new GroupChunkProcessor<>(router);
            return this;
        }

        public Builder<T> router(Router<T> router) {
            this.chunkProcessor = new ChunkProcessor<>(router);
            return this;
        }

        public Builder<T> metricsRegistry(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
            return this;
        }

        public ServerConfig<T> build() {
            return new ServerConfig<>(this);
        }
    }
}
