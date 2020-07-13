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

package io.mantisrx.server.core;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class StatusPayloads {

    public enum Type {
        SubscriptionState,
        IncomingDataDrop,
        ResourceUsage
    }

    public static class DataDropCounts {

        private final long onNextCount;
        private final long droppedCount;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public DataDropCounts(@JsonProperty("onNextCount") long onNextCount, @JsonProperty("droppedCount") long droppedCount) {
            this.onNextCount = onNextCount;
            this.droppedCount = droppedCount;
        }

        public long getOnNextCount() {
            return onNextCount;
        }

        public long getDroppedCount() {
            return droppedCount;
        }
    }

    public static class ResourceUsage {

        private final double cpuLimit;
        private final double cpuUsageCurrent;
        private final double cpuUsagePeak;
        private final double memLimit;
        private final double memCacheCurrent;
        private final double memCachePeak;
        private final double totMemUsageCurrent;
        private final double totMemUsagePeak;
        private final double nwBytesCurrent;
        private final double nwBytesPeak;

        @JsonCreator
        public ResourceUsage(@JsonProperty("cpuLimit") double cpuLimit,
                             @JsonProperty("cpuUsageCurrent") double cpuUsageCurrent,
                             @JsonProperty("cpuUsagePeak") double cpuUsagePeak,
                             @JsonProperty("memLimit") double memLimit,
                             @JsonProperty("memCacheCurrent") double memCacheCurrent,
                             @JsonProperty("memCachePeak") double memCachePeak,
                             @JsonProperty("totMemUsageCurrent") double totMemUsageCurrent,
                             @JsonProperty("totMemUsagePeak") double totMemUsagePeak,
                             @JsonProperty("nwBytesCurrent") double nwBytesCurrent,
                             @JsonProperty("nwBytesPeak") double nwBytesPeak) {
            this.cpuLimit = cpuLimit;
            this.cpuUsageCurrent = cpuUsageCurrent;
            this.cpuUsagePeak = cpuUsagePeak;
            this.memLimit = memLimit;
            this.memCacheCurrent = memCacheCurrent;
            this.memCachePeak = memCachePeak;
            this.totMemUsageCurrent = totMemUsageCurrent;
            this.totMemUsagePeak = totMemUsagePeak;
            this.nwBytesCurrent = nwBytesCurrent;
            this.nwBytesPeak = nwBytesPeak;
        }

        public double getCpuLimit() {
            return cpuLimit;
        }

        public double getCpuUsageCurrent() {
            return cpuUsageCurrent;
        }

        public double getCpuUsagePeak() {
            return cpuUsagePeak;
        }

        public double getMemLimit() {
            return memLimit;
        }

        public double getMemCacheCurrent() {
            return memCacheCurrent;
        }

        public double getMemCachePeak() {
            return memCachePeak;
        }

        public double getTotMemUsageCurrent() {
            return totMemUsageCurrent;
        }

        public double getTotMemUsagePeak() {
            return totMemUsagePeak;
        }

        public double getNwBytesCurrent() {
            return nwBytesCurrent;
        }

        public double getNwBytesPeak() {
            return nwBytesPeak;
        }

        @Override
        public String toString() {
            return "cpuLimit=" + cpuLimit + ", cpuUsageCurrent=" + cpuUsageCurrent + ", cpuUsagePeak=" + cpuUsagePeak +
                    ", memLimit=" + memLimit + ", memCacheCurrent=" + memCacheCurrent + ", memCachePeak=" + memCachePeak +
                    ", totMemUsageCurrent=" + totMemUsageCurrent + ", totMemUsagePeak=" + totMemUsagePeak +
                    ", nwBytesCurrent=" + nwBytesCurrent + ", nwBytesPeak=" + nwBytesPeak;
        }
    }
}
