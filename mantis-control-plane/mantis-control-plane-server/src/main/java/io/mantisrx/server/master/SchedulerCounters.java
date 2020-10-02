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

package io.mantisrx.server.master;

import java.util.concurrent.atomic.AtomicInteger;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;


public class SchedulerCounters {

    private static final SchedulerCounters instance = new SchedulerCounters();
    private final AtomicInteger iterationNumberCounter = new AtomicInteger();
    private final AtomicInteger numResourceAllocationTrials = new AtomicInteger(0);
    private volatile IterationCounter counter = null;
    private SchedulerCounters() {
    }

    public static SchedulerCounters getInstance() {
        return instance;
    }

    public void incrementResourceAllocationTrials(int delta) {
        numResourceAllocationTrials.addAndGet(delta);
    }

    public IterationCounter getCounter() {
        return counter;
    }

    void endIteration(int numWorkersToLaunch, int numWorkersLaunched, int numSlavesToUse, int numSlavesRejected) {
        counter = new IterationCounter(iterationNumberCounter.getAndIncrement(), numWorkersToLaunch,
                numWorkersLaunched, numSlavesToUse, numSlavesRejected, numResourceAllocationTrials.getAndSet(0));
    }

    String toJsonString() {
        return counter.toJsonString();
    }

    public class IterationCounter {

        @JsonIgnore
        private final ObjectMapper mapper = new ObjectMapper();
        private int iterationNumber;
        private int numWorkersToLaunch;
        private int numWorkersLaunched;
        private int numSlavesToUse;
        private int numSlavesRejected;
        private int numResourceAllocations;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        IterationCounter(@JsonProperty("iterationNumber") int iterationNumber,
                         @JsonProperty("numWorkersToLaunch") int numWorkersToLaunch,
                         @JsonProperty("numWorkersLaunched") int numWorkersLaunched,
                         @JsonProperty("numSlavesToUse") int numOffersToUse,
                         @JsonProperty("numSlavesRejected") int numOffersRejected,
                         @JsonProperty("numResourceAllocationTrials") int numResourceAllocations) {
            this.iterationNumber = iterationNumber;
            this.numWorkersToLaunch = numWorkersToLaunch;
            this.numWorkersLaunched = numWorkersLaunched;
            this.numSlavesToUse = numOffersToUse;
            this.numSlavesRejected = numOffersRejected;
            this.numResourceAllocations = numResourceAllocations;
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        void setCounters(int numWorkersToLaunch,
                         int numWorkersLaunched,
                         int numOffersToUse,
                         int numOffersRejected) {
            this.iterationNumber++;
            this.numWorkersToLaunch = numWorkersToLaunch;
            this.numWorkersLaunched = numWorkersLaunched;
            this.numSlavesToUse = numOffersToUse;
            this.numSlavesRejected = numOffersRejected;
        }

        public int getIterationNumber() {
            return iterationNumber;
        }

        public int getNumWorkersToLaunch() {
            return numWorkersToLaunch;
        }

        public int getNumWorkersLaunched() {
            return numWorkersLaunched;
        }

        public int getNumSlavesToUse() {
            return numSlavesToUse;
        }

        public int getNumSlavesRejected() {
            return numSlavesRejected;
        }

        public int getNumResourceAllocations() {
            return numResourceAllocations;
        }

        public String toJsonString() {
            try {
                return mapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                // shouldn't happen
                return iterationNumber + ", " + numWorkersToLaunch + ", " + numWorkersLaunched + ", " + numSlavesToUse
                        + ", " + numSlavesRejected + ", " + numResourceAllocations;
            }
        }
    }

}
