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

import java.util.List;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.codec.JsonType;


public class JobAssignmentResult implements JsonType {

    private final String jobId;
    private final List<Failure> failures;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobAssignmentResult(@JsonProperty("jobId") String jobId,
                               @JsonProperty("failures") List<Failure> failures) {
        this.jobId = jobId;
        this.failures = failures;
    }

    private static boolean failuresIdentical(List<Failure> first, List<Failure> second) {
        if (first == null) {
            return second == null;
        }
        if (second == null)
            return false;
        if (first.size() != second.size())
            return false;
        int item = 0;
        for (Failure f : first) {
            boolean found = false;
            for (int fi = 0; fi < second.size() && !found; fi++) {
                if (f.isIdentical(second.get(fi)))
                    found = true;
            }
            if (!found)
                return false;
        }
        return true;
    }

    public String getJobId() {
        return jobId;
    }

    public List<Failure> getFailures() {
        return failures;
    }

    public boolean isIdentical(JobAssignmentResult that) {
        if (that == null)
            return false;
        if (this == that)
            return true;
        if (!jobId.equals(that.jobId))
            return false;
        return failuresIdentical(failures, that.failures);
    }

    public static class Failure {

        private int workerNumber;
        private String type;
        private double asking;
        private double used;
        private double available;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Failure(@JsonProperty("workerNumber") int workerNumber,
                       @JsonProperty("type") String type,
                       @JsonProperty("asking") double asking,
                       @JsonProperty("used") double used,
                       @JsonProperty("available") double available) {
            this.workerNumber = workerNumber;
            this.type = type;
            this.asking = asking;
            this.used = used;
            this.available = available;
        }

        public int getWorkerNumber() {
            return workerNumber;
        }

        public String getType() {
            return type;
        }

        public double getAsking() {
            return asking;
        }

        public double getUsed() {
            return used;
        }

        public double getAvailable() {
            return available;
        }

        private boolean isIdentical(Failure that) {
            if (that == null)
                return false;
            return workerNumber == that.workerNumber
                    && type.equals(that.type)
                    && asking == that.asking
                    && used == that.used
                    && available == that.available;
        }
    }
}
