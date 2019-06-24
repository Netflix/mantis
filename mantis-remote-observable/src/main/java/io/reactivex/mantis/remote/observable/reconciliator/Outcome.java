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

package io.reactivex.mantis.remote.observable.reconciliator;

public class Outcome {

    private int numChecks;
    private int numFailedChecks;
    private double failurePercentage;
    private boolean exceedsFailureThreshold;

    public Outcome(int numChecks, int numFailedChecks,
                   double failurePercentage, boolean exceedsFailureThreshold) {
        this.numChecks = numChecks;
        this.numFailedChecks = numFailedChecks;
        this.failurePercentage = failurePercentage;
        this.exceedsFailureThreshold = exceedsFailureThreshold;
    }

    public int getNumChecks() {
        return numChecks;
    }

    public int getNumFailedChecks() {
        return numFailedChecks;
    }

    public double getFailurePercentage() {
        return failurePercentage;
    }

    public boolean isExceedsFailureThreshold() {
        return exceedsFailureThreshold;
    }
}
