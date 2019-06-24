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

public class Check<T> {

    private T expected;
    private T actual;
    private boolean passedCheck;
    private long timestamp;

    public Check(T expected, T actual,
                 boolean passedCheck, long timestamp) {
        this.expected = expected;
        this.actual = actual;
        this.passedCheck = passedCheck;
        this.timestamp = timestamp;
    }

    public T getExpected() {
        return expected;
    }

    public T getActual() {
        return actual;
    }

    public boolean isPassedCheck() {
        return passedCheck;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
