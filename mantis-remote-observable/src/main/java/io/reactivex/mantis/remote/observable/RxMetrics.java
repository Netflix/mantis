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

package io.reactivex.mantis.remote.observable;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;


public class RxMetrics {

    private Metrics metrics;
    private Counter next;
    private Counter nextFailure;
    private Counter error;
    private Counter errorFailure;
    private Counter complete;
    private Counter completeFailure;
    private Counter subscribe;
    private Counter unsubscribe;

    public RxMetrics() {
        metrics = new Metrics.Builder()
                .name("RemoteObservableMetrics")
                .addCounter("onNext")
                .addCounter("onNextFailure")
                .addCounter("onError")
                .addCounter("onErrorFailure")
                .addCounter("onComplete")
                .addCounter("onCompleteFailure")
                .addCounter("subscribe")
                .addCounter("unsubscribe")
                .build();

        next = metrics.getCounter("onNext");
        nextFailure = metrics.getCounter("onNextFailure");
        error = metrics.getCounter("onError");
        errorFailure = metrics.getCounter("onErrorFailure");
        complete = metrics.getCounter("onComplete");
        completeFailure = metrics.getCounter("onCompleteFailure");
        subscribe = metrics.getCounter("subscribe");
        unsubscribe = metrics.getCounter("unsubscribe");
    }

    public Metrics getCountersAndGauges() {
        return metrics;
    }

    public void incrementNextFailureCount() {
        nextFailure.increment();
    }

    public void incrementNextFailureCount(long numFailed) {
        nextFailure.increment(numFailed);
    }

    public void incrementNextCount() {
        next.increment();
    }

    public void incrementNextCount(long numOnNext) {
        next.increment(numOnNext);
    }

    public void incrementErrorCount() {
        error.increment();
    }

    public void incrementErrorFailureCount() {
        errorFailure.increment();
    }

    public void incrementCompletedCount() {
        complete.increment();
    }

    public void incrementCompletedFailureCount() {
        completeFailure.increment();
    }

    public void incrementSubscribedCount() {
        subscribe.increment();
    }

    public void incrementUnsubscribedCount() {
        unsubscribe.increment();
    }

    public long getOnNextRollingCount() {
        return next.rateValue();
    }

    public long getOnNextCount() {
        return next.value();
    }

    public long getOnNextFailureCount() {
        return nextFailure.value();
    }

    public long getOnNextRollingFailureCount() {
        return nextFailure.rateValue();
    }

    public long getOnErrorRollingCount() {
        return error.rateValue();
    }

    public long getOnErrorCount() {
        return error.value();
    }

    public long getOnErrorFailureCount() {
        return errorFailure.value();
    }

    public long getOnErrorRollingFailureCount() {
        return errorFailure.rateValue();
    }

    public long getOnCompletedRollingCount() {
        return complete.rateValue();
    }

    public long getOnCompletedRollingFailureCount() {
        return completeFailure.rateValue();
    }

    public long getOnCompletedCount() {
        return complete.value();
    }

    public long getOnCompletedFailureCount() {
        return completeFailure.value();
    }

    public long getSubscribedCount() {
        return subscribe.value();
    }

    public long getUnsubscribedCount() {
        return unsubscribe.value();
    }
}
