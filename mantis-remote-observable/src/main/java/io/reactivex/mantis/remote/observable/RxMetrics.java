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

//import io.mantisrx.common.metrics.Counter;
//import io.mantisrx.common.metrics.Metrics;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.List;

public class RxMetrics {

//    private Metrics metrics;
    private MeterRegistry meterRegistry;
    private Counter next;
    private Counter nextFailure;
    private Counter error;
    private Counter errorFailure;
    private Counter complete;
    private Counter completeFailure;
    private Counter subscribe;
    private Counter unsubscribe;

    public RxMetrics() {
//        metrics = new Metrics.Builder()
//                .name("RemoteObservableMetrics")
//                .addCounter("onNext")
//                .addCounter("onNextFailure")
//                .addCounter("onError")
//                .addCounter("onErrorFailure")
//                .addCounter("onComplete")
//                .addCounter("onCompleteFailure")
//                .addCounter("subscribe")
//                .addCounter("unsubscribe")
//                .build();

        next = meterRegistry.counter("RemoteObservableMetrics_onNext");
        nextFailure = meterRegistry.counter("RemoteObservableMetrics_onNextFailure");
        error = meterRegistry.counter("RemoteObservableMetrics_onError");
        errorFailure = meterRegistry.counter("RemoteObservableMetrics_onErrorFailure");
        complete = meterRegistry.counter("RemoteObservableMetrics_onComplete");
        completeFailure = meterRegistry.counter("RemoteObservableMetrics_onCompleteFailure");
        subscribe = meterRegistry.counter("RemoteObservableMetrics_subscribe");
        unsubscribe = meterRegistry.counter("RemoteObservableMetrics_unsubscribe");
    }

    public List<Meter> getCountersAndGauges() {
        List<Meter> meters = new ArrayList<>();
        meters.add(next);
        meters.add(nextFailure);
        meters.add(error);
        meters.add(errorFailure);
        meters.add(complete);
        meters.add(completeFailure);
        meters.add(subscribe);
        meters.add(unsubscribe);
        return meters;
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
        return (long)next.count();
    }

    public long getOnNextFailureCount() {
        return (long)nextFailure.count();
    }

    public long getOnNextRollingFailureCount() {
        return nextFailure.rateValue();
    }

    public long getOnErrorRollingCount() {
        return error.rateValue();
    }

    public long getOnErrorCount() {
        return (long)error.count();
    }

    public long getOnErrorFailureCount() {
        return (long)errorFailure.count();
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
        return (long)complete.count();
    }

    public long getOnCompletedFailureCount() {
        return (long)completeFailure.count();
    }

    public long getSubscribedCount() {
        return (long)subscribe.count();
    }

    public long getUnsubscribedCount() {
        return (long)unsubscribe.count();
    }
}
