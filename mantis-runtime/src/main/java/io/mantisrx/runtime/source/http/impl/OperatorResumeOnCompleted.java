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

package io.mantisrx.runtime.source.http.impl;

import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.functions.Action0;
import rx.schedulers.Schedulers;
import rx.subscriptions.SerialSubscription;


public class OperatorResumeOnCompleted<T> implements Operator<T, T> {

    private static final Scheduler scheduler = Schedulers.trampoline();
    private final ResumeOnCompletedPolicy<T> resumePolicy;
    private final int currentAttempts;


    private OperatorResumeOnCompleted(int currentAttempts, ResumeOnCompletedPolicy<T> resumePolicy) {
        this.currentAttempts = currentAttempts;
        this.resumePolicy = resumePolicy;
    }

    public OperatorResumeOnCompleted(ResumeOnCompletedPolicy<T> resumePolicy) {
        this(0, resumePolicy);
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super T> child) {
        final SerialSubscription serialSubscription = new SerialSubscription();
        child.add(serialSubscription);

        return new Subscriber<T>(child) {
            private final Worker worker = scheduler.createWorker();

            @Override
            public void onCompleted() {
                worker.schedule(new Action0() {
                    @Override
                    public void call() {
                        try {
                            int newAttempts = currentAttempts + 1;

                            Observable<? extends T> resume = resumePolicy.call(newAttempts);
                            if (resume == null) {
                                child.onCompleted();
                            } else {
                                resume = resume.lift(new OperatorResumeOnCompleted<>(
                                        newAttempts,
                                        resumePolicy
                                ));

                                serialSubscription.set(resume.unsafeSubscribe(child));
                            }
                        } catch (Throwable e2) {
                            child.onError(e2);
                        }
                    }
                });
            }

            @Override
            public void onError(final Throwable e) {
                child.onError(e);
            }

            @Override
            public void onNext(T t) {
                child.onNext(t);
            }
        };
    }
}
