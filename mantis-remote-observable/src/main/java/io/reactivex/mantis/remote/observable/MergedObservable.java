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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;


public class MergedObservable<T> {

    private static final Logger logger = LoggerFactory.getLogger(MergedObservable.class);

    private Subject<Observable<T>, Observable<T>> subject;
    private MergeCounts counts;
    private Map<String, PublishSubject<Integer>> takeUntilSubjects =
            new HashMap<String, PublishSubject<Integer>>();

    private MergedObservable(int expectedTerminalCount, Subject<Observable<T>, Observable<T>> subject) {
        this.subject = subject;
        counts = new MergeCounts(expectedTerminalCount);
    }

    public static <T> MergedObservable<T> create(int expectedTerminalCount) {
        return new MergedObservable<T>(expectedTerminalCount, PublishSubject.<Observable<T>>create());
    }

    public static <T> MergedObservable<T> createWithReplay(int expectedTerminalCount) {
        return new MergedObservable<T>(expectedTerminalCount, ReplaySubject.<Observable<T>>create());
    }

    public synchronized void mergeIn(String key, Observable<T> o) {
        if (!takeUntilSubjects.containsKey(key)) {
            PublishSubject<Integer> takeUntil = PublishSubject.create();
            publishWithCallbacks(key, o.takeUntil(takeUntil), null, null);
            takeUntilSubjects.put(key, takeUntil);
        } else {
            logger.warn("Key alreay exists, ignoring merge request for observable with key: " + key);
        }
    }

    public synchronized void mergeIn(String key, Observable<T> o, Action1<Throwable> errorCallback,
                                     Action0 successCallback) {
        if (!takeUntilSubjects.containsKey(key)) {
            PublishSubject<Integer> takeUntil = PublishSubject.create();
            publishWithCallbacks(key, o.takeUntil(takeUntil), errorCallback, successCallback);
            takeUntilSubjects.put(key, takeUntil);
        } else {
            logger.warn("Key alreay exists, ignoring merge request for observable with key: " + key);
        }
    }

    synchronized void clear() {
        takeUntilSubjects.clear();
    }

    private synchronized void publishWithCallbacks(final String key, Observable<T> o,
                                                   final Action1<Throwable> errorCallback, final Action0 successCallback) {
        subject.onNext(o
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        if (errorCallback != null) {
                            errorCallback.call(t1);
                        }
                        logger.error("Inner observable with key: " + key + " terminated with onError, calling onError() on outer observable." + t1.getMessage(), t1);
                        takeUntilSubjects.remove(key);
                        subject.onError(t1);
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        if (successCallback != null) {
                            successCallback.call();
                        }
                        logger.debug("Inner observable with key: " + key + " completed, incrementing terminal count.");
                        takeUntilSubjects.remove(key);
                        if (counts.incrementTerminalCountAndCheck()) {
                            logger.debug("All inner observables terminated, calling onCompleted() on outer observable.");
                            subject.onCompleted();
                        }
                    }
                }));
    }

    public synchronized void forceComplete(String key) {
        PublishSubject<Integer> takeUntil = takeUntilSubjects.get(key);
        if (takeUntil != null) {
            takeUntil.onNext(1); // complete observable
        } else {
            logger.debug("Nothing to force complete, key doesn't exist: " + key);
        }
    }

    public synchronized Observable<Observable<T>> get() {
        return subject;
    }
}
