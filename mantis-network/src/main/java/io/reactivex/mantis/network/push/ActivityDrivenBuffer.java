/*
 * Copyright 2026 Netflix, Inc.
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.subscriptions.SerialSubscription;
import rx.subscriptions.Subscriptions;

/** Connection buffering operator: one timer while a buffer is nonempty. */
final class ActivityDrivenBuffer<T> implements Observable.Operator<List<T>, T> {
    private final long interval;
    private final TimeUnit unit;
    private final Scheduler scheduler;

    ActivityDrivenBuffer(long interval, TimeUnit unit, Scheduler scheduler) {
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.interval = interval;
        this.unit = unit;
        this.scheduler = scheduler;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super List<T>> child) {
        var state = new State(child, scheduler.createWorker());
        child.add(Subscriptions.create(state::cancel));
        var parent = new Subscriber<T>() {
            @Override
            public void onStart() {
                request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T value) {
                state.next(value);
            }

            @Override
            public void onError(Throwable error) {
                state.finish(error);
            }

            @Override
            public void onCompleted() {
                state.finish(null);
            }
        };
        child.add(parent);
        return parent;
    }

    private final class Emission {
        final List<T> batch;
        final Throwable error;
        Emission(List<T> batch, Throwable error) {
            this.batch = batch;
            this.error = error;
        }
    }

    private final class State {
        final Subscriber<? super List<T>> child;
        final Scheduler.Worker worker;
        final SerialSubscription timer = new SerialSubscription();
        final ArrayDeque<Emission> ready = new ArrayDeque<>();
        List<T> pending;
        boolean done;
        boolean cancelled;
        boolean draining;

        State(Subscriber<? super List<T>> child, Scheduler.Worker worker) {
            this.child = child;
            this.worker = worker;
        }

        void next(T value) {
            Throwable schedulingError = null;
            synchronized (this) {
                if (done || cancelled || child.isUnsubscribed()) {
                    return;
                }
                if (pending != null) {
                    pending.add(value);
                    return;
                }
                pending = new ArrayList<>();
                pending.add(value);
                try {
                    timer.set(worker.schedule(this::tick, interval, unit));
                } catch (Throwable error) {
                    Exceptions.throwIfFatal(error);
                    schedulingError = error;
                }
            }
            if (schedulingError != null) {
                finish(schedulingError);
            }
        }

        void tick() {
            boolean drain;
            synchronized (this) {
                if (done || cancelled) {
                    return;
                }
                // Detach the fired action before downstream work. A subsequent arrival
                // must not cancel (and interrupt) this callback while it is draining.
                timer.set(Subscriptions.empty());
                if (pending != null) {
                    ready.add(new Emission(pending, null));
                    pending = null;
                }
                drain = !draining && !ready.isEmpty();
                if (drain) {
                    draining = true;
                }
            }
            if (drain) {
                drain();
            }
        }

        void finish(Throwable error) {
            boolean drain;
            synchronized (this) {
                if (done || cancelled) {
                    return;
                }
                done = true;
                if (error == null && pending != null) {
                    ready.add(new Emission(pending, null));
                }
                pending = null; // An error discards the unflushed buffer, like RxJava's buffer.
                ready.add(new Emission(null, error));
                drain = !draining;
                if (drain) {
                    draining = true;
                }
            }
            if (drain) {
                drain();
            }
        }

        void drain() {
            for (;;) {
                Emission emission;
                synchronized (this) {
                    if (cancelled || child.isUnsubscribed()) {
                        ready.clear();
                        draining = false;
                        return;
                    }
                    emission = ready.poll();
                    if (emission == null) {
                        draining = false;
                        return;
                    }
                }
                if (emission.batch != null) {
                    try {
                        child.onNext(emission.batch);
                    } catch (Throwable error) {
                        Exceptions.throwIfFatal(error);
                        cancel();
                        child.onError(error);
                        return;
                    }
                } else {
                    // Dispose here, after earlier emissions finish. Disposing from the producer
                    // during finish() could interrupt a timer thread inside child.onNext().
                    timer.unsubscribe();
                    worker.unsubscribe();
                    if (emission.error != null) {
                        child.onError(emission.error);
                    } else {
                        child.onCompleted();
                    }
                    return;
                }
            }
        }

        void cancel() {
            synchronized (this) {
                cancelled = true;
                pending = null;
                ready.clear();
            }
            timer.unsubscribe();
            worker.unsubscribe();
        }
    }
}
