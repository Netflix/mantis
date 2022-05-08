/*
 * Copyright 2022 Netflix, Inc.
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

package com.mantisrx.common.utils;

import static io.mantisrx.shaded.com.google.common.base.Preconditions.checkNotNull;

import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.collect.Queues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.jcip.annotations.GuardedBy;

public final class ListenerCallQueue<L> {
    // TODO(cpovirk): consider using the logger associated with listener.getClass().
    private static final Logger logger = Logger.getLogger(ListenerCallQueue.class.getName());

    // TODO(chrisn): promote AppendOnlyCollection for use here.
    private final List<PerListenerQueue<L>> listeners =
        Collections.synchronizedList(new ArrayList<PerListenerQueue<L>>());

    /** Method reference-compatible listener event. */
    public interface Event<L> {
        /** Call a method on the listener. */
        void call(L listener);
    }

    /**
     * Adds a listener that will be called using the given executor when events are later {@link
     * #enqueue enqueued} and {@link #dispatch dispatched}.
     */
    public void addListener(L listener, Executor executor) {
        checkNotNull(listener, "listener");
        checkNotNull(executor, "executor");
        listeners.add(new PerListenerQueue<>(listener, executor));
    }

    /**
     * Enqueues an event to be run on currently known listeners.
     *
     * <p>The {@code toString} method of the Event itself will be used to describe the event in the
     * case of an error.
     *
     * @param event the callback to execute on {@link #dispatch}
     */
    public void enqueue(Event<L> event) {
        enqueueHelper(event, event);
    }

    /**
     * Enqueues an event to be run on currently known listeners, with a label.
     *
     * @param event the callback to execute on {@link #dispatch}
     * @param label a description of the event to use in the case of an error
     */
    public void enqueue(Event<L> event, String label) {
        enqueueHelper(event, label);
    }

    private void enqueueHelper(Event<L> event, Object label) {
        checkNotNull(event, "event");
        checkNotNull(label, "label");
        synchronized (listeners) {
            for (PerListenerQueue<L> queue : listeners) {
                queue.add(event, label);
            }
        }
    }

    /**
     * Dispatches all events enqueued prior to this call, serially and in order, for every listener.
     *
     * <p>Note: this method is idempotent and safe to call from any thread
     */
    public void dispatch() {
        // iterate by index to avoid concurrent modification exceptions
        for (int i = 0; i < listeners.size(); i++) {
            listeners.get(i).dispatch();
        }
    }

    /**
     * A special purpose queue/executor that dispatches listener events serially on a configured
     * executor. Each event event can be added and dispatched as separate phases.
     *
     * <p>This class is very similar to {@link SequentialExecutor} with the exception that events can
     * be added without necessarily executing immediately.
     */
    private static final class PerListenerQueue<L> implements Runnable {
        final L listener;
        final Executor executor;

        @GuardedBy("this")
        final Queue<Event<L>> waitQueue = Queues.newArrayDeque();

        @GuardedBy("this")
        final Queue<Object> labelQueue = Queues.newArrayDeque();

        @GuardedBy("this")
        boolean isThreadScheduled;

        PerListenerQueue(L listener, Executor executor) {
            this.listener = checkNotNull(listener);
            this.executor = checkNotNull(executor);
        }

        /** Enqueues a event to be run. */
        synchronized void add(ListenerCallQueue.Event<L> event, Object label) {
            waitQueue.add(event);
            labelQueue.add(label);
        }

        /**
         * Dispatches all listeners {@linkplain #enqueue enqueued} prior to this call, serially and in
         * order.
         */
        void dispatch() {
            boolean scheduleEventRunner = false;
            synchronized (this) {
                if (!isThreadScheduled) {
                    isThreadScheduled = true;
                    scheduleEventRunner = true;
                }
            }
            if (scheduleEventRunner) {
                try {
                    executor.execute(this);
                } catch (RuntimeException e) {
                    // reset state in case of an error so that later dispatch calls will actually do something
                    synchronized (this) {
                        isThreadScheduled = false;
                    }
                    // Log it and keep going.
                    logger.log(
                        Level.SEVERE,
                        "Exception while running callbacks for " + listener + " on " + executor,
                        e);
                    throw e;
                }
            }
        }

        @Override
        public void run() {
            boolean stillRunning = true;
            try {
                while (true) {
                    ListenerCallQueue.Event<L> nextToRun;
                    Object nextLabel;
                    synchronized (PerListenerQueue.this) {
                        Preconditions.checkState(isThreadScheduled);
                        nextToRun = waitQueue.poll();
                        nextLabel = labelQueue.poll();
                        if (nextToRun == null) {
                            isThreadScheduled = false;
                            stillRunning = false;
                            break;
                        }
                    }

                    // Always run while _not_ holding the lock, to avoid deadlocks.
                    try {
                        nextToRun.call(listener);
                    } catch (RuntimeException e) {
                        // Log it and keep going.
                        logger.log(
                            Level.SEVERE,
                            "Exception while executing callback: " + listener + " " + nextLabel,
                            e);
                    }
                }
            } finally {
                if (stillRunning) {
                    // An Error is bubbling up. We should mark ourselves as no longer running. That way, if
                    // anyone tries to keep using us, we won't be corrupted.
                    synchronized (PerListenerQueue.this) {
                        isThreadScheduled = false;
                    }
                }
            }
        }
    }
}
