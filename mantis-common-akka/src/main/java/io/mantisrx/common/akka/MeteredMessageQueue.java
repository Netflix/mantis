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

package io.mantisrx.common.akka;

import akka.actor.ActorRef;
import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import akka.dispatch.UnboundedMessageQueueSemantics;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;


/**
 * A custom implementation of a message queue used by a few key Actors. This implementation
 * keeps track of enqueue and wait rates to the Actor queue.
 */
public class MeteredMessageQueue implements MessageQueue, UnboundedMessageQueueSemantics {
    private final String path;
    private final Counter insertCounter;
    private final Timer waitTimer;
    private final ConcurrentLinkedQueue<Entry> queue = new ConcurrentLinkedQueue<>();

    /**
     * Creates an instance.
     * @param path The actor path.
     */
    public MeteredMessageQueue(final String path) {
        Registry registry = SpectatorRegistryFactory.getRegistry();
        this.path = path;
        this.insertCounter = registry.counter("akka.queue.insert", "path", path);
        this.waitTimer = registry.timer("akka.queue.wait", "path", path);
        PolledMeter
            .using(registry)
            .withName("akka.queue.size")
            .withTag("path", path)
            .monitorSize(queue);
    }

    /**
     * A wrapper class that adds the time of creation of a message.
     */
    static final class Entry {

        /**
         * The {@link Envelope} used by Akka around each enqueued message.
         */
        private final Envelope v;
        /**
         * Nano time of when the message was enqueued.
         */
        private final long t;

        /**
         * Creates an instance of this class.
         * @param v
         */
         Entry(final Envelope v) {
            this.v = v;
            this.t = System.nanoTime();
        }
    }

    /**
     * Invoked every time a message is enqueued for an Actor.
     * @param receiver
     * @param handle
     */
    public void enqueue(ActorRef receiver, Envelope handle) {
        insertCounter.increment();
        queue.offer(new Entry(handle));
    }

    /**
     * Invoked every time a message is dequeued from an Actor's queue.
     * @return
     */
    public Envelope dequeue() {
        Entry tmp = queue.poll();
        if (tmp == null) {
            return null;
        } else {
            long dur = System.nanoTime() - tmp.t;
            waitTimer.record(dur, TimeUnit.NANOSECONDS);
            return tmp.v;
        }
    }

    /**
     * Returns current queue size.
     * @return queue size
     */
    public int numberOfMessages() {
        return queue.size();
    }

    /**
     * Returns true if there is atleast a single message in the queue.
     * @return boolean whether queue is not empty.
     */
    public boolean hasMessages() {
        return !queue.isEmpty();
    }

    /**
     * Clears the Actor queue.
     * @param owner
     * @param deadLetters
     */
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        queue.clear();
    }
}
