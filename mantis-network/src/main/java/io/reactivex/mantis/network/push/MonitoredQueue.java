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

package io.reactivex.mantis.network.push;

import com.mantisrx.common.utils.MantisMetricStringConstants;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import java.util.AbstractQueue;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.jctools.queues.SpscArrayQueue;


public class MonitoredQueue<T> {

    final boolean isSpsc;
    //private BlockingQueue<T> queue;
    private AbstractQueue<T> queue;
    private MeterRegistry meterRegistry;
    private Counter numSuccessEnqueu;
    private Counter numFailedEnqueu;
    private Gauge queueDepth;
    private AtomicLong queueDepthValue = new AtomicLong(0);

    public MonitoredQueue(String name, int capacity) {
        this(name, capacity, true);
    }

    public MonitoredQueue(String name, int capacity, boolean useSpsc) {
        this.isSpsc = useSpsc;
        if (!useSpsc) {
            queue = new LinkedBlockingQueue<>(capacity);
        } else {
            queue = new SpscArrayQueue<>(capacity);
        }

        final String qId = Optional.ofNullable(name).orElse("none");
        final Tags idTag = Tags.of(MantisMetricStringConstants.GROUP_ID_TAG, qId);

        numSuccessEnqueu = meterRegistry.counter("MonitoredQueue_numSuccessEnqueu", idTag);
        numFailedEnqueu = meterRegistry.counter("MonitoredQueue_numFailedEnqueu", idTag);
        queueDepth = Gauge.builder("MonitoredQueue_queueDepth", queueDepthValue::get)
                .tags(idTag)
                .register(meterRegistry);
    }

    public boolean write(T data) {
        boolean offer = queue.offer(data);
        queueDepthValue.set(queue.size());
        if (offer) {
            numSuccessEnqueu.increment();
        } else {
            numFailedEnqueu.increment();
        }
        return offer;
    }

    public List<Meter> getMetrics() {
        List<Meter> meters = new LinkedList<>();
        meters.add(numSuccessEnqueu);
        meters.add(numFailedEnqueu);
        meters.add(queueDepth);
        return meters;
    }

    public T get() throws InterruptedException {
        if (!isSpsc) {
            return ((LinkedBlockingQueue<T>) queue).take();
        }
        //return queue.take();
        //spsc does not implement take
        return queue.poll();
    }

    //	public T poll(long timeout, TimeUnit unit) throws InterruptedException{
    //		return queue.poll(timeout, unit);
    //	}
    public T poll() {
        return queue.poll();
    }

    public void clear() {
        queue.clear();
    }
}
