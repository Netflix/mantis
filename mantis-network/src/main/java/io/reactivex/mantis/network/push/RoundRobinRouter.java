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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import rx.functions.Func1;


public class RoundRobinRouter<T> extends Router<T> {

    public RoundRobinRouter(String name, Func1<T, byte[]> encoder) {
        super("RoundRobinRouter_" + name, encoder);
    }

    @Override
    public void route(Set<AsyncConnection<T>> connections, List<T> chunks) {
        if (chunks != null && !chunks.isEmpty()) {
            numEventsProcessed.increment(chunks.size());
        }
        List<AsyncConnection<T>> randomOrder = new ArrayList<>(connections);
        Collections.shuffle(randomOrder);
        if (chunks != null && !chunks.isEmpty() && !randomOrder.isEmpty()) {
            Iterator<AsyncConnection<T>> iter = loopingIterator(randomOrder);
            Map<AsyncConnection<T>, List<byte[]>> writes = new HashMap<>();
            // process chunks
            for (T chunk : chunks) {
                AsyncConnection<T> connection = iter.next();
                Func1<T, Boolean> predicate = connection.getPredicate();
                if (predicate == null || predicate.call(chunk)) {
                    List<byte[]> buffer = writes.get(connection);
                    if (buffer == null) {
                        buffer = new LinkedList<>();
                        writes.put(connection, buffer);
                    }
                    buffer.add(encoder.call(chunk));
                }
            }
            if (!writes.isEmpty()) {
                for (Entry<AsyncConnection<T>, List<byte[]>> entry : writes.entrySet()) {
                    AsyncConnection<T> connection = entry.getKey();
                    List<byte[]> toWrite = entry.getValue();
                    connection.write(toWrite);
                    numEventsRouted.increment(toWrite.size());
                }
            }
        }
    }

    private Iterator<AsyncConnection<T>> loopingIterator(final Collection<AsyncConnection<T>> connections) {
        final AtomicReference<Iterator<AsyncConnection<T>>> iterRef = new AtomicReference<>(connections.iterator());
        return
                new Iterator<AsyncConnection<T>>() {
                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public AsyncConnection<T> next() {
                        Iterator<AsyncConnection<T>> iter = iterRef.get();
                        if (iter.hasNext()) {
                            return iter.next();
                        } else {
                            iterRef.set(connections.iterator());
                            return iterRef.get().next();
                        }
                    }
                };
    }
}
