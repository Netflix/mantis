/*
 * Copyright 2024 Netflix, Inc.
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

import org.junit.jupiter.api.Test;
import rx.subjects.PublishSubject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentHashingRouterTest {

    @Test
    public void shouldNotHaveHashCollisionsUsingKetamaExtended() {
        final AtomicLong hashInvocationCounter = new AtomicLong(0);
        HashFunction instrumentedKetama = bytes -> {
            hashInvocationCounter.getAndIncrement();
            return HashFunctions.xxh3().computeHash(bytes);
        };

        ConsistentHashingRouter<String, String> router = new ConsistentHashingRouter<>("test-router", x -> x.getKeyBytes(), instrumentedKetama);

        PublishSubject<List<byte[]>> subj = PublishSubject.create();
        Set<AsyncConnection<KeyValuePair<String, String>>> connections = generateStageToStageSlots("2", 40, 2).stream()
            .map(slot -> new AsyncConnection<KeyValuePair<String, String>>("fakehost", 123456, slot, slot, "test-group", subj, x -> true, null))
            .collect(Collectors.toSet());

        List<KeyValuePair<String, String>> data = new ArrayList<>();
        data.add(new KeyValuePair<>(12345, "12345".getBytes(), "test-value"));

        router.route(connections, data);
        router.route(connections, data);

        // We should perform 1000 hashes per connection, and used a cached value on subsequent calls
        assertEquals(connections.size() * 1000, hashInvocationCounter.get());
    }

    @Test
    public void shouldNotHaveHashCollisionsStageToStage() {

        int numberOfRingEntriesPerSlot = 1000;
        List<String> ringEntries = generateStageToStageSlots("2", 40, 2)
            .stream()
            .flatMap(slot -> IntStream.range(0, numberOfRingEntriesPerSlot)
                .boxed()
                .map(entryNum -> slot + "-" + entryNum))
            .collect(Collectors.toList());

        HashFunction hashFunction = HashFunctions.xxh3();

        Set<Long> ring = new HashSet<>();
        ringEntries.stream().forEach(entry -> ring.add(hashFunction.computeHash(entry.getBytes())));

        assertEquals(ringEntries.size(), ring.size());
    }

    @Test
    public void shouldNotHaveHashCollisionsLargeJob() {

        int numberOfRingEntriesPerSlot = 1000;
        List<String> ringEntries = generateStageToStageSlots("2", 500, 4)
            .stream()
            .flatMap(slot -> IntStream.range(0, numberOfRingEntriesPerSlot)
                .boxed()
                .map(entryNum -> slot + "-" + entryNum))
            .collect(Collectors.toList());

        HashFunction hashFunction = HashFunctions.xxh3();

        Set<Long> ring = new HashSet<>();
        ringEntries.stream().forEach(entry -> ring.add(hashFunction.computeHash(entry.getBytes())));

        assertEquals(ringEntries.size(), ring.size());
    }

    /**
     * Generates slot ids that look the same as those that come from mantis stages.
     * Example: stage_2_index_38_partition_1
     * @param stage The stage number as a string.
     * @param indices The number of indices. Typically the number of workers in said stage.
     * @param partitions The number of partitions the stage uses when connecting upstream.
     * @return A List of slotId / AsyncConnection id values for use in testing.
     */
    private List<String> generateStageToStageSlots(String stage, int indices, int partitions) {
        return IntStream.range(0, indices).boxed().map(index -> "stage_" + stage + "_index_" + index)
            .flatMap(prefix -> IntStream.range(1, partitions+1).boxed().map(partition -> prefix + "_partition_" + partition))
            .collect(Collectors.toList());
    }
}
