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

package io.mantisrx.publish.netty.transmitters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.exceptions.NonRetryableException;
import io.mantisrx.publish.internal.exceptions.RetryableException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class ChoiceOfTwoWorkerPoolTest {

    private ChoiceOfTwoWorkerPool workerPool;
    private MrePublishConfiguration config;
    private Registry registry;

    @BeforeEach
    void setUp() {
        config = mock(MrePublishConfiguration.class);
        when(config.getWorkerPoolRefreshIntervalSec()).thenReturn(-1);
        registry = new NoopRegistry();
        when(config.getWorkerPoolCapacity()).thenReturn(2);
        when(config.getWorkerPoolWorkerErrorQuota()).thenReturn(2);
        workerPool = new ChoiceOfTwoWorkerPool(config, registry, mock(EventChannel.class));
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void shouldRefreshEmptyPool() {
        List<MantisWorker> freshWorkers = Collections.singletonList(mock(MantisWorker.class));
        workerPool.refresh(freshWorkers);
        assertEquals(1, workerPool.size());
    }

    @Test
    void shouldRefreshPoolOnNonexistentWorker() {
        MantisWorker nonExistentWorker = mock(MantisWorker.class);
        when(nonExistentWorker.getHost()).thenReturn("1.1.1.1");
        MantisWorker newWorker = mock(MantisWorker.class);
        when(newWorker.getHost()).thenReturn("1.1.1.2");
        List<MantisWorker> freshWorkers = Collections.singletonList(nonExistentWorker);

        workerPool.refresh(freshWorkers);
        assertEquals(1, workerPool.size());

        freshWorkers = Collections.singletonList(newWorker);
        workerPool.refresh(freshWorkers);
        assertEquals(1, workerPool.size());
        assertEquals(newWorker, workerPool.getRandomWorker());
    }

    @Test
    void shouldForceRefreshFullPool() {
        when(config.getWorkerPoolCapacity()).thenReturn(1);
        when(config.getWorkerPoolWorkerErrorQuota()).thenReturn(1);
        workerPool = new ChoiceOfTwoWorkerPool(config, registry, mock(EventChannel.class));
        MantisWorker worker = mock(MantisWorker.class);
        List<MantisWorker> freshWorkers = Collections.singletonList(worker);
        workerPool.refresh(freshWorkers);

        workerPool.refresh(freshWorkers, true);
        assertEquals(0, workerPool.getWorkerErrors(worker));
    }

    @Test
    void shouldNotRefreshFullAndHealthyPool() throws NonRetryableException {
        when(config.getWorkerPoolCapacity()).thenReturn(1);
        when(config.getWorkerPoolWorkerErrorQuota()).thenReturn(1);
        workerPool = new ChoiceOfTwoWorkerPool(config, registry, mock(EventChannel.class));
        MantisWorker worker = mock(MantisWorker.class);
        List<MantisWorker> freshWorkers = Collections.singletonList(worker);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new RetryableException("expected test exception"));
        workerPool.refresh(freshWorkers);
        CompletableFuture<Void> future = workerPool.record(mock(Event.class), (w, e) -> completableFuture);

        future.whenComplete((v, t) -> {
            // Fresh workers haven't changed (i.e., haven't scaled up or down) and workers in the pool aren't blacklisted.
            workerPool.refresh(freshWorkers);
            // Check to see if the same worker, with the same number of errors still exists.
            assertEquals(1, workerPool.getWorkerErrors(worker));
        });
    }

    @Test
    void shouldGetRandomWorkerFromPool() {
        List<MantisWorker> freshWorkers = Collections.singletonList(mock(MantisWorker.class));
        workerPool.refresh(freshWorkers);
        assertNotNull(workerPool.getRandomWorker());
        freshWorkers = Arrays.asList(mock(MantisWorker.class), mock(MantisWorker.class));
        workerPool.refresh(freshWorkers);
        assertNotNull(workerPool.getRandomWorker());
    }

    @Test
    void shouldNotGetRandomWorkerFromEmptyPool() {
        assertNull(workerPool.getRandomWorker());
    }

    @Test
    void shouldNotRecordOnEmptyPool() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        assertThrows(
                NonRetryableException.class,
                () -> workerPool.record(mock(Event.class), (w, e) -> completableFuture));
    }

    @Test
    void shouldRecordOnHealthyPool() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        List<MantisWorker> freshWorkers = Collections.singletonList(mock(MantisWorker.class));
        workerPool.refresh(freshWorkers);
        Assertions.assertDoesNotThrow(() -> workerPool.record(mock(Event.class), (w, e) -> completableFuture).get());
    }

    @Test
    void shouldRecordAndIncrementError() throws NonRetryableException {
        MantisWorker worker = mock(MantisWorker.class);
        List<MantisWorker> freshWorkers = Collections.singletonList(worker);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new RetryableException("expected test exception"));
        workerPool.refresh(freshWorkers);
        CompletableFuture<Void> future = workerPool.record(mock(Event.class), (w, e) -> completableFuture);
        future.whenComplete((v, t) -> assertEquals(1, workerPool.getWorkerErrors(worker)));
    }

    @Test
    void shouldRecordAndImmediatelyBlacklist() throws NonRetryableException {
        when(config.getWorkerPoolCapacity()).thenReturn(1);
        when(config.getWorkerPoolWorkerErrorQuota()).thenReturn(1);
        workerPool = new ChoiceOfTwoWorkerPool(config, registry, mock(EventChannel.class));
        MantisWorker worker = mock(MantisWorker.class);
        List<MantisWorker> freshWorkers = Collections.singletonList(worker);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new RetryableException("expected test exception"));
        workerPool.refresh(freshWorkers);

        CompletableFuture<Void> future = workerPool.record(mock(Event.class), (w, e) -> completableFuture);
        future.whenComplete((v, t) -> {
            assertEquals(1, workerPool.size());
            assertFalse(workerPool.isBlacklisted(worker));
        });

        future = workerPool.record(mock(Event.class), (w, e) -> completableFuture);
        future.whenComplete((v, t) -> assertEquals(0, workerPool.size()));
    }
}
