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

package io.mantisrx.server.core.master;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.server.core.highavailability.LeaderRetrievalService;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.subjects.BehaviorSubject;

@Slf4j
public final class MasterMonitorImpl implements MasterMonitor, LeaderRetrievalService.Listener {

    private final LeaderRetrievalService leaderRetrievalService;
    private final BehaviorSubject<MasterDescription> masterSubject = BehaviorSubject.create();
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();
    private final JsonSerializer jsonSerializer = new JsonSerializer();
    private final ExecutorService executor;

    public MasterMonitorImpl(LeaderRetrievalService leaderRetrievalService) {
        this.leaderRetrievalService = leaderRetrievalService;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("default-master-monitor-%d")
            .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.leaderRetrievalService.getListenable().addListener(this, executor);
    }

    @Override
    public void onLeaderChanged(@Nullable byte[] leaderMetadata) {
        if (leaderMetadata != null) {
            try {
                log.info("value is {}", new String(leaderMetadata));
                final MasterDescription description =
                    jsonSerializer.fromJSON(new String(leaderMetadata), MasterDescription.class);
                log.info("new master description = {}", description);
                latestMaster.set(description);
                masterSubject.onNext(description);
            } catch (Exception e) {
                log.error("Failed to deserialize leader metadata {}", new String(leaderMetadata));
            }
        } else {
            log.info("looks like there's no value at the moment");
        }
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    @Nullable
    @Override
    public MasterDescription getLatestMaster() {
        return latestMaster.get();
    }

    @Override
    public void close() throws IOException {
        leaderRetrievalService.getListenable().removeListener(this);
        executor.shutdown();
    }
}
