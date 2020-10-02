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

package com.netflix.mantis.samples.source;

import java.util.concurrent.TimeUnit;

import com.netflix.mantis.samples.proto.RequestEvent;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import lombok.extern.slf4j.Slf4j;
import net.andreinc.mockneat.MockNeat;
import rx.Observable;


/**
 * Generates random set of RequestEvents at a preconfigured interval.
 */
@Slf4j
public class RandomRequestSource implements Source<RequestEvent> {
    private MockNeat mockDataGenerator;

    @Override
    public Observable<Observable<RequestEvent>> call(Context context, Index index) {
        return Observable.just(Observable.interval(250, TimeUnit.MILLISECONDS).map((tick) -> {
            String ip = mockDataGenerator.ipv4s().get();
            String path = mockDataGenerator.probabilites(String.class)
                    .add(0.1, "/login")
                    .add(0.2, "/genre/horror")
                    .add(0.5, "/genre/comedy")
                    .add(0.2, "/mylist")
                    .get();
            return RequestEvent.builder().ipAddress(ip).requestPath(path).latency(1).build();
        }).doOnNext((event) -> {
            log.debug("Generated Event {}", event);
        }));

    }

    @Override
    public void init(Context context, Index index) {
        mockDataGenerator = MockNeat.threadLocal();
    }

}
