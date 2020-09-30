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

package com.netflix.mantis.examples.mantispublishsample;

import java.util.concurrent.TimeUnit;

import com.netflix.mantis.examples.mantispublishsample.proto.RequestEvent;
import net.andreinc.mockneat.MockNeat;
import rx.Observable;


/**
 * Uses MockNeat to generate a random stream of events. Each event represents a hypothetical request
 * made by an end user to this service.
 */
public class DataGenerator implements IDataGenerator {
    private final int rateMs = 1000;
    private final MockNeat mockDataGenerator = MockNeat.threadLocal();

    @Override
    public Observable<RequestEvent> generateEvents() {
        return Observable
                .interval(rateMs, TimeUnit.MILLISECONDS)
                .map((tick) -> generateEvent());
    }

    private  RequestEvent generateEvent() {

        String path = mockDataGenerator.probabilites(String.class)
                .add(0.1, "/login")
                .add(0.2, "/genre/horror")
                .add(0.5, "/genre/comedy")
                .add(0.2, "/mylist")
                .get();

        String deviceType = mockDataGenerator.probabilites(String.class)
                .add(0.1, "ps4")
                .add(0.1, "xbox")
                .add(0.2, "browser")
                .add(0.3, "ios")
                .add(0.3, "android")
                .get();
        String userId = mockDataGenerator.strings().size(10).get();
        int status = mockDataGenerator.probabilites(Integer.class)
                .add(0.1,500)
                .add(0.7,200)
                .add(0.2,500)
                .get();


        String country = mockDataGenerator.countries().names().get();
        return RequestEvent.builder()
                .status(status)
                .uri(path)
                .country(country)
                .userId(userId)
                .deviceType(deviceType)
                .build();
    }

}
